/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.iotdb.db.sync.datasource;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.sync.externalpipe.operation.InsertOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.FULL_DELETED;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.NO_DELETED;

/** This class will parse 1 TsFile's content to 1 operation block. */
public class TsFileOpBlock extends AbstractOpBlock {
  private static final Logger logger = LoggerFactory.getLogger(TsFileOpBlock.class);

  // tsFile name
  private String tsFileName;
  private String modsFileName;
  private TsFileFullReader tsFileFullSeqReader;

  // full Timeseries Metadata TreeMap : FileOffset => Pair(DeviceId, TimeseriesMetadata)
  private Map<Long, Pair<Path, TimeseriesMetadata>> fullTsMetadataMap;
  // TreeMap: LocalIndex => ChunkInfo (measurementFullPath, ChunkOffset, PointCount, deletedFlag)
  private TreeMap<Long, ChunkInfo> indexToChunkInfoMap;

  // Save all modifications that are from .mods file.
  // (modificationList == null) means no .mods file or .mods file is empty.
  Collection<Modification> modificationList;
  // HashMap: measurement FullPath => DeletionGroup(save deletion info)
  private Map<String, DeletionGroup> fullPathToDeletionMap;

  // LRUMap: PageOffsetInTsfile => PageData
  private LRUCache<Long, List<TimeValuePair>> pageCache;

  private boolean dataReady = false;

  Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  private class ChunkInfo {
    public String measurementFullPath;
    public long chunkOffset;
    public long pointCount;
    public DeletionGroup.DeletedType deletedFlag = NO_DELETED;
  }

  public TsFileOpBlock(String sg, String tsFileName, long pipeDataSerialNumber) throws IOException {
    this(sg, tsFileName, pipeDataSerialNumber, 0);
  }

  public TsFileOpBlock(String sg, String tsFileName, long pipeDataSerialNumber, long beginIndex)
      throws IOException {
    this(sg, tsFileName, null, pipeDataSerialNumber, beginIndex);
  }

  public TsFileOpBlock(String sg, String tsFileName, String modsFileName, long pipeDataSerialNumber)
      throws IOException {
    this(sg, tsFileName, modsFileName, pipeDataSerialNumber, 0);
  }

  public TsFileOpBlock(
      String sg, String tsFileName, String modsFileName, long pipeDataSerialNumber, long beginIndex)
      throws IOException {
    super(sg, beginIndex);
    this.filePipeSerialNumber = pipeDataSerialNumber;
    this.tsFileName = tsFileName;

    this.modsFileName = null;
    if (modsFileName != null) {
      if (new File(modsFileName).exists()) {
        this.modsFileName = modsFileName;
      }
    }

    pageCache =
        new LRUCache<Long, List<TimeValuePair>>(3) {
          @Override
          public List<TimeValuePair> loadObjectByKey(Long key) throws IOException {
            return null;
          }
        };

    calculateDataCount();
  }

  /**
   * Calculate TsfileOpBlock's dataCount
   *
   * @throws IOException
   */
  private void calculateDataCount() throws IOException {
    // == calculate dataCount according to tsfile
    tsFileFullSeqReader = new TsFileFullReader(this.tsFileName);
    fullTsMetadataMap = tsFileFullSeqReader.getAllTimeseriesMeta(false);
    // close reader to avoid fd leak
    tsFileFullSeqReader.close();
    tsFileFullSeqReader = null;

    dataCount = 0;
    for (Pair<Path, TimeseriesMetadata> entry : fullTsMetadataMap.values()) {
      dataCount += entry.right.getStatistics().getCount();
    }

    // == Here, release fullTsMetadataMap for saving memory.
    fullTsMetadataMap = null;
  }

  /**
   * return the Count of data points in this TsFile
   *
   * @return
   */
  @Override
  public long getDataCount() {
    return dataCount;
  }

  /**
   * Check the deletion-state of the data-points of specific time range according to the info of
   * .mods.
   *
   * @param measurementPath - measurementPath full path without wildcard
   * @param startTime - the start time of data set, inclusive
   * @param endTime - the end time of data set, inclusive
   * @return
   */
  private DeletionGroup.DeletedType checkDeletedState(
      String measurementPath, long startTime, long endTime) {
    DeletionGroup deletionGroup = getFullPathDeletion(measurementPath);

    if (deletionGroup == null) {
      return NO_DELETED;
    }

    return deletionGroup.checkDeletedState(startTime, endTime);
  }

  /**
   * Generate indexToChunkInfoMap for whole TsFile
   *
   * @throws IOException
   */
  private void buildIndexToChunkMap() throws IOException {
    if (tsFileFullSeqReader == null) {
      tsFileFullSeqReader = new TsFileFullReader(tsFileName);
    }

    if (fullTsMetadataMap == null) {
      fullTsMetadataMap = tsFileFullSeqReader.getAllTimeseriesMeta(true);
    }

    // chunkOffset => ChunkInfo (measurementFullPath, ChunkOffset, PointCount, deletedFlag)
    Map<Long, ChunkInfo> offsetToCountMap = new TreeMap<>();
    for (Pair<Path, TimeseriesMetadata> value : fullTsMetadataMap.values()) {
      List<IChunkMetadata> chunkMetaList = value.right.getChunkMetadataList();

      if (chunkMetaList == null) {
        continue;
      }

      for (IChunkMetadata chunkMetadata : chunkMetaList) {
        // traverse every chunk
        ChunkInfo chunkInfo = new ChunkInfo();
        chunkInfo.measurementFullPath = value.left.getFullPath();
        chunkInfo.chunkOffset = chunkMetadata.getOffsetOfChunkHeader();
        chunkInfo.pointCount = chunkMetadata.getStatistics().getCount();

        chunkInfo.deletedFlag =
            checkDeletedState(
                chunkInfo.measurementFullPath,
                chunkMetadata.getStatistics().getStartTime(),
                chunkMetadata.getStatistics().getEndTime());

        offsetToCountMap.put(chunkInfo.chunkOffset, chunkInfo);
      }
    }

    indexToChunkInfoMap = new TreeMap<>();
    long localIndex = 0;
    for (ChunkInfo chunkInfo : offsetToCountMap.values()) {
      indexToChunkInfoMap.put(localIndex, chunkInfo);
      localIndex += chunkInfo.pointCount;
    }
  }

  /**
   * Generate modificationList using .mods file. Result: (modificationList == null) means no .mods
   * file or .mods file is empty.
   *
   * @throws IOException
   */
  private void buildModificationList() throws IOException {
    if (modsFileName == null) {
      logger.debug("buildModificationList(), modsFileName is null.");
      modificationList = null;
      return;
    }

    try (ModificationFile modificationFile = new ModificationFile(modsFileName)) {
      modificationList = modificationFile.getModifications();
    }

    if (modificationList.isEmpty()) {
      modificationList = null;
    }
  }

  /**
   * Generate fullPathToDeletionMap for fullPath
   *
   * @param fullPath measurement full path without wildcard
   * @return
   */
  private DeletionGroup getFullPathDeletion(String fullPath) {
    // (fullPathToDeletionMap == null) means modificationList is null or empty
    if (fullPathToDeletionMap == null) {
      return null;
    }

    // Try to get data from cache firstly
    if (fullPathToDeletionMap.containsKey(fullPath)) {
      return fullPathToDeletionMap.get(fullPath);
    }

    // == insert all deletion intervals info to deletionGroup.
    DeletionGroup deletionGroup = new DeletionGroup();
    PartialPath partialPath = null;
    try {
      partialPath = new PartialPath(fullPath);
    } catch (IllegalPathException e) {
      logger.error("getFullPathDeletion(), find invalid fullPath: {}", fullPath);
    }

    if (partialPath != null) {
      // == Here, has been sure  (modificationList != null) && (!modificationList.isEmpty())
      for (Modification modification : modificationList) {
        if ((modification instanceof Deletion)
            && (modification.getPath().matchFullPath(partialPath))) {
          Deletion deletion = (Deletion) modification;
          deletionGroup.addDelInterval(deletion.getStartTime(), deletion.getEndTime());
        }
      }
    }

    if (deletionGroup.isEmpty()) {
      deletionGroup = null;
    }

    fullPathToDeletionMap.put(fullPath, deletionGroup);

    return deletionGroup;
  }

  /**
   * Parse 1 NonAligned Chunk to get all data points. Note: new data will be appended to parameter
   * tvPairList for better performance
   *
   * @param chunkHeader
   * @param startIndexInChunk
   * @param length
   * @param deletionGroup - if it is not null, need to check whether data points have benn deleted
   * @param tvPairList
   * @return
   * @throws IOException
   */
  private long getNonAlignedChunkPoints(
      ChunkHeader chunkHeader,
      long startIndexInChunk,
      long length,
      DeletionGroup deletionGroup,
      List<TimeValuePair> tvPairList)
      throws IOException {
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    int chunkLeftDataSize = chunkHeader.getDataSize();

    long index = 0;
    while ((chunkLeftDataSize > 0) && ((index - startIndexInChunk) < length)) {
      // begin to traverse every page
      long pagePosInTsfile = tsFileFullSeqReader.position();
      boolean pageHeaderHasStatistic =
          ((chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      PageHeader pageHeader =
          tsFileFullSeqReader.readPageHeader(chunkHeader.getDataType(), pageHeaderHasStatistic);

      int pageSize = pageHeader.getSerializedPageSize();
      chunkLeftDataSize -= pageSize;

      if (pageHeaderHasStatistic) {
        // == check whether whole page is out of  [startIndexInChunk, startIndexInChunk + length)
        long pageDataCount = pageHeader.getNumOfValues();
        if ((index + pageDataCount) <= startIndexInChunk) { // skip this page
          tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
          index += pageDataCount;
          continue;
        }

        // == check whether whole page has been deleted by .mods file
        if (deletionGroup == null) { // No deletion related to current chunk
          continue;
        }

        DeletionGroup.DeletedType deletedType =
            deletionGroup.checkDeletedState(pageHeader.getStartTime(), pageHeader.getEndTime());

        if (deletedType == FULL_DELETED) {
          long needCount =
              min(index + pageDataCount, startIndexInChunk + length)
                  - max(index, startIndexInChunk);
          for (long i = 0; i < needCount; i++) {
            tvPairList.add(null);
          }
          tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
          index += pageDataCount;
          continue;
        }
      }

      // == At first, try to get page-data from pageCache.
      List<TimeValuePair> pageTVList = pageCache.get(pagePosInTsfile);
      // == if pageCache has no data
      if (pageTVList == null) {
        // == read the page's all data
        pageTVList = getNonAlignedPagePoints(pageHeader, chunkHeader, valueDecoder, deletionGroup);
        pageCache.put(pagePosInTsfile, pageTVList);
      }

      int beginIdxInPage = (int) (max(index, startIndexInChunk) - index);
      int countInPage = (int) min(pageTVList.size(), length - index + startIndexInChunk);
      tvPairList.addAll(
          ((LinkedList) pageTVList).subList(beginIdxInPage, beginIdxInPage + countInPage));

      index += countInPage;
    }

    return (index - startIndexInChunk);
  }

  /**
   * Parse 1 NonAligned page to get all data points. Note:
   *
   * <p>1) New data will be appended to parameter tvPairList for better performance
   *
   * <p>2) deleted data by .mods will be set to null.
   *
   * @param pageHeader
   * @param chunkHeader
   * @param valueDecoder
   * @param deletionGroup
   * @return
   * @throws IOException
   */
  private List<TimeValuePair> getNonAlignedPagePoints(
      PageHeader pageHeader,
      ChunkHeader chunkHeader,
      Decoder valueDecoder,
      DeletionGroup deletionGroup)
      throws IOException {
    List<TimeValuePair> tvList = new LinkedList<>();

    ByteBuffer pageData =
        tsFileFullSeqReader.readPage(pageHeader, chunkHeader.getCompressionType());

    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    if (chunkHeader.getChunkType() == MetaMarker.CHUNK_HEADER) {
      logger.debug("points in the page(by pageHeader): " + pageHeader.getNumOfValues());
    } else {
      logger.debug("points in the page(by batchData): " + batchData.length());
    }

    if (batchData.isEmpty()) {
      logger.warn("getNonAlignedChunkPoints(), chunk is empty, chunkHeader = {}.", chunkHeader);
      return tvList;
    }

    batchData.resetBatchData();
    DeletionGroup.IntervalCursor intervalCursor = new DeletionGroup.IntervalCursor();
    while (batchData.hasCurrent()) {
      long ts = batchData.currentTime();
      if ((deletionGroup != null) && (deletionGroup.isDeleted(ts, intervalCursor))) {
        tvList.add(null);
      } else {
        TimeValuePair timeValuePair = new TimeValuePair(ts, batchData.currentTsPrimitiveType());
        logger.debug("getNonAlignedChunkPoints(), timeValuePair = {} ", timeValuePair);
        tvList.add(timeValuePair);
      }

      batchData.next();
    }

    return tvList;
  }

  /**
   * Parse 1 Aligned Chunk to get all data points. Note: new data will be appended to parameter
   * tvPairList for better performance
   *
   * @param chunkHeader
   * @param indexInChunk
   * @param lengthInChunk
   * @param deletionGroup - if it is not null, need to check whether data points have benn deleted
   * @param tvPairList
   * @return
   * @throws IOException
   */
  private long getAlignedChunkPoints(
      ChunkHeader chunkHeader,
      long indexInChunk,
      long lengthInChunk,
      DeletionGroup deletionGroup,
      List<TimeValuePair> tvPairList)
      throws IOException {
    List<long[]> timeBatch = new ArrayList<>();
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());

    int timePageIndex = 0, valuePageIndex = 0;
    boolean timeChunkEnd = false;
    byte chunkTypeByte = chunkHeader.getChunkType();
    while (true) {
      int chunkDataSize = chunkHeader.getDataSize();
      while (chunkDataSize > 0) {
        valueDecoder.reset();

        // begin to traverse every page
        PageHeader pageHeader =
            tsFileFullSeqReader.readPageHeader(
                chunkHeader.getDataType(), (chunkTypeByte & 0x3F) == MetaMarker.CHUNK_HEADER);
        ByteBuffer pageData =
            tsFileFullSeqReader.readPage(pageHeader, chunkHeader.getCompressionType());

        if ((chunkTypeByte & (byte) TsFileConstant.TIME_COLUMN_MASK)
            == (byte) TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
          TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
          timeBatch.add(timePageReader.getNextTimeBatch());

          for (int i = 0; i < timeBatch.get(timePageIndex).length; i++) {
            logger.debug("time: " + timeBatch.get(timePageIndex)[i]);
          }
          timePageIndex++;
        } else if ((chunkTypeByte & (byte) TsFileConstant.VALUE_COLUMN_MASK)
            == (byte) TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
          timeChunkEnd = true;
          ValuePageReader valuePageReader =
              new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
          TsPrimitiveType[] valueBatch =
              valuePageReader.nextValueBatch(timeBatch.get(valuePageIndex));
          if (valueBatch.length == 0) {
            logger.debug(
                "getAlignedChunkPoints(), Empty Page. ValuePageReader = {}", valuePageReader);
          }

          for (int i = 0; i < valueBatch.length; i++) {
            TimeValuePair timeValuePair =
                new TimeValuePair(timeBatch.get(valuePageIndex)[i], valueBatch[i]);
            logger.debug("getNonAlignedChunkPoints(), timeValuePair = {} ", timeValuePair);
            tvPairList.add(timeValuePair);
          }

          valuePageIndex++;
        }
        chunkDataSize -= pageHeader.getSerializedPageSize();
      }

      chunkTypeByte = tsFileFullSeqReader.readMarker();
      chunkHeader = tsFileFullSeqReader.readChunkHeader(chunkTypeByte);
      if (timeChunkEnd
          && ((chunkTypeByte & (byte) TsFileConstant.VALUE_COLUMN_MASK)
              != (byte) TsFileConstant.VALUE_COLUMN_MASK)) {
        break;
      }
    }

    // return tvPairList;
    return 0;
  }

  /**
   * get 1 Chunk's partial data points according to indexInChunk & lengthInChunk Note: new data will
   * be appended to parameter tvPairList for better performance
   *
   * @param chunkInfo
   * @param indexInChunk
   * @param lengthInChunk
   * @param tvPairList, the got data points will be appended to this List.
   * @return
   * @throws IOException
   */
  private long getChunkPoints(
      ChunkInfo chunkInfo, long indexInChunk, long lengthInChunk, List<TimeValuePair> tvPairList)
      throws IOException {

    // == If whole chunk has been deleted according to .mods file
    if (chunkInfo.deletedFlag == FULL_DELETED) {
      for (long i = 0; i < lengthInChunk; i++) {
        tvPairList.add(null);
      }
      return lengthInChunk;
    }

    tsFileFullSeqReader.position(chunkInfo.chunkOffset);
    byte chunkTypeByte = tsFileFullSeqReader.readMarker();
    ChunkHeader chunkHeader = tsFileFullSeqReader.readChunkHeader(chunkTypeByte);

    DeletionGroup deletionGroup = null;
    if (chunkInfo.deletedFlag != NO_DELETED) {
      deletionGroup = getFullPathDeletion(chunkInfo.measurementFullPath);
    }

    if (chunkHeader.getDataType() == TSDataType.VECTOR) {
      return getAlignedChunkPoints(
          chunkHeader, indexInChunk, lengthInChunk, deletionGroup, tvPairList);
    } else {
      return getNonAlignedChunkPoints(
          chunkHeader, indexInChunk, lengthInChunk, deletionGroup, tvPairList);
    }
  }

  /**
   * Add sensorFullPath + tvPairList to dataList
   *
   * @param dataList
   * @param sensorFullPath
   * @param tvPairList
   * @throws IOException
   */
  private void insertToDataList(
      List<Pair<MeasurementPath, List<TimeValuePair>>> dataList,
      String sensorFullPath,
      List<TimeValuePair> tvPairList)
      throws IOException {
    MeasurementPath measurementPath;
    try {
      measurementPath = new MeasurementPath(sensorFullPath);
    } catch (IllegalPathException e) {
      logger.error("TsFileOpBlock.insertToDataList(), Illegal MeasurementPath: {}", "");
      throw new IOException("Illegal MeasurementPath: " + sensorFullPath, e);
    }
    dataList.add(new Pair<>(measurementPath, tvPairList));
  }

  private void prepareData() throws IOException {
    if (tsFileFullSeqReader == null) {
      tsFileFullSeqReader = new TsFileFullReader(tsFileName);
    }

    if (modsFileName != null) {
      buildModificationList();
    }

    if ((fullPathToDeletionMap == null) && (modificationList != null)) {
      fullPathToDeletionMap = new HashMap<>();
    }

    if (indexToChunkInfoMap == null) {
      buildIndexToChunkMap();
    }

    dataReady = true;
  }

  /**
   * Get 1 Operation that contain needed data. Note: 1) Expected data range is [index, index+length)
   * 2) Real returned data length can less than input parameter length
   *
   * @param index
   * @param length
   * @return
   * @throws IOException
   */
  @Override
  public Operation getOperation(long index, long length) throws IOException {
    if (closed) {
      logger.error("TsFileOpBlock.getOperation(), can not access closed TsFileOpBlock: {}.", this);
      throw new IOException("can not access closed TsFileOpBlock: " + this);
    }

    long indexInTsfile = index - beginIndex;
    if (indexInTsfile < 0 || indexInTsfile >= dataCount) {
      logger.error("TsFileOpBlock.getOperation(), Error: index {} is out of range.", index);
      // throw new IOException("index is out of range.");
      return null;
    }

    if (!dataReady) {
      prepareData();
    }

    LinkedList<Pair<MeasurementPath, List<TimeValuePair>>> dataList = new LinkedList<>();
    String lastSensorFullPath = "";
    List<TimeValuePair> tvPairList = null;

    // handle all chunks that contain needed data
    long remain = length;
    while (remain > 0) {
      Map.Entry<Long, ChunkInfo> entry = indexToChunkInfoMap.floorEntry(indexInTsfile);
      if (entry == null) {
        logger.error(
            "TsFileOpBlock.getOperation(), indexInTsfile {} if out of indexToChunkOffsetMap.",
            indexInTsfile);
        throw new IOException("indexInTsfile is out of range.");
      }

      long indexInChunk = indexInTsfile - entry.getKey();
      ChunkInfo chunkInfo = entry.getValue();
      String sensorFullPath = chunkInfo.measurementFullPath;
      long chunkPointCount = chunkInfo.pointCount;

      long lengthInChunk = min(chunkPointCount - indexInChunk, remain);

      if (!sensorFullPath.equals(lastSensorFullPath)) {
        if ((tvPairList != null) && (tvPairList.size() > 0)) {
          insertToDataList(dataList, lastSensorFullPath, tvPairList);
          tvPairList = null;
        }
        lastSensorFullPath = sensorFullPath;
      }

      // == get data from 1 chunk
      if (tvPairList == null) {
        tvPairList = new LinkedList<>();
      }
      long readCount = getChunkPoints(chunkInfo, indexInChunk, lengthInChunk, tvPairList);
      if (readCount != lengthInChunk) {
        logger.error(
            "TsFileOpBlock.getOperation(), error when read chunk from file {}. lengthInChunk={}, readCount={}, ",
            indexInTsfile,
            lengthInChunk,
            readCount);
        throw new IOException("Error read chunk from file:" + indexInTsfile);
      }

      remain -= readCount;
      indexInTsfile = entry.getKey() + chunkPointCount; // next chunk's local index

      if (indexInTsfile >= dataCount) { // has reached the end of this Tsfile
        break;
      }
    }

    if ((tvPairList != null) && (tvPairList.size() > 0)) {
      insertToDataList(dataList, lastSensorFullPath, tvPairList);
    }

    return new InsertOperation(storageGroup, index, index + length - remain, dataList);
  }

  /** release the current class object's resource */
  @Override
  public void close() {
    super.close();
    if (tsFileFullSeqReader != null) {
      try {
        tsFileFullSeqReader.close();
      } catch (IOException e) {
        logger.error("tsFileFullSeqReader.close() exception, file = {}", tsFileName, e);
      }
      tsFileFullSeqReader = null;
    }

    dataReady = false;
  }

  /** This class is used to read & parse Tsfile */
  private class TsFileFullReader extends TsFileSequenceReader {
    protected TsFileMetadata tsFileMetaData;

    /**
     * @param file, Tsfile's full path name.
     * @throws IOException
     */
    public TsFileFullReader(String file) throws IOException {
      super(file);
    }

    /**
     * Generate timeseriesMetadataMap, Offset => (DeviceID, TimeseriesMetadata)
     *
     * @param startOffset
     * @param metadataIndex
     * @param buffer
     * @param deviceId
     * @param type
     * @param timeseriesMetadataMap, output param
     * @param needChunkMetadata, input param, whether need parser ChunkMetaData
     * @throws IOException
     */
    private void genTSMetadataFromMetaIndexEntry(
        long startOffset,
        MetadataIndexEntry metadataIndex,
        ByteBuffer buffer,
        String deviceId,
        MetadataIndexNodeType type,
        Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap,
        boolean needChunkMetadata)
        throws IOException {
      try {
        if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          while (buffer.hasRemaining()) {
            long pos = startOffset + buffer.position();
            TimeseriesMetadata timeseriesMetadata =
                TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata);
            timeseriesMetadataMap.put(
                pos,
                new Pair<>(
                    new Path(deviceId, timeseriesMetadata.getMeasurementId()), timeseriesMetadata));
          }
        } else { // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = metadataIndex.getName();
          }
          MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
          int metadataIndexListSize = metadataIndexNode.getChildren().size();
          for (int i = 0; i < metadataIndexListSize; i++) {
            long endOffset = metadataIndexNode.getEndOffset();
            if (i != metadataIndexListSize - 1) {
              endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
            }
            ByteBuffer nextBuffer =
                readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
            genTSMetadataFromMetaIndexEntry(
                metadataIndexNode.getChildren().get(i).getOffset(),
                metadataIndexNode.getChildren().get(i),
                nextBuffer,
                deviceId,
                metadataIndexNode.getNodeType(),
                timeseriesMetadataMap,
                needChunkMetadata);
          }
        }
      } catch (BufferOverflowException e) {
        throw e;
      }
    }

    /**
     * collect all Timeseries Meta of the tsFile
     *
     * @param needChunkMetadata
     * @return Map, FileOffset => pair(deviceId => TimeseriesMetadata)
     * @throws IOException
     */
    public Map<Long, Pair<Path, TimeseriesMetadata>> getAllTimeseriesMeta(boolean needChunkMetadata)
        throws IOException {
      if (tsFileMetaData == null) {
        readFileMetadata();
      }

      MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
        long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
        if (i != metadataIndexEntryList.size() - 1) {
          endOffset = metadataIndexEntryList.get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
        genTSMetadataFromMetaIndexEntry(
            metadataIndexEntry.getOffset(),
            metadataIndexEntry,
            buffer,
            null,
            metadataIndexNode.getNodeType(),
            timeseriesMetadataMap,
            needChunkMetadata);
      }

      return timeseriesMetadataMap;
    }

    /**
     * read File meta-data of TsFile
     *
     * @return
     * @throws IOException
     */
    public TsFileMetadata readFileMetadata() throws IOException {
      if (tsFileMetaData != null) {
        return tsFileMetaData;
      }

      try {
        tsFileMetaData =
            TsFileMetadata.deserializeFrom(
                readData(getFileMetadataPos(), (int) getFileMetadataSize()));
      } catch (BufferOverflowException e) {
        logger.error("readFileMetadata(), reading file metadata of file {}", file);
        throw e;
      }
      return tsFileMetaData;
    }
  }

  @TestOnly
  public Collection<Modification> getModificationList() {
    return modificationList;
  }

  @TestOnly
  public Map<String, DeletionGroup> getFullPathToDeletionMap() {
    return fullPathToDeletionMap;
  }

  @Override
  public String toString() {
    return "storageGroup="
        + storageGroup
        + ", tsFileName="
        + tsFileName
        + ", modsFileName="
        + modsFileName
        + ", filePipeSerialNumber="
        + filePipeSerialNumber
        + ", beginIndex="
        + beginIndex
        + ", dataCount="
        + dataCount;
  }
}
