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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.sync.externalpipe.operation.InsertOperation;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.FULL_DELETED;
import static org.apache.iotdb.db.sync.datasource.DeletionGroup.DeletedType.NO_DELETED;
import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.VECTOR;

/** This class will parse 1 TsFile's content to 1 operation block. */
public class TsFileOpBlock extends AbstractOpBlock {
  private static final Logger logger = LoggerFactory.getLogger(TsFileOpBlock.class);

  // tsFile name
  private String tsFileName;
  private String modsFileName;
  private TsFileFullReader tsFileFullSeqReader;

  // full Timeseries Metadata TreeMap : FileOffset => Pair(MeasurementFullPath, TimeseriesMetadata)
  private Map<Long, Pair<Path, TimeseriesMetadata>> fullTsMetadataMap;
  // TreeMap: IndexInFile => ChunkInfo (measurementFullPath, ChunkOffset, PointCount, deletedFlag)
  private TreeMap<Long, ChunkInfo> indexToChunkInfoMap;

  // Save all modifications that are from .mods file.
  // (modificationList == null) means no .mods file or .mods file is empty.
  Collection<Modification> modificationList;
  // HashMap: measurement FullPath => DeletionGroup(save deletion info)
  private Map<String, DeletionGroup> fullPathToDeletionMap;

  // LRUMap: startIndexOfPage => PageData
  private LRUCache<Long, List<TimeValuePair>> pageCache;

  private boolean dataReady = false;

  Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  private class ChunkInfo {
    public String measurementFullPath;
    public long chunkOffsetInFile = -1;
    public long startIndexInFile = -1;
    public long pointCount = 0;
    public boolean isTimeAligned = false;
    public long timeChunkOffsetInFile = -1;
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
    super(sg, pipeDataSerialNumber, beginIndex);
    this.tsFileName = tsFileName;

    this.modsFileName = null;
    if (modsFileName != null) {
      if (new File(modsFileName).exists()) {
        this.modsFileName = modsFileName;
      }
    }

    pageCache =
        new LRUCache<Long, List<TimeValuePair>>(5) {
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

    dataCount = 0;
    for (String device : tsFileFullSeqReader.getAllDevices()) {
      // == Here use not readChunkMetadataInDevice(device) but readDeviceMetadata(Device).
      // == Because readDeviceMetadata() will not parse chunkMeta, so has better performance.
      // == deviceMetaData is HashMap: Measurement => TimeseriesMetadata
      Map<String, TimeseriesMetadata> deviceMetaData =
          tsFileFullSeqReader.readDeviceMetadata(device);

      long countInDevice = 0;
      for (TimeseriesMetadata tsMetaData : deviceMetaData.values()) {
        if (tsMetaData.getTSDataType() == VECTOR) {
          countInDevice = tsMetaData.getStatistics().getCount() * (deviceMetaData.size() - 1);
          break;
        }
        countInDevice += tsMetaData.getStatistics().getCount();
      }
      dataCount += countInDevice;
    }

    // close reader to avoid fd leak
    tsFileFullSeqReader.close();
    tsFileFullSeqReader = null;

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

    // chunkOffset => ChunkInfo (measurementFullPath, ChunkOffset, PointCount, deletedFlag etc.)
    Map<Long, ChunkInfo> offsetToChunkInfoMap = new TreeMap<>();
    Map<Long, ChunkInfo> tmpOffsetToChunkInfoMap = new TreeMap<>();

    for (String device : tsFileFullSeqReader.getAllDevices()) {
      // == Here use not readDeviceMetadata(Device) but readChunkMetadataInDevice(device).
      // == Because readChunkMetadataInDevice() will parse chunkMeta.
      // == Get LinkedHashMap: measurementId -> ChunkMetadata list
      Map<String, List<ChunkMetadata>> chunkMetadataMap =
          tsFileFullSeqReader.readChunkMetadataInDevice(device);

      tmpOffsetToChunkInfoMap.clear();
      boolean isTimeAlignedDevice = false;
      for (List<ChunkMetadata> chunkMetadataList : chunkMetadataMap.values()) {
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          ChunkInfo chunkInfo = new ChunkInfo();

          chunkInfo.measurementFullPath =
              new Path(device, chunkMetadata.getMeasurementUid(), true).getFullPath();
          chunkInfo.chunkOffsetInFile = chunkMetadata.getOffsetOfChunkHeader();
          chunkInfo.pointCount = chunkMetadata.getStatistics().getCount();

          if (chunkMetadata.getDataType() == VECTOR) {
            isTimeAlignedDevice = true;
            chunkInfo.isTimeAligned = true;
          } else {
            chunkInfo.deletedFlag =
                checkDeletedState(
                    chunkInfo.measurementFullPath,
                    chunkMetadata.getStatistics().getStartTime(),
                    chunkMetadata.getStatistics().getEndTime());
          }

          tmpOffsetToChunkInfoMap.put(chunkInfo.chunkOffsetInFile, chunkInfo);
        }
      }

      if (isTimeAlignedDevice) {
        long timeChunkOffset = -1;
        long timeChunkPointCount = 0;

        Iterator<Map.Entry<Long, ChunkInfo>> iter = tmpOffsetToChunkInfoMap.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<Long, ChunkInfo> entry = iter.next();
          ChunkInfo chunkInfo = entry.getValue();
          if (chunkInfo.isTimeAligned) {
            timeChunkOffset = entry.getKey();
            timeChunkPointCount = chunkInfo.pointCount;
            iter.remove();
          } else {
            chunkInfo.isTimeAligned = true;
            chunkInfo.timeChunkOffsetInFile = timeChunkOffset;
            chunkInfo.pointCount = timeChunkPointCount;
          }
        }
      }

      offsetToChunkInfoMap.putAll(tmpOffsetToChunkInfoMap);
    }

    indexToChunkInfoMap = new TreeMap<>();
    long indexInFile = 0;
    for (ChunkInfo chunkInfo : offsetToChunkInfoMap.values()) {
      chunkInfo.startIndexInFile = indexInFile;
      indexToChunkInfoMap.put(indexInFile, chunkInfo);
      indexInFile += chunkInfo.pointCount;
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
   * Try to get page data from page cache. *
   *
   * @param pageStartIndexInFile page's first data's index in Tsfile. It is also the key of
   *     page-cache.
   * @param dataIndexInfile requested data's first dota's index in Tsfile.
   * @param dataLen requested data's length
   * @param destTVList returned data will be appended to this variable.
   * @return -1 : pageCache has no this page. >=0 : data number of the found page in page-cache. (No
   *     matter whether this page has requested data)
   * @throws IOException
   */
  private long getDataFromPageCache(
      long pageStartIndexInFile, long dataIndexInfile, long dataLen, List<TimeValuePair> destTVList)
      throws IOException {
    List<TimeValuePair> pageTVList = pageCache.get(pageStartIndexInFile);
    if (pageTVList == null) {
      return -1;
    }

    // == PageCache has this page. Try to get data from pageCache
    long pageDataCount = pageTVList.size();
    // == If this page has no requested data
    if (((pageStartIndexInFile + pageDataCount) < dataIndexInfile)
        || (pageStartIndexInFile >= (dataIndexInfile + dataLen))) {
      return pageDataCount;
    }

    int beginIdxInPage = (int) (max(dataIndexInfile, pageStartIndexInFile) - pageStartIndexInFile);
    int needCountInPage =
        (int)
            (min(pageStartIndexInFile + pageDataCount, dataIndexInfile + dataLen)
                - max(dataIndexInfile, pageStartIndexInFile));
    destTVList.addAll(
        ((LinkedList) pageTVList).subList(beginIdxInPage, beginIdxInPage + needCountInPage));

    return pageDataCount;
  }
  /**
   * Parse 1 normal (not Time-Aligned) Chunk to get partial data points according to indexInChunk &
   * lengthInChunk
   *
   * <p>Note: new data will be appended to parameter tvPairList for better performance.
   *
   * @param chunkInfo
   * @param startIndexInChunk
   * @param lengthInChunk
   * @param deletionGroup - If it is not null, need to check whether data points have benn deleted
   * @param tvPairList
   * @return the number of returned data points
   * @throws IOException
   */
  private long getNonAlignedChunkPoints(
      ChunkInfo chunkInfo,
      long startIndexInChunk,
      long lengthInChunk,
      DeletionGroup deletionGroup,
      List<TimeValuePair> tvPairList)
      throws IOException {

    if (chunkInfo.chunkOffsetInFile < 0) {
      String errMsg =
          String.format(
              "getNonAlignedChunkPoints(), invalid chunkOffsetInFile=%d.",
              chunkInfo.chunkOffsetInFile);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }
    tsFileFullSeqReader.position(chunkInfo.chunkOffsetInFile);
    byte chunkTypeByte = tsFileFullSeqReader.readMarker();
    ChunkHeader chunkHeader = tsFileFullSeqReader.readChunkHeader(chunkTypeByte);

    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    boolean pageHeaderHasStatistic =
        ((chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
    int chunkLeftDataSize = chunkHeader.getDataSize();

    long indexInChunk = 0;
    while ((chunkLeftDataSize > 0) && (indexInChunk < (startIndexInChunk + lengthInChunk))) {
      // == Begin to traverse every page
      long pagePosInTsfile = tsFileFullSeqReader.position();
      PageHeader pageHeader =
          tsFileFullSeqReader.readPageHeader(chunkHeader.getDataType(), pageHeaderHasStatistic);

      int pageSize = pageHeader.getSerializedPageSize();
      chunkLeftDataSize -= pageSize;

      long pageStartIndexInFile = chunkInfo.startIndexInFile + indexInChunk;

      // == At first, try to find page data from pageCache.
      long pageDataNum =
          getDataFromPageCache(
              pageStartIndexInFile,
              chunkInfo.startIndexInFile + startIndexInChunk,
              lengthInChunk,
              tvPairList);
      if (pageDataNum >= 0) { // find page in page cache
        tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
        indexInChunk += pageDataNum;
        continue;
      }

      // Note: Here, we can not use pageHeaderHasStatistic to do judge.
      // Because, even if pageHeaderHasStatistic == true,
      // pageHeader.getStatistics() can still be null when page uncompressedSize == 0 (empty page).
      if (pageHeader.getStatistics() != null) {
        // == check whether whole page is out of  [startIndexInChunk, startIndexInChunk +
        // lengthInChunk)
        long pageDataCount = pageHeader.getNumOfValues();
        if ((indexInChunk + pageDataCount) <= startIndexInChunk) { // skip this page
          tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
          indexInChunk += pageDataCount;
          continue;
        }

        // == check whether whole page has been deleted by .mods file
        if (deletionGroup != null) { // have deletion related to current chunk
          DeletionGroup.DeletedType pageDeletedStatus =
              deletionGroup.checkDeletedState(pageHeader.getStartTime(), pageHeader.getEndTime());
          if (pageDeletedStatus == FULL_DELETED) {
            // == put page data to page-cache
            List<TimeValuePair> pageTVList = new LinkedList<>();
            for (long i = 0; i < pageDataCount; i++) {
              pageTVList.add(null);
            }
            pageCache.put(pageStartIndexInFile, pageTVList);

            // == gen requested TV data list
            long needCount =
                min(indexInChunk + pageDataCount, startIndexInChunk + lengthInChunk)
                    - max(indexInChunk, startIndexInChunk);
            for (long i = 0; i < needCount; i++) {
              tvPairList.add(null);
            }

            tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
            indexInChunk += pageDataCount;
            continue;
          }
        }
      }

      // == read page data from Tsfile
      List<TimeValuePair> pageTVList;
      pageTVList = getNonAlignedPagePoints(pageHeader, chunkHeader, valueDecoder, deletionGroup);
      pageCache.put(pageStartIndexInFile, pageTVList);

      // == check whether whole page is out of [startIndexInChunk, startIndexInChunk +
      // lengthInChunk)
      long pageDataCount = pageTVList.size();
      if ((indexInChunk + pageDataCount) <= startIndexInChunk) { // skip this page
        // == Here, not check (indexInChunk >= (startIndexInChunk + lengthInChunk)), because the
        // preceding  while(xxx) has checked it.
        // tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
        indexInChunk += pageDataCount;
        continue;
      }

      // == select needed data and append them to tvPairList
      int beginIdxInPage = (int) (max(indexInChunk, startIndexInChunk) - indexInChunk);
      int needCountInPage =
          (int)
              (min(indexInChunk + pageDataCount, startIndexInChunk + lengthInChunk)
                  - max(indexInChunk, startIndexInChunk));
      tvPairList.addAll(
          ((LinkedList) pageTVList).subList(beginIdxInPage, beginIdxInPage + needCountInPage));

      // tsFileFullSeqReader.position(pagePosInTsfile + pageSize);
      indexInChunk += pageDataCount;
    }

    return tvPairList.size();
  }

  /**
   * Parse 1 normal(not Time-Aligned) page to get all data points.
   *
   * <p>Note: *
   *
   * <p>1) New data will be appended to parameter tvPairList for better performance.
   *
   * <p>2) The deleted data points by .mods will be set to null.
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
    List<TimeValuePair> pageTVList = new LinkedList<>();

    ByteBuffer pageData =
        tsFileFullSeqReader.readPage(pageHeader, chunkHeader.getCompressionType());

    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    if (chunkHeader.getChunkType() == MetaMarker.CHUNK_HEADER) {
      logger.debug("points in the page(by pageHeader): {}", pageHeader.getNumOfValues());
    } else {
      logger.debug("points in the page(by batchData): {}", batchData.length());
    }

    if (batchData.isEmpty()) {
      logger.warn("getNonAlignedChunkPoints(), chunk is empty, chunkHeader = {}.", chunkHeader);
      return pageTVList;
    }

    batchData.resetBatchData();
    DeletionGroup.IntervalCursor intervalCursor = new DeletionGroup.IntervalCursor();
    while (batchData.hasCurrent()) {
      long ts = batchData.currentTime();
      if ((deletionGroup != null) && (deletionGroup.isDeleted(ts, intervalCursor))) {
        pageTVList.add(null);
      } else {
        TimeValuePair timeValuePair = new TimeValuePair(ts, batchData.currentTsPrimitiveType());
        logger.debug("getNonAlignedChunkPoints(), timeValuePair = {} ", timeValuePair);
        pageTVList.add(timeValuePair);
      }

      batchData.next();
    }

    return pageTVList;
  }

  /**
   * Get the data of Time-Aligned Value page.
   *
   * @param chunkInfo The chunk-info that value page exists in.
   * @param pageIndexInChunk needed page's index in value chunk, 0 mean the first page of this
   *     chunk.
   * @param timeBatch timeStamp info got from Time-Aligned time page
   * @param deletionGroup If it is not null, need to check whether data points have benn deleted
   * @return The data list of this value page.
   * @throws IOException
   */
  private List<TimeValuePair> getAlignedValuePagePoints(
      ChunkInfo chunkInfo, int pageIndexInChunk, long[] timeBatch, DeletionGroup deletionGroup)
      throws IOException {

    if (chunkInfo.chunkOffsetInFile < 0) {
      String errMsg =
          String.format(
              "getAlignedValuePagePoints(), invalid chunkOffsetInFile=%d.",
              chunkInfo.chunkOffsetInFile);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }

    tsFileFullSeqReader.position(chunkInfo.chunkOffsetInFile);
    byte valueChunkTypeByte = tsFileFullSeqReader.readMarker();
    ChunkHeader valueChunkHeader = tsFileFullSeqReader.readChunkHeader(valueChunkTypeByte);

    byte chunkType = valueChunkHeader.getChunkType();
    // == If this chunk is not time-aligned value chunk
    if ((chunkType & (byte) TsFileConstant.VALUE_COLUMN_MASK)
        != (byte) TsFileConstant.VALUE_COLUMN_MASK) {
      String errMsg =
          String.format(
              "getAlignedValuePagePoints(), tsFile(%s) current chunk is not time-aligned value chunk. chunkOffsetInFile=%d.",
              tsFileName, chunkInfo.chunkOffsetInFile);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }

    boolean pageHeaderHasStatistic = ((chunkType & 0x3F) == MetaMarker.CHUNK_HEADER);
    if ((!pageHeaderHasStatistic) && (pageIndexInChunk != 0)) {
      String errMsg =
          String.format(
              "getAlignedValuePagePoints(), current value chunk has only 1 page, but pageIndexInChunk=%d.",
              pageIndexInChunk);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }

    int chunkLeftDataSize = valueChunkHeader.getDataSize();

    // == skip the first (pageIndexInChunk) pages.
    int k = 0;
    TSDataType tsDataType = valueChunkHeader.getDataType();
    while ((chunkLeftDataSize > 0) && (k++ < pageIndexInChunk)) {
      long pagePosInTsFile = tsFileFullSeqReader.position();
      PageHeader pageHeader =
          tsFileFullSeqReader.readPageHeader(tsDataType, pageHeaderHasStatistic);

      int pageSize = pageHeader.getSerializedPageSize();
      tsFileFullSeqReader.position(pagePosInTsFile + pageSize);
      chunkLeftDataSize -= pageSize;
    }

    if (chunkLeftDataSize <= 0) {
      String errMsg =
          String.format(
              "getAlignedValuePagePoints(), current value chunk has only %d pages, but pageIndexInChunk=%d.",
              k, pageIndexInChunk);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }

    PageHeader pageHeader = tsFileFullSeqReader.readPageHeader(tsDataType, pageHeaderHasStatistic);
    ByteBuffer pageData =
        tsFileFullSeqReader.readPage(pageHeader, valueChunkHeader.getCompressionType());
    Decoder valueDecoder = Decoder.getDecoderByType(valueChunkHeader.getEncodingType(), tsDataType);
    ValuePageReader valuePageReader =
        new ValuePageReader(pageHeader, pageData, tsDataType, valueDecoder);
    TsPrimitiveType[] valueBatch = valuePageReader.nextValueBatch(timeBatch);

    if (timeBatch.length != valueBatch.length) {
      String errMsg =
          String.format(
              "getAlignedValuePagePoints(), timeBatch & valueBatch have different length. %d != %d.",
              timeBatch.length, valueBatch.length);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }

    List<TimeValuePair> pageTVList = new LinkedList<>();
    DeletionGroup.IntervalCursor intervalCursor = new DeletionGroup.IntervalCursor();
    for (int i = 0; i < timeBatch.length; i++) {
      long ts = timeBatch[i];
      if ((valueBatch[i] == null)
          || ((deletionGroup != null) && (deletionGroup.isDeleted(ts, intervalCursor)))) {
        pageTVList.add(null);
      } else {
        TimeValuePair timeValuePair = new TimeValuePair(ts, valueBatch[i]);
        logger.debug("getNonAlignedChunkPoints(), timeValuePair = {} ", timeValuePair);
        pageTVList.add(timeValuePair);
      }
    }

    return pageTVList;
  }

  /**
   * Parse 1 time-aligned value chunk to get partial data points according to startIndexInChunk &
   * lengthInChunk.
   *
   * <p>Note:
   *
   * <p>1) New data will be appended to parameter tvPairList for better performance
   *
   * <p>2) Parsing value chunk will automatically involve time chunk's info.
   *
   * @param chunkInfo - The info of 1 time-aligned value chunk
   * @param startIndexInChunk
   * @param lengthInChunk
   * @param deletionGroup - If it is not null, need to check whether data points have benn deleted
   * @param tvPairList
   * @return
   * @throws IOException
   */
  private long getAlignedChunkPoints(
      ChunkInfo chunkInfo,
      long startIndexInChunk,
      long lengthInChunk,
      DeletionGroup deletionGroup,
      List<TimeValuePair> tvPairList)
      throws IOException {

    if (chunkInfo.timeChunkOffsetInFile < 0) {
      String errMsg =
          String.format(
              "getAlignedChunkPoints(), invalid timeChunkOffsetInFile=%d.",
              chunkInfo.timeChunkOffsetInFile);
      logger.error(errMsg);
      throw new IOException(errMsg);
    }
    tsFileFullSeqReader.position(chunkInfo.timeChunkOffsetInFile);
    byte timeChunkTypeByte = tsFileFullSeqReader.readMarker();
    ChunkHeader timeChunkHeader = tsFileFullSeqReader.readChunkHeader(timeChunkTypeByte);

    TSDataType tsDataType = timeChunkHeader.getDataType();
    boolean pageHeaderHasStatistic =
        ((timeChunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
    int chunkLeftDataSize = timeChunkHeader.getDataSize();
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);

    long indexInChunk = 0;
    int pageIndexInChunk = -1;
    while ((chunkLeftDataSize > 0) && (indexInChunk < (startIndexInChunk + lengthInChunk))) {
      // == Begin to traverse every time+value page
      long timePagePosInTsfile = tsFileFullSeqReader.position();
      PageHeader timePageHeader =
          tsFileFullSeqReader.readPageHeader(tsDataType, pageHeaderHasStatistic);

      int timePageSize = timePageHeader.getSerializedPageSize();
      chunkLeftDataSize -= timePageSize;
      pageIndexInChunk++;

      long pageStartIndexInFile = chunkInfo.startIndexInFile + indexInChunk;

      // == At first, try to find page data from pageCache.
      long pageDataNum =
          getDataFromPageCache(
              pageStartIndexInFile,
              chunkInfo.startIndexInFile + startIndexInChunk,
              lengthInChunk,
              tvPairList);
      if (pageDataNum >= 0) { // find page in page cache
        tsFileFullSeqReader.position(timePagePosInTsfile + timePageSize);
        indexInChunk += pageDataNum;
        continue;
      }

      // Note: Here, we can not use pageHeaderHasStatistic to do judge.
      // Because, even if pageHeaderHasStatistic == true,
      // pageHeader.getStatistics() can still be null when page uncompressedSize == 0 (empty page).
      if (timePageHeader.getStatistics() != null) {
        // == check whether whole time page is out of  [startIndexInChunk, startIndexInChunk +
        // lengthInChunk)
        long pageDataCount = timePageHeader.getNumOfValues();
        if ((indexInChunk + pageDataCount) <= startIndexInChunk) { // skip this page
          tsFileFullSeqReader.position(timePagePosInTsfile + timePageSize);
          indexInChunk += pageDataCount;
          continue;
        }

        // == check whether whole page has been deleted by .mods file
        if (deletionGroup != null) { // have deletion related to current chunk
          DeletionGroup.DeletedType pageDeletedStatus =
              deletionGroup.checkDeletedState(
                  timePageHeader.getStartTime(), timePageHeader.getEndTime());
          if (pageDeletedStatus == FULL_DELETED) {
            // == put page data to page-cache
            List<TimeValuePair> pageTVList = new LinkedList<>();
            for (long i = 0; i < pageDataCount; i++) {
              pageTVList.add(null);
            }
            pageCache.put(pageStartIndexInFile, pageTVList);

            // == gen requested TV data list
            long needCount =
                min(indexInChunk + pageDataCount, startIndexInChunk + lengthInChunk)
                    - max(indexInChunk, startIndexInChunk);
            for (long i = 0; i < needCount; i++) {
              tvPairList.add(null);
            }

            tsFileFullSeqReader.position(timePagePosInTsfile + timePageSize);
            indexInChunk += pageDataCount;
            continue;
          }
        }
      }

      // == read time page data from Tsfile (for pageIndexInChunk)
      ByteBuffer timePageData =
          tsFileFullSeqReader.readPage(timePageHeader, timeChunkHeader.getCompressionType());
      TimePageReader timePageReader =
          new TimePageReader(timePageHeader, timePageData, defaultTimeDecoder);
      long[] timeBatch = timePageReader.getNextTimeBatch();
      if (logger.isDebugEnabled()) {
        logger.debug("Time pager content: {}", timeBatch);
      }

      // == check whether whole time page is out of [startIndexInChunk, startIndexInChunk +
      // lengthInChunk)
      long timePageDataCount = timeBatch.length;
      if ((indexInChunk + timePageDataCount) <= startIndexInChunk) { // skip this page
        // == Here, not check (indexInChunk >= (startIndexInChunk + lengthInChunk)), because the
        // preceding  while(xxx) has checked it.
        tsFileFullSeqReader.position(timePagePosInTsfile + timePageSize);
        indexInChunk += timePageDataCount;
        continue;
      }

      // == read value page data from Tsfile (for pageIndexInChunk)
      List<TimeValuePair> pageTVList =
          getAlignedValuePagePoints(chunkInfo, pageIndexInChunk, timeBatch, deletionGroup);

      pageCache.put(pageStartIndexInFile, pageTVList);

      // == select needed data and append them to tvPairList
      int beginIdxInPage = (int) (max(indexInChunk, startIndexInChunk) - indexInChunk);
      int needCountInPage =
          (int)
              (min(indexInChunk + timePageDataCount, startIndexInChunk + lengthInChunk)
                  - max(indexInChunk, startIndexInChunk));

      tvPairList.addAll(
          ((LinkedList) pageTVList).subList(beginIdxInPage, beginIdxInPage + needCountInPage));

      tsFileFullSeqReader.position(timePagePosInTsfile + timePageSize);
      indexInChunk += timePageDataCount;
    }

    return tvPairList.size();
  }

  /**
   * Get 1 Chunk's partial data points according to indexInChunk & lengthInChunk.
   *
   * <p>Note: new data will be appended to parameter tvPairList for better performance
   *
   * @param chunkInfo
   * @param indexInChunk
   * @param lengthInChunk
   * @param tvPairList, the got data points will be appended to this List.
   * @return the number of returned data points
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

    DeletionGroup deletionGroup = null;
    if (chunkInfo.deletedFlag != NO_DELETED) {
      deletionGroup = getFullPathDeletion(chunkInfo.measurementFullPath);
    }

    if (chunkInfo.isTimeAligned) {
      return getAlignedChunkPoints(
          chunkInfo, indexInChunk, lengthInChunk, deletionGroup, tvPairList);
    } else {
      return getNonAlignedChunkPoints(
          chunkInfo, indexInChunk, lengthInChunk, deletionGroup, tvPairList);
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

  /* Prepare TsfileOpBlock's internal data before formal accessing. */
  private synchronized void prepareData() throws IOException {
    if (dataReady) {
      return;
    }

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

    long indexInTsFile = index - beginIndex;
    if (indexInTsFile < 0 || indexInTsFile >= dataCount) {
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
      Map.Entry<Long, ChunkInfo> entry = indexToChunkInfoMap.floorEntry(indexInTsFile);
      if (entry == null) {
        logger.error(
            "TsFileOpBlock.getOperation(), indexInTsFile {} if out of indexToChunkOffsetMap.",
            indexInTsFile);
        throw new IOException("indexInTsFile is out of range.");
      }

      long indexInChunk = indexInTsFile - entry.getKey();
      ChunkInfo chunkInfo = entry.getValue();
      String sensorFullPath = chunkInfo.measurementFullPath;
      long chunkPointCount = chunkInfo.pointCount;

      long lengthInChunk = min(chunkPointCount - indexInChunk, remain);

      if (!sensorFullPath.equals(lastSensorFullPath)) {
        if ((tvPairList != null) && (!tvPairList.isEmpty())) {
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
        String errMsg =
            String.format(
                "TsFileOpBlock.getOperation(), error when read chunk from file %s. indexInTsFile=%d, lengthInChunk=%d, readCount=%d.",
                tsFileName, indexInTsFile, lengthInChunk, readCount);
        logger.error(errMsg);
        throw new IOException(errMsg);
      }

      remain -= readCount;
      indexInTsFile = entry.getKey() + chunkPointCount; // next chunk's local index

      if (indexInTsFile >= dataCount) { // has reached the end of this Tsfile
        break;
      }
    }

    if ((tvPairList != null) && (!tvPairList.isEmpty())) {
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

    /**
     * @param file, Tsfile's full path name.
     * @throws IOException
     */
    public TsFileFullReader(String file) throws IOException {
      super(file);
    }

    /**
     * Generate timeseriesMetadataMap, Offset => (MeasurementID, TimeseriesMetadata)
     *
     * @param startOffset
     * @param metadataIndex
     * @param buffer
     * @param deviceId
     * @param type
     * @param timeseriesMetadataMap, output param
     * @param needChunkMetadata, input param, indicates whether need parser ChunkMetaData
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
                    new Path(deviceId, timeseriesMetadata.getMeasurementId(), true),
                    timeseriesMetadata));
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
     * @return Map, FileOffset => pair(MeasurementId => TimeseriesMetadata)
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
    @Override
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
    return super.toString() + ", tsFileName=" + tsFileName + ", modsFileName=" + modsFileName;
  }
}
