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
package org.apache.iotdb.db.newsync.datasource;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.pipe.external.operation.InsertOperation;
import org.apache.iotdb.db.pipe.external.operation.Operation;
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

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Math.min;

/** This class will parse 1 TsFile's content to 1 operation block. */
public class TsfilePipeOpBlock extends AbstractPipeOpBlock {
  private static final Logger logger = LoggerFactory.getLogger(TsfilePipeOpBlock.class);

  // tsFile name
  private String tsFileName;
  private TsFileFullReader tsFileFullSeqReader;

  // full Timeseries Metadata TreeMap : FileOffset => Pair(DeviceId, TimeseriesMetadata)
  private Map<Long, Pair<Path, TimeseriesMetadata>> fullTsMetadataMap;
  // TreeMap: LocalIndex => Pair(SensorFullPath, ChunkOffset, PointCount)
  private TreeMap<Long, Triple<String, Long, Long>> indexToChunkInfoMap;

  Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  public TsfilePipeOpBlock(String sg, String tsFileName, long pipeDataSerialNumber)
      throws IOException {
    this(sg, tsFileName, pipeDataSerialNumber, 0);
  }

  public TsfilePipeOpBlock(String sg, String tsFileName, long pipeDataSerialNumber, long beginIndex)
      throws IOException {
    super(sg, beginIndex);
    this.filePipeSerialNumber = pipeDataSerialNumber;
    this.tsFileName = tsFileName;
    init();
  }

  /**
   * Init TsfileDataSrcEntry's dataCount
   *
   * @throws IOException
   */
  private void init() throws IOException {
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

    // chunkOffset => pair(SensorFullPath, chunkPointCount)
    Map<Long, Pair<String, Long>> offsetToCountMap = new TreeMap<>();
    for (Pair<Path, TimeseriesMetadata> value : fullTsMetadataMap.values()) {
      List<IChunkMetadata> chunkMetaList = value.right.getChunkMetadataList();

      if (chunkMetaList == null) {
        continue;
      }

      for (IChunkMetadata chunkMetadata : chunkMetaList) {
        // traverse every chunk
        long chunkOffset = chunkMetadata.getOffsetOfChunkHeader();
        long chunkPointCount = chunkMetadata.getStatistics().getCount();
        String sensorFullPath = value.left.getFullPath();
        ;
        offsetToCountMap.put(chunkOffset, new Pair<>(sensorFullPath, chunkPointCount));
      }
    }

    indexToChunkInfoMap = new TreeMap<>();
    long localIndex = 0;
    for (Map.Entry<Long, Pair<String, Long>> entry : offsetToCountMap.entrySet()) {
      Long chunkHeaderOffset = entry.getKey();
      Long pointCount = entry.getValue().right;
      String sensorFullPath = entry.getValue().left;
      indexToChunkInfoMap.put(
          localIndex, new ImmutableTriple<>(sensorFullPath, chunkHeaderOffset, pointCount));
      localIndex += pointCount;
    }
  }

  /**
   * Parse 1 NonAligned Chunk to get all data points. Note: new data will be appended to parameter
   * tvPairList for better performance
   *
   * @param chunkHeader
   * @param indexInChunk
   * @param length
   * @param tvPairList
   * @return
   * @throws IOException
   */
  private long getNonAlignedChunkPoints(
      ChunkHeader chunkHeader, long indexInChunk, long length, List<TimeValuePair> tvPairList)
      throws IOException {
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    int chunkDataSize = chunkHeader.getDataSize();

    int index = 0;
    while (chunkDataSize > 0) {
      // begin to traverse every page
      long filePos = tsFileFullSeqReader.position();
      boolean hasStatistic = ((chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      PageHeader pageHeader =
          tsFileFullSeqReader.readPageHeader(chunkHeader.getDataType(), hasStatistic);

      int pageSize = pageHeader.getSerializedPageSize();
      chunkDataSize -= pageSize;

      if (hasStatistic) {
        long pageDataCount = pageHeader.getNumOfValues();
        if ((index + pageDataCount) < indexInChunk) { // skip this page
          tsFileFullSeqReader.position(filePos + pageSize);
          index += pageDataCount;
          continue;
        }
      }

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
      }

      batchData.resetBatchData();
      while (batchData.hasCurrent()) {
        if (index++ >= indexInChunk) {
          TimeValuePair timeValuePair =
              new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
          logger.debug("getNonAlignedChunkPoints(), timeValuePair = {} ", timeValuePair);
          tvPairList.add(timeValuePair);
        }
        if ((index - indexInChunk) >= length) { // data point is enough
          return (index - indexInChunk);
        }
        batchData.next();
      }
    }

    return (index - indexInChunk);
  }

  /**
   * Parse 1 Aligned Chunk to get all data points. Note: new data will be appended to parameter
   * tvPairList for better performance
   *
   * @param chunkHeader
   * @param indexInChunk
   * @param lengthInChunk
   * @param tvPairList
   * @return
   * @throws IOException
   */
  private long getAlignedChunkPoints(
      ChunkHeader chunkHeader,
      long indexInChunk,
      long lengthInChunk,
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
   * get 1 Chunk's partial data points according to indexInChunk & length Note: new data will be
   * appended to parameter tvPairList for better performance
   *
   * @param chunkHeaderOffset
   * @param indexInChunk
   * @param length
   * @param tvPairList, the got data points will be appended to this List.
   * @return
   * @throws IOException
   */
  private long getChunkPoints(
      long chunkHeaderOffset, long indexInChunk, long length, List<TimeValuePair> tvPairList)
      throws IOException {
    tsFileFullSeqReader.position(chunkHeaderOffset);
    byte chunkTypeByte = tsFileFullSeqReader.readMarker();
    ChunkHeader chunkHeader = tsFileFullSeqReader.readChunkHeader(chunkTypeByte);

    if (chunkHeader.getDataType() == TSDataType.VECTOR) {
      return getAlignedChunkPoints(chunkHeader, indexInChunk, length, tvPairList);
    } else {
      return getNonAlignedChunkPoints(chunkHeader, indexInChunk, length, tvPairList);
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
      logger.error("TsfileDataSrcEntry.insertToDataList(), Illegal MeasurementPath: {}", "");
      throw new IOException("Illegal MeasurementPath: " + sensorFullPath, e);
    }
    dataList.add(new Pair<>(measurementPath, tvPairList));
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
    long indexInTsfile = index - beginIndex;

    if (indexInTsfile < 0 || indexInTsfile >= dataCount) {
      logger.error("TsfileDataSrcEntry.getOperation(), index {} is out of range.", index);
      throw new IOException("index is out of range.");
    }

    if (tsFileFullSeqReader == null) {
      tsFileFullSeqReader = new TsFileFullReader(tsFileName);
    }

    if (indexToChunkInfoMap == null) {
      buildIndexToChunkMap();
    }

    LinkedList<Pair<MeasurementPath, List<TimeValuePair>>> dataList = new LinkedList<>();
    String lastSensorFullPath = "";
    List<TimeValuePair> tvPairList = null;

    // handle all chunks that contain needed data
    long remain = length;
    while (remain > 0) {
      Map.Entry<Long, Triple<String, Long, Long>> entry =
          indexToChunkInfoMap.floorEntry(indexInTsfile);
      if (entry == null) {
        logger.error(
            "TsfileDataSrcEntry.getOperation(), indexInTsfile {} if out of indexToChunkOffsetMap.",
            indexInTsfile);
        throw new IOException("indexInTsfile is out of range.");
      }

      Long indexInChunk = indexInTsfile - entry.getKey();
      String sensorFullPath = entry.getValue().getLeft();
      Long chunkHeaderOffset = entry.getValue().getMiddle();
      Long chunkPointCount = entry.getValue().getRight();

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
      long daltaCount = getChunkPoints(chunkHeaderOffset, indexInChunk, lengthInChunk, tvPairList);
      if (daltaCount != lengthInChunk) {
        logger.error(
            "TsfileDataSrcEntry.getOperation(), error when read chunk from file {}. lengthInChunk={}, daltaCount={}, ",
            indexInTsfile,
            lengthInChunk,
            daltaCount);
        throw new IOException("Error read chunk from file:" + indexInTsfile);
      }

      remain -= daltaCount;
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
            TsFileMetadata.deserializeFrom(readData(getFileMetadataPos(), getFileMetadataSize()));
      } catch (BufferOverflowException e) {
        logger.error("readFileMetadata(), reading file metadata of file {}", file);
        throw e;
      }
      return tsFileMetaData;
    }
  }
}
