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
 */
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.DeviceTimeseriesMetadataRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter implements AutoCloseable {

  protected static final byte[] MAGIC_STRING_BYTES;
  public static final byte VERSION_NUMBER_BYTE;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    VERSION_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
  }

  protected TsFileOutput tsFileOutput;
  //  protected TsFileOutput tmpFileOutput;
  protected TsFileOutput indexFileOutput;
  protected boolean canWrite = true;
  protected File file;

  // current flushed Chunk
  private ChunkMetadata currentChunkMetadata;
  // current flushed ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();

  private long markedPosition;
  private String currentChunkGroupDeviceId;

  // convert ChunkMetadataList to this field; also for upgrade tool and split tool
  // device -> {measurementId -> TimeseriesMetadataRecord}
  Map<String, DeviceTimeseriesMetadataRecord> deviceTimeseriesMetadataMap = new TreeMap<>();
  Set<Path> paths = new HashSet<>();

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  private long minPlanIndex;
  private long maxPlanIndex;

  /** empty construct function. */
  protected TsFileIOWriter() {}

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.tsFileOutput =
        FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), false);

    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.tsFileOutput = output;
    startFile();
  }

  /** for test only */
  public TsFileIOWriter(TsFileOutput tsFileOutput, TsFileOutput indexFileOutput, boolean test) {
    this.tsFileOutput = tsFileOutput;
    this.indexFileOutput = indexFileOutput;
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(tsFileOutput.wrapAsStream());
  }

  public void startFile() throws IOException {
    tsFileOutput.write(MAGIC_STRING_BYTES);
    tsFileOutput.write(VERSION_NUMBER_BYTE);
  }

  public void startIndexFile() throws IOException {
    indexFileOutput =
        FSFactoryProducer.getFileOutputFactory()
            .getTsFileOutput(file.getPath() + TsFileConstant.INDEX_SUFFIX, false);
    indexFileOutput.write(MAGIC_STRING_BYTES);
    indexFileOutput.write(VERSION_NUMBER_BYTE);
  }

  public void startChunkGroup(String deviceId) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("start chunk group:{}, file position {}", deviceId, tsFileOutput.getPosition());
    }
    currentChunkGroupDeviceId = deviceId;
    chunkMetadataList = new ArrayList<>();
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup() throws IOException {
    if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }

    chunkGroupMetadataList.add(
        new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList));

    // group ChunkMetadata by series
    Map<Path, ArrayList<IChunkMetadata>> chunkMetadataListMap = new TreeMap<>();
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      Path series = new Path(currentChunkGroupDeviceId, chunkMetadata.getMeasurementUid());
      paths.add(series);
      chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
    }

    // flush marker and deviceId to tmp file
    //    ChunkGroupHeader chunkGroupHeader = new ChunkGroupHeader(currentChunkGroupDeviceId);
    //    chunkGroupHeader.serializeTo(tmpFileOutput.wrapAsStream());

    for (Map.Entry<Path, ArrayList<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      // for ordinary path
      flushOneChunkMetadata(entry.getKey(), entry.getValue());
    }

    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    // V3-2: flush both tsFileOutput and indexFileOutput when ending chunkGroup
    tsFileOutput.flush();
    //    tmpFileOutput.flush();
  }

  /**
   * For TsFileReWriteTool / UpgradeTool. Use this method to determine if needs to start a
   * ChunkGroup.
   *
   * @return isWritingChunkGroup
   */
  public boolean isWritingChunkGroup() {
    return currentChunkGroupDeviceId != null;
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementId - measurementId of this time series
   * @param compressionType - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @param mask - 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(
      String measurementId,
      CompressionType compressionType,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics,
      int dataSize,
      int numOfPages,
      int mask)
      throws IOException {

    currentChunkMetadata =
        new ChunkMetadata(
            measurementId,
            tsDataType,
            tsFileOutput.getPosition(), // start offset of chunk
            statistics,
            dataSize,
            compressionType,
            encodingType,
            numOfPages,
            mask);
    currentChunkMetadata.setMask((byte) mask);
  }

  /** Write a whole chunk in another file into this file. Providing fast merge for IoTDB. */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    currentChunkMetadata =
        new ChunkMetadata(
            chunkMetadata.getMeasurementUid(),
            chunkMetadata.getDataType(),
            tsFileOutput.getPosition(),
            chunkMetadata.getStatistics(),
            chunkMetadata.getDataSize(),
            chunkMetadata.getCompressionType(),
            chunkMetadata.getEncodingType(),
            chunkMetadata.getNumOfPages(),
            chunkMetadata.getMask());
    tsFileOutput.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "end flushing a chunk:{}, total value:{}",
          chunkMetadata.getMeasurementUid(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /** end chunk and write some log. */
  public void endCurrentChunk() {
    chunkMetadataList.add(currentChunkMetadata);
    currentChunkMetadata = null;
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void endFile() throws IOException {
    startIndexFile();

    MetadataIndexNode metadataIndex =
        MetadataIndexConstructor.constructMetadataIndex(
            deviceTimeseriesMetadataMap, indexFileOutput);
    TsFileMetadata tsFileMetaData = new TsFileMetadata(metadataIndex);

    long footerIndex = indexFileOutput.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write TsFileMetaData
    int size = tsFileMetaData.serializeTo(indexFileOutput.wrapAsStream());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "finish flushing the footer {}, file pos:{}",
          tsFileMetaData,
          indexFileOutput.getPosition());
    }

    // write bloom filter
    size += tsFileMetaData.serializeBloomFilter(indexFileOutput.wrapAsStream(), paths);
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", indexFileOutput.getPosition());
    }

    // write TsFileMetaData size
    ReadWriteIOUtils.write(
        size, indexFileOutput.wrapAsStream()); // write the size of the file metadata.

    // write magic string
    tsFileOutput.write(MAGIC_STRING_BYTES);
    indexFileOutput.write(MAGIC_STRING_BYTES);

    // close file
    tsFileOutput.close();
    indexFileOutput.close();
    //    tmpFileOutput.close();

    //    boolean isTmpFileDeleted =
    //        FSFactoryProducer.getFSFactory()
    //            .getFile(file.getPath() + TsFileConstant.INDEX_TMP_SUFFIX)
    //            .delete();

    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
      //      resourceLogger.debug("tmp file is deleted {}.", isTmpFileDeleted ? "successfully" :
      // "FAILED");
    }
    canWrite = false;
  }

  /**
   * Flush one chunkMetadata
   *
   * @param path Path of chunk
   * @param chunkMetadataList List of chunkMetadata about path(previous param)
   */
  private void flushOneChunkMetadata(Path path, ArrayList<IChunkMetadata> chunkMetadataList)
      throws IOException {
    // create TimeseriesMetadata
    String deviceId = path.getDeviceIdString();
    String measurementId = path.getMeasurement();

    TimeseriesMetadata timeseriesMetadata = null;

    new TimeseriesMetadata(chunkMetadataList);

    // update timeseriesMetadata
    if (deviceTimeseriesMetadataMap.containsKey(deviceId)
        && deviceTimeseriesMetadataMap.get(deviceId).containsMeasurement(measurementId)) {
      timeseriesMetadata =
          deviceTimeseriesMetadataMap.get(deviceId).getStoredStatistics(measurementId);
    }
    if (timeseriesMetadata == null) {
      timeseriesMetadata = new TimeseriesMetadata(chunkMetadataList);
    } else {
      timeseriesMetadata.mergeTimeseriesMetadata(new TimeseriesMetadata(chunkMetadataList));
    }

    // serialize to tmp file
    //    timeseriesMetadata.serializeTo(tmpFileOutput.wrapAsStream());

    deviceTimeseriesMetadataMap
        .computeIfAbsent(deviceId, k -> new DeviceTimeseriesMetadataRecord())
        .addMeasurementTimeseriesMetadataRecord(measurementId, timeseriesMetadata);
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return tsFileOutput.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      deviceChunkMetadataMap
          .computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    tsFileOutput.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    tsFileOutput.close();
    indexFileOutput.close();
  }

  public void writeSeparatorMaskForTest() throws IOException {
    tsFileOutput.write(new byte[] {MetaMarker.SEPARATOR});
  }

  public void writeChunkGroupMarkerForTest() throws IOException {
    indexFileOutput.write(new byte[] {MetaMarker.CHUNK_GROUP_HEADER});
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  /** Remove such ChunkMetadata that its startTime is not in chunkStartTimes */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetadata> chunkGroupMetaDataIterator = chunkGroupMetadataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetadata chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDevice();
      int chunkNum = chunkGroupMetaData.getChunkMetadataList().size();
      Iterator<ChunkMetadata> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetadataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        IChunkMetadata chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid =
            startTimeIdx < pathChunkStartTimes.size()
                && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }

  public void writePlanIndices() throws IOException {
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, tsFileOutput.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, tsFileOutput.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, tsFileOutput.wrapAsStream());
    tsFileOutput.flush();
  }

  public void truncate(long offset) throws IOException {
    tsFileOutput.truncate(offset);
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return tsFileOutput;
  }

  /**
   * this function is for Upgrade Tool and Split Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<String, DeviceTimeseriesMetadataRecord> getDeviceTimeseriesMetadataMap() {
    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }
}
