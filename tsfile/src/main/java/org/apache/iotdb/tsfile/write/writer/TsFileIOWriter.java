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
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter {

  public static final byte[] magicStringBytes;
  public static final byte[] versionNumberBytes;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  static {
    magicStringBytes = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    versionNumberBytes = TSFileConfig.VERSION_NUMBER.getBytes();
  }

  protected TsFileOutput out;
  protected List<Pair<String, List<ChunkMetadata>>> chunkGroupInfoList = new ArrayList<>();
  protected boolean canWrite = true;
  protected int totalChunkNum = 0;
  protected int invalidChunkNum;
  protected File file;
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  protected Map<Path, List<ChunkMetadata>> chunkMetadataListMap = new TreeMap<>();
  private ChunkMetadata currentChunkMetadata;
  private long markedPosition;
  private String deviceId;
  private long currentChunkGroupStartOffset;
  private Map<Long, Long> versionInfo = new HashMap<>();

  /**
   * empty construct function.
   */
  protected TsFileIOWriter() {

  }

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.out = new DefaultTsFileOutput(file);
    this.file = file;
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    startFile();
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile() throws IOException {
    out.write(magicStringBytes);
    out.write(versionNumberBytes);
  }

  /**
   * start a {@linkplain ChunkGroupMetaData ChunkGroupMetaData}.
   *
   * @param deviceId device id
   */
  public void startChunkGroup(String deviceId) throws IOException {
    this.deviceId = deviceId;
    currentChunkGroupStartOffset = out.getPosition();
    logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    chunkMetadataList = new ArrayList<>();
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup(long version) throws IOException {
    if (deviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }
    long dataSize = out.getPosition() - currentChunkGroupStartOffset;
    ChunkGroupFooter chunkGroupFooter = new ChunkGroupFooter(deviceId, dataSize,
        chunkMetadataList.size());
    chunkGroupFooter.serializeTo(out.wrapAsStream());
    chunkGroupInfoList.add(new Pair<>(deviceId, chunkMetadataList));
    versionInfo.put(out.getPosition(), version);
    logger.debug("end chunk group:{}", chunkMetadataList);
    deviceId = null;
    chunkMetadataList = null;
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param MeasurementSchema     - schema of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType           - data type
   * @param statistics           - Chunk statistics
   * @param dataSize             - the serialized size of all pages
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(MeasurementSchema measurementSchema,
      CompressionType compressionCodecName,
      TSDataType tsDataType, TSEncoding encodingType, Statistics<?> statistics, int dataSize,
      int numOfPages)
      throws IOException {

    currentChunkMetadata = new ChunkMetadata(measurementSchema.getMeasurementId(), tsDataType,
        out.getPosition(),
        statistics);

    // flush ChunkHeader to TsFileIOWriter
    if (logger.isDebugEnabled()) {
      logger.debug("start series chunk:{}, file position {}", measurementSchema, out.getPosition());
    }

    ChunkHeader header = new ChunkHeader(measurementSchema.getMeasurementId(), dataSize, tsDataType,
        compressionCodecName, encodingType, numOfPages);
    header.serializeTo(out.wrapAsStream());

    if (logger.isDebugEnabled()) {
      logger.debug("finish series chunk:{} header, file position {}", header, out.getPosition());
    }
  }

  /**
   * Write a whole chunk in another file into this file. Providing fast merge for IoTDB.
   */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata = new ChunkMetadata(chunkHeader.getMeasurementID(),
        chunkHeader.getDataType(),
        out.getPosition(), chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    logger.debug("end flushing a chunk:{}, totalvalue:{}", currentChunkMetadata,
        chunkMetadata.getNumOfPoints());
  }

  /**
   * end chunk and write some log.
   */
  public void endCurrentChunk() {
    chunkMetadataList.add(currentChunkMetadata);
    Path path = new Path(deviceId, currentChunkMetadata.getMeasurementUid());
    chunkMetadataListMap.computeIfAbsent(path, k -> new ArrayList<>()).add(currentChunkMetadata);
    currentChunkMetadata = null;
    totalChunkNum++;
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @param schema Schema
   * @throws IOException if I/O error occurs
   */
  public void endFile() throws IOException {

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());
    
    logger.debug("get time series list:{}", chunkMetadataListMap.keySet());
    
    Map<String, Pair<Long, Integer>> deviceMetaDataMap = flushAllChunkMetadataList();
    
    TsFileMetadata tsFileMetaData = new TsFileMetadata();
    tsFileMetaData.setDeviceMetadataMap(deviceMetaDataMap);
    tsFileMetaData.setVersionInfo(versionInfo);
    tsFileMetaData.setTotalChunkNum(totalChunkNum);
    tsFileMetaData.setInvalidChunkNum(invalidChunkNum);

    long footerIndex = out.getPosition();
    logger.debug("start to flush the footer,file pos:{}", footerIndex);

    // write TsFileMetaData
    int size = tsFileMetaData.serializeTo(out.wrapAsStream());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());
    }

    // write bloom filter
    size += tsFileMetaData.serializeBloomFilter(out.wrapAsStream(), chunkMetadataListMap.keySet());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", out.getPosition());
    }

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());// write the size of the file metadata.

    // write magic string
    out.write(magicStringBytes);

    // close file
    out.close();
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} writer is closed.", file.getName());
    }
    canWrite = false;
    chunkMetadataListMap = new TreeMap<>();
    logger.info("output stream is closed");
  }

  /**
   * Flush ChunkMetadataList and TimeseriesMetaData
   * @return DeviceMetaDataMap in TsFileMetaData
   */
  private Map<String, Pair<Long, Integer>> flushAllChunkMetadataList() throws IOException {

    // convert ChunkMetadataList to this field
    Map<String, List<TimeseriesMetaData>> deviceTimeseriesMetadataMap = new LinkedHashMap<>();
    // create device -> TimeseriesMetaDataList Map
    for (Map.Entry<Path, List<ChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path path = entry.getKey();
      String deviceId = path.getDevice();
      // create TimeseriesMetaData
      TimeseriesMetaData timeseriesMetaData = new TimeseriesMetaData();
      timeseriesMetaData.setMeasurementId(path.getMeasurement());
      timeseriesMetaData.setTSDataType(entry.getValue().get(0).getDataType());
      timeseriesMetaData.setOffsetOfChunkMetaDataList(out.getPosition());
      Statistics<?> statistics = entry.getValue().get(0).getStatistics();
      int chunkMetadataListLength = 0;
      // flush chunkMetadataList one by one
      for (ChunkMetadata chunkMetadata : entry.getValue()) {
        statistics.mergeStatistics(chunkMetadata.getStatistics());
        chunkMetadataListLength += chunkMetadata.serializeTo(out.wrapAsStream());
      }
      timeseriesMetaData.setStatistics(statistics);
      timeseriesMetaData.setDataSizeOfChunkMetaDataList(chunkMetadataListLength);
      deviceTimeseriesMetadataMap.computeIfAbsent(deviceId, k -> new ArrayList<>())
          .add(timeseriesMetaData);
    }
    // create DeviceMetaDataMap device -> Pair<TimeseriesMetaDataOffset, TimeseriesMetaDataLength> 
    Map<String, Pair<Long, Integer>> deviceMetadataMap = new HashMap<>();
    for (Map.Entry<String, List<TimeseriesMetaData>> entry : deviceTimeseriesMetadataMap
        .entrySet()) {
      String deviceId = entry.getKey();
      List<TimeseriesMetaData> timeseriesMetaDataList = entry.getValue();
      long offsetOfFirstTimeseriesMetaDataInDevice = out.getPosition();
      int size = 0;
      for (TimeseriesMetaData timeseriesMetaData : timeseriesMetaDataList) {
        size += timeseriesMetaData.serializeTo(out.wrapAsStream());
      }
      deviceMetadataMap
          .put(deviceId, new Pair<Long, Integer>(offsetOfFirstTimeseriesMetaDataInDevice, size));
    }
    // return
    return deviceMetadataMap;
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return out.getPosition();
  }

  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();
    for (Map.Entry<Path, List<ChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path path = entry.getKey();
      String deviceId = path.getDevice();
      deviceChunkMetadataMap.computeIfAbsent(deviceId, k -> new ArrayList<>())
      .addAll(entry.getValue());
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
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[]{MetaMarker.SEPARATOR});
  }

  void writeChunkMaskForTest() throws IOException {
    out.write(new byte[]{MetaMarker.CHUNK_HEADER});
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  public int getInvalidChunkNum() {
    return invalidChunkNum;
  }

  public File getFile() {
    return file;
  }

  /**
   * Remove such ChunkMetadata that its startTime is not in chunkStartTimes
   */

  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    for (Map.Entry<Path, List<ChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      Path path = entry.getKey();
      int chunkNum = chunkMetadataList.size();
      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        int startTimeIdx = startTimeIdxes.get(path);
        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid = startTimeIdx < pathChunkStartTimes.size()
            && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetadataList.remove(chunkMetaData);
          chunkNum--;
          invalidChunkNum++;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkMetadataListMap.remove(path);
      }
    }
  }


  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return this.out;
  }
}
