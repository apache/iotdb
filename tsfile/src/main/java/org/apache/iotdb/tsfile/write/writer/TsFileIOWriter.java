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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter {

  public static final byte[] magicStringBytes;
  public static final byte[] versionNumberBytes;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);

  static {
    magicStringBytes = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    versionNumberBytes = TSFileConfig.VERSION_NUMBER.getBytes();
  }

  protected TsFileOutput out;
  protected List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
  protected boolean canWrite = true;
  protected int totalChunkNum = 0;
  protected int invalidChunkNum;
  protected File file;
  private ChunkGroupMetaData currentChunkGroupMetaData;
  private ChunkMetaData currentChunkMetaData;
  private long markedPosition;

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
    logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    currentChunkGroupMetaData = new ChunkGroupMetaData(deviceId, new ArrayList<>(),
        out.getPosition());
  }

  /**
   * end chunk and write some log.
   * If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup(long version) throws IOException {
    if (currentChunkGroupMetaData == null || currentChunkGroupMetaData.getChunkMetaDataList().isEmpty()) {
      return;
    }
    long dataSize = out.getPosition() - currentChunkGroupMetaData.getStartOffsetOfChunkGroup();
    ChunkGroupFooter chunkGroupFooter = new ChunkGroupFooter(
        currentChunkGroupMetaData.getDeviceID(),
        dataSize, currentChunkGroupMetaData.getChunkMetaDataList().size());
    chunkGroupFooter.serializeTo(out.wrapAsStream());
    currentChunkGroupMetaData.setEndOffsetOfChunkGroup(out.getPosition());
    currentChunkGroupMetaData.setVersion(version);
    chunkGroupMetaDataList.add(currentChunkGroupMetaData);
    logger.debug("end chunk group:{}", currentChunkGroupMetaData);
    currentChunkGroupMetaData = null;
  }

  /**
   * start a {@linkplain ChunkMetaData ChunkMetaData}.
   *
   * @param descriptor - measurement of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(MeasurementSchema descriptor, CompressionType compressionCodecName,
      TSDataType tsDataType, TSEncoding encodingType, Statistics<?> statistics,
      int dataSize, int numOfPages) throws IOException {

    currentChunkMetaData = new ChunkMetaData(descriptor.getMeasurementId(), tsDataType,
        out.getPosition(), statistics);

    // flush ChunkHeader to TsFileIOWriter
    if (logger.isDebugEnabled()) {
      logger.debug("start series chunk:{}, file position {}", descriptor, out.getPosition());
    }

    ChunkHeader header = new ChunkHeader(descriptor.getMeasurementId(), dataSize, tsDataType,
        compressionCodecName, encodingType, numOfPages);
    header.serializeTo(out.wrapAsStream());

    if (logger.isDebugEnabled()) {
      logger.debug("finish series chunk:{} header, file position {}", header, out.getPosition());
    }
  }

  /**
   * Write a whole chunk in another file into this file. Providing fast merge for IoTDB.
   */
  public void writeChunk(Chunk chunk, ChunkMetaData chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetaData = new ChunkMetaData(chunkHeader.getMeasurementID(),
        chunkHeader.getDataType(), out.getPosition(), chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    logger.debug("end flushing a chunk:{}, totalvalue:{}", currentChunkMetaData, chunkMetadata.getNumOfPoints());
  }

  /**
   * end chunk and write some log.
   */
  public void endCurrentChunk() {
    currentChunkGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
    currentChunkMetaData = null;
    totalChunkNum++;
  }

  /**
   * write {@linkplain TsFileMetaData TSFileMetaData} to output stream and close it.
   *
   * @param schema Schema
   * @throws IOException if I/O error occurs
   */
  public void endFile(Schema schema) throws IOException {

    // serialize the SEPARATOR of MetaData and ChunkGroups
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    // get all measurementSchema of this TsFile
    Map<String, MeasurementSchema> schemaDescriptors = schema.getMeasurementSchemaMap();
    logger.debug("get time series list:{}", schemaDescriptors);

    Map<String, TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = flushTsDeviceMetaDataAndGetIndex(
        this.chunkGroupMetaDataList);

    TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeviceMetadataIndexMap, schemaDescriptors);

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
    size += tsFileMetaData.serializeBloomFilter(out.wrapAsStream(), chunkGroupMetaDataList);
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", out.getPosition());
    }

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());// write the size of the file metadata.

    // write magic string
    out.write(magicStringBytes);

    // close file
    logger.error("{} is closed.", file.getName());
    out.close();
    canWrite = false;
    logger.info("output stream is closed");
  }

  /**
   * 1. group chunkGroupMetaDataList to TsDeviceMetadata 2. flush TsDeviceMetadata 3. get
   * TsDeviceMetadataIndex
   *
   * @param chunkGroupMetaDataList all chunk group metadata in memory
   * @return TsDeviceMetadataIndex in TsFileMetaData
   */
  private Map<String, TsDeviceMetadataIndex> flushTsDeviceMetaDataAndGetIndex(
      List<ChunkGroupMetaData> chunkGroupMetaDataList) throws IOException {

    Map<String, TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = new HashMap<>();

    long offset; /* offset for the flushing TsDeviceMetadata */

    TsDeviceMetadata currentTsDeviceMetadata;

    // flush TsDeviceMetadata by string order of deviceId
    for (Map.Entry<String, TsDeviceMetadata> entry : getAllTsDeviceMetadata(chunkGroupMetaDataList)
        .entrySet()) {
      // update statistics in TsDeviceMetadata
      currentTsDeviceMetadata = entry.getValue();

      // flush tsChunkGroupBlockMetaData
      offset = out.getPosition();
      int size = currentTsDeviceMetadata.serializeTo(out.wrapAsStream());

      TsDeviceMetadataIndex tsDeviceMetadataIndex = new TsDeviceMetadataIndex(offset, size,
          currentTsDeviceMetadata);
      tsDeviceMetadataIndexMap.put(entry.getKey(), tsDeviceMetadataIndex);
    }

    return tsDeviceMetadataIndexMap;
  }

  /**
   * group all chunk group metadata by device.
   *
   * @param chunkGroupMetaDataList all chunk group metadata
   * @return TsDeviceMetadata of all devices
   */
  private TreeMap<String, TsDeviceMetadata> getAllTsDeviceMetadata(
      List<ChunkGroupMetaData> chunkGroupMetaDataList) {
    String currentDevice;
    TreeMap<String, TsDeviceMetadata> tsDeviceMetadataMap = new TreeMap<>();

    for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
      currentDevice = chunkGroupMetaData.getDeviceID();

      if (!tsDeviceMetadataMap.containsKey(currentDevice)) {
        TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
        tsDeviceMetadataMap.put(currentDevice, tsDeviceMetadata);
      }
      tsDeviceMetadataMap.get(currentDevice).addChunkGroupMetaData(chunkGroupMetaData);
    }
    return tsDeviceMetadataMap;
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


  /**
   * get chunkGroupMetaDataList.
   *
   * @return - List of chunkGroupMetaData
   */
  public List<ChunkGroupMetaData> getChunkGroupMetaDatas() {
    return chunkGroupMetaDataList;
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

  /**
   * @return all Schema that this ioWriter know. By default implementation (TsFileIOWriter.class),
   * it is empty
   */
  public Map<String, MeasurementSchema> getKnownSchema() {
    return Collections.emptyMap();
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

    Iterator<ChunkGroupMetaData> chunkGroupMetaDataIterator = chunkGroupMetaDataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetaData chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDeviceID();
      int chunkNum = chunkGroupMetaData.getChunkMetaDataList().size();
      Iterator<ChunkMetaData> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetaDataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        ChunkMetaData chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid = startTimeIdx < pathChunkStartTimes.size()
            && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
          invalidChunkNum++;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
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
