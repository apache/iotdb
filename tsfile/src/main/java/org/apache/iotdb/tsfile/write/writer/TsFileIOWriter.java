/**
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 *
 * @author kangrong
 */
public class TsFileIOWriter {

  public static final byte[] magicStringBytes;
  private static final Logger LOG = LoggerFactory.getLogger(TsFileIOWriter.class);

  static {
    magicStringBytes = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
  }

  protected TsFileOutput out;
  protected List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
  private ChunkGroupMetaData currentChunkGroupMetaData;
  private ChunkMetaData currentChunkMetaData;
  protected boolean canWrite = true;

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
   * for writing data into an existing and incomplete Tsfile. The caller need to guarantee existing
   * data in the TsFileOutput matches the given metadata list
   *
   * @param out the target output
   * @param chunkGroupMetaDataList existing chunkgroups' metadata
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(TsFileOutput out, List<ChunkGroupMetaData> chunkGroupMetaDataList)
      throws IOException {
    this.out = out;
    this.chunkGroupMetaDataList = chunkGroupMetaDataList;
    if (chunkGroupMetaDataList.isEmpty()) {
      startFile();
    }
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
  }

  /**
   * start a {@linkplain ChunkGroupMetaData ChunkGroupMetaData}.
   *
   * @param deviceId device id
   */
  public void startChunkGroup(String deviceId) throws IOException {
    LOG.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    currentChunkGroupMetaData = new ChunkGroupMetaData(deviceId, new ArrayList<>(),
        out.getPosition());
  }

  /**
   * end chunk and write some log.
   */
  public void endChunkGroup(long version) throws IOException {
    long dataSize = out.getPosition() - currentChunkGroupMetaData.getStartOffsetOfChunkGroup();
    ChunkGroupFooter chunkGroupFooter = new ChunkGroupFooter(
        currentChunkGroupMetaData.getDeviceID(),
        dataSize, currentChunkGroupMetaData.getChunkMetaDataList().size());
    chunkGroupFooter.serializeTo(out.wrapAsStream());
    currentChunkGroupMetaData.setEndOffsetOfChunkGroup(out.getPosition());
    currentChunkGroupMetaData.setVersion(version);
    chunkGroupMetaDataList.add(currentChunkGroupMetaData);
    LOG.debug("end chunk group:{}", currentChunkGroupMetaData);
    currentChunkGroupMetaData = null;
  }

  /**
   * start a {@linkplain ChunkMetaData ChunkMetaData}.
   *
   * @param descriptor - measurement of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - statistic of the whole series
   * @param maxTime - maximum timestamp of the whole series in this stage
   * @param minTime - minimum timestamp of the whole series in this stage
   * @param dataSize - the serialized size of all pages
   * @return the serialized size of CHunkHeader
   * @throws IOException if I/O error occurs
   */
  public int startFlushChunk(MeasurementSchema descriptor, CompressionType compressionCodecName,
      TSDataType tsDataType, TSEncoding encodingType, Statistics<?> statistics, long maxTime,
      long minTime,
      int dataSize, int numOfPages) throws IOException {
    LOG.debug("start series chunk:{}, file position {}", descriptor, out.getPosition());

    currentChunkMetaData = new ChunkMetaData(descriptor.getMeasurementId(), tsDataType,
        out.getPosition(), minTime,
        maxTime);

    ChunkHeader header = new ChunkHeader(descriptor.getMeasurementId(), dataSize, tsDataType,
        compressionCodecName,
        encodingType, numOfPages);
    header.serializeTo(out.wrapAsStream());
    LOG.debug("finish series chunk:{} header, file position {}", header, out.getPosition());

    Map<StatisticType, ByteBuffer> statisticsMap = new HashMap<>();
    // TODO add your statistics
    statisticsMap.put(StatisticType.max_value, ByteBuffer.wrap(statistics.getMaxBytes()));
    statisticsMap.put(StatisticType.min_value, ByteBuffer.wrap(statistics.getMinBytes()));
    statisticsMap.put(StatisticType.first, ByteBuffer.wrap(statistics.getFirstBytes()));
    statisticsMap.put(StatisticType.sum, ByteBuffer.wrap(statistics.getSumBytes()));
    statisticsMap.put(StatisticType.last, ByteBuffer.wrap(statistics.getLastBytes()));

    TsDigest tsDigest = new TsDigest();

    tsDigest.setStatistics(statisticsMap);

    currentChunkMetaData.setDigest(tsDigest);

    return header.getSerializedSize();
  }

  /**
   * end chunk and write some log.
   *
   * @param totalValueCount -set the number of points to the currentChunkMetaData
   */
  public void endChunk(long totalValueCount) {
    currentChunkMetaData.setNumOfPoints(totalValueCount);
    currentChunkGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
    LOG.debug("end series chunk:{},totalvalue:{}", currentChunkMetaData, totalValueCount);
    currentChunkMetaData = null;
  }

  /**
   * write {@linkplain TsFileMetaData TSFileMetaData} to output stream and close it.
   *
   * @param schema FileSchema
   * @throws IOException if I/O error occurs
   */
  public void endFile(FileSchema schema) throws IOException {

    // serialize the SEPARATOR of MetaData and ChunkGroups
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    // get all measurementSchema of this TsFile
    Map<String, MeasurementSchema> schemaDescriptors = schema.getAllMeasurementSchema();
    LOG.debug("get time series list:{}", schemaDescriptors);

    Map<String, TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = flushTsDeviceMetaDataAndGetIndex(
        this.chunkGroupMetaDataList);

    TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeviceMetadataIndexMap, schemaDescriptors,
        TSFileConfig.CURRENT_VERSION);

    long footerIndex = out.getPosition();
    LOG.debug("start to flush the footer,file pos:{}", footerIndex);

    // write TsFileMetaData
    int size = tsFileMetaData.serializeTo(out.wrapAsStream());
    LOG.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());// write the size of the file metadata.

    // write magic string
    out.write(magicStringBytes);

    // close file
    out.close();
    canWrite = false;
    LOG.info("output stream is closed");
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
}
