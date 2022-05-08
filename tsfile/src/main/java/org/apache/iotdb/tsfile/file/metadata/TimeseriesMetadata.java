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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TimeseriesMetadata implements ITimeSeriesMetadata {

  /** used for old version tsfile */
  private long startOffsetOfChunkMetaDataList;
  /**
   * 0 means this time series has only one chunk, no need to save the statistic again in chunk
   * metadata;
   *
   * <p>1 means this time series has more than one chunk, should save the statistic again in chunk
   * metadata;
   *
   * <p>if the 8th bit is 1, it means it is the time column of a vector series;
   *
   * <p>if the 7th bit is 1, it means it is the value column of a vector series
   */
  private byte timeSeriesMetadataType;

  private int chunkMetadataListDataSize;

  private String measurementId;
  private TSDataType dataType;
  private TSEncoding encodingType;
  private CompressionType compressionType;

  private Statistics<? extends Serializable> statistics;

  // modified is true when there are modifications of the series, or from unseq file
  private boolean modified;

  private IChunkMetadataLoader chunkMetadataLoader;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;

  private ArrayList<IChunkMetadata> chunkMetadataList;

  public TimeseriesMetadata() {}

  public TimeseriesMetadata(ArrayList<IChunkMetadata> chunkMetadataList) {
    this.timeSeriesMetadataType =
        (byte)
            ((chunkMetadataList.size() > 1 ? (byte) 1 : (byte) 0)
                | chunkMetadataList.get(0).getMask());
    this.chunkMetadataList = chunkMetadataList;

    IChunkMetadata firstChunkMetadata = chunkMetadataList.get(0);
    this.measurementId = firstChunkMetadata.getMeasurementUid();
    this.dataType = firstChunkMetadata.getDataType();
    this.encodingType = firstChunkMetadata.getEncodingType();
    this.compressionType = firstChunkMetadata.getCompressionType();

    this.statistics = Statistics.getStatsByType(dataType);
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      statistics.mergeStatistics(chunkMetadata.getStatistics());
    }
  }

  public TimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) {
    this.timeSeriesMetadataType = timeseriesMetadata.timeSeriesMetadataType;
    this.chunkMetadataListDataSize = timeseriesMetadata.chunkMetadataListDataSize;
    this.measurementId = timeseriesMetadata.measurementId;
    this.dataType = timeseriesMetadata.dataType;
    this.statistics = timeseriesMetadata.statistics;
    this.modified = timeseriesMetadata.modified;
    this.chunkMetadataList = new ArrayList<>(timeseriesMetadata.chunkMetadataList);
  }

  public static TimeseriesMetadata deserializeFrom(ByteBuffer buffer, boolean needChunkMetadata) {
    TimeseriesMetadata timeseriesMetadata = new TimeseriesMetadata();
    timeseriesMetadata.setTimeSeriesMetadataType(ReadWriteIOUtils.readByte(buffer));
    timeseriesMetadata.setMeasurementId(ReadWriteIOUtils.readVarIntString(buffer));
    timeseriesMetadata.setTSDataType(ReadWriteIOUtils.readDataType(buffer));
    // for compaction
    timeseriesMetadata.setEncodingType(ReadWriteIOUtils.readEncoding(buffer));
    timeseriesMetadata.setCompressionType(ReadWriteIOUtils.readCompressionType(buffer));

    int chunkMetaDataListDataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    timeseriesMetadata.setDataSizeOfChunkMetaDataList(chunkMetaDataListDataSize);

    timeseriesMetadata.setStatistics(Statistics.deserialize(buffer, timeseriesMetadata.dataType));
    if (needChunkMetadata) {
      ByteBuffer byteBuffer = buffer.slice();
      byteBuffer.limit(chunkMetaDataListDataSize);
      timeseriesMetadata.chunkMetadataList = new ArrayList<>();
      while (byteBuffer.hasRemaining()) {
        timeseriesMetadata.chunkMetadataList.add(
            ChunkMetadata.deserializeFrom(byteBuffer, timeseriesMetadata));
      }
      // minimize the storage of an ArrayList instance.
      timeseriesMetadata.chunkMetadataList.trimToSize();
    }
    buffer.position(buffer.position() + chunkMetaDataListDataSize);
    return timeseriesMetadata;
  }

  public static TimeseriesMetadata deserializeFrom(
      InputStream inputStream, boolean needChunkMetadata) throws IOException {
    TimeseriesMetadata timeseriesMetadata = new TimeseriesMetadata();
    timeseriesMetadata.setTimeSeriesMetadataType(ReadWriteIOUtils.readByte(inputStream));
    timeseriesMetadata.setMeasurementId(ReadWriteIOUtils.readVarIntString(inputStream));
    timeseriesMetadata.setTSDataType(ReadWriteIOUtils.readDataType(inputStream));
    // for compaction
    timeseriesMetadata.setEncodingType(ReadWriteIOUtils.readEncoding(inputStream));
    timeseriesMetadata.setCompressionType(ReadWriteIOUtils.readCompressionType(inputStream));

    int chunkMetaDataListDataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    timeseriesMetadata.setDataSizeOfChunkMetaDataList(chunkMetaDataListDataSize);
    timeseriesMetadata.setStatistics(
        Statistics.deserialize(inputStream, timeseriesMetadata.dataType));

    if (needChunkMetadata) {
      timeseriesMetadata.chunkMetadataList = new ArrayList<>();
      while (chunkMetaDataListDataSize > 0) {
        ChunkMetadata chunkMetadata =
            ChunkMetadata.deserializeFrom(inputStream, timeseriesMetadata);
        timeseriesMetadata.chunkMetadataList.add(chunkMetadata);
        chunkMetaDataListDataSize -= chunkMetadata.getSerializedSize();
      }
      // minimize the storage of an ArrayList instance.
      timeseriesMetadata.chunkMetadataList.trimToSize();
    } else {
      inputStream.skip(chunkMetaDataListDataSize);
    }
    return timeseriesMetadata;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return byte length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(timeSeriesMetadataType, outputStream);
    byteLen += ReadWriteIOUtils.writeVar(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(dataType, outputStream);

    // for compaction
    byteLen += ReadWriteIOUtils.write(encodingType, outputStream);
    byteLen += ReadWriteIOUtils.write(compressionType, outputStream);

    PublicBAOS publicBAOS = new PublicBAOS();
    int chunkMetaDataListDataSize = 0;
    boolean serializeStatistic = (chunkMetadataList.size() > 1);
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      chunkMetaDataListDataSize += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);
    }

    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(chunkMetaDataListDataSize, outputStream);
    byteLen += statistics.serialize(outputStream);

    publicBAOS.writeTo(outputStream);
    byteLen += publicBAOS.size();
    return byteLen;
  }

  public byte getTimeSeriesMetadataType() {
    return timeSeriesMetadataType;
  }

  public void setTimeSeriesMetadataType(byte timeSeriesMetadataType) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
  }

  public void setOffsetOfChunkMetaDataList(long position) {
    this.startOffsetOfChunkMetaDataList = position;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public int getDataSizeOfChunkMetaDataList() {
    return chunkMetadataListDataSize;
  }

  public void setDataSizeOfChunkMetaDataList(int size) {
    this.chunkMetadataListDataSize = size;
  }

  public TSDataType getTSDataType() {
    return dataType;
  }

  public void setTSDataType(TSDataType tsDataType) {
    this.dataType = tsDataType;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.statistics = statistics;
  }

  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    this.chunkMetadataLoader = chunkMetadataLoader;
  }

  public IChunkMetadataLoader getChunkMetadataLoader() {
    return chunkMetadataLoader;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList() throws IOException {
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public void setModified(boolean modified) {
    this.modified = modified;
  }

  @Override
  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  @Override
  public boolean isSeq() {
    return isSeq;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public void setEncodingType(TSEncoding encodingType) {
    this.encodingType = encodingType;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }

  public void setChunkMetadataList(ArrayList<ChunkMetadata> chunkMetadataList) {
    this.chunkMetadataList = new ArrayList<>(chunkMetadataList);
  }

  @Override
  public String toString() {
    return "TimeseriesMetadata{"
        + "startOffsetOfChunkMetaDataList="
        + startOffsetOfChunkMetaDataList
        + ", timeSeriesMetadataType="
        + timeSeriesMetadataType
        + ", chunkMetaDataListDataSize="
        + chunkMetadataListDataSize
        + ", measurementId='"
        + measurementId
        + '\''
        + ", dataType="
        + dataType
        + ", statistics="
        + statistics
        + ", modified="
        + modified
        + ", isSeq="
        + isSeq
        + ", chunkMetadataList="
        + chunkMetadataList
        + '}';
  }

  public void mergeTimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) throws IOException {
    if (!this.measurementId.equals(timeseriesMetadata.getMeasurementId())
        || !this.dataType.equals(timeseriesMetadata.getTSDataType())
        || !this.compressionType.equals(timeseriesMetadata.getCompressionType())
        || !this.encodingType.equals(timeseriesMetadata.getEncodingType())) {
      throw new IOException("Incompatible TimeseriesMetadata to merge");
    }
    this.chunkMetadataList.addAll(timeseriesMetadata.getChunkMetadataList());
    this.statistics.mergeStatistics(timeseriesMetadata.getStatistics());
    this.chunkMetadataListDataSize += timeseriesMetadata.getDataSizeOfChunkMetaDataList();
    this.timeSeriesMetadataType =
        (byte)
            ((chunkMetadataList.size() > 1 ? (byte) 1 : (byte) 0)
                | chunkMetadataList.get(0).getMask());
  }
}
