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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TimeseriesMetadata implements ITimeSeriesMetadata {

  /** used for old version tsfile. */
  private long startOffsetOfChunkMetaDataList;

  /**
   * 0 means this time series has only one chunk, no need to save the statistic again in chunk
   * metadata.
   *
   * <p>1 means this time series has more than one chunk, should save the statistic again in chunk
   * metadata;
   *
   * <p>if the 8th bit is 1, it means it is the time column of a vector series;
   *
   * <p>if the 7th bit is 1, it means it is the value column of a vector series
   */
  private byte timeSeriesMetadataType;

  private int chunkMetaDataListDataSize;

  private String measurementId;
  private TSDataType dataType;

  private Statistics<? extends Serializable> statistics;

  // modified is true when there are modifications of the series, or from unseq file
  private boolean modified;

  private IChunkMetadataLoader chunkMetadataLoader;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;

  // used to save chunk metadata list while serializing
  private PublicBAOS chunkMetadataListBuffer;

  private ArrayList<IChunkMetadata> chunkMetadataList;

  public TimeseriesMetadata() {}

  public TimeseriesMetadata(
      byte timeSeriesMetadataType,
      int chunkMetaDataListDataSize,
      String measurementId,
      TSDataType dataType,
      Statistics<? extends Serializable> statistics,
      PublicBAOS chunkMetadataListBuffer) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
    this.chunkMetaDataListDataSize = chunkMetaDataListDataSize;
    this.measurementId = measurementId;
    this.dataType = dataType;
    this.statistics = statistics;
    this.chunkMetadataListBuffer = chunkMetadataListBuffer;
  }

  public TimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) {
    this.timeSeriesMetadataType = timeseriesMetadata.timeSeriesMetadataType;
    this.chunkMetaDataListDataSize = timeseriesMetadata.chunkMetaDataListDataSize;
    this.measurementId = timeseriesMetadata.measurementId;
    this.dataType = timeseriesMetadata.dataType;
    this.statistics = timeseriesMetadata.statistics;
    this.modified = timeseriesMetadata.modified;
    this.chunkMetadataList = new ArrayList<>(timeseriesMetadata.chunkMetadataList);
  }

  public static TimeseriesMetadata deserializeFrom(ByteBuffer buffer, boolean needChunkMetadata) {
    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    timeseriesMetaData.setTimeSeriesMetadataType(ReadWriteIOUtils.readByte(buffer));
    timeseriesMetaData.setMeasurementId(ReadWriteIOUtils.readVarIntString(buffer));
    timeseriesMetaData.setTsDataType(ReadWriteIOUtils.readDataType(buffer));
    int chunkMetaDataListDataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(chunkMetaDataListDataSize);
    timeseriesMetaData.setStatistics(Statistics.deserialize(buffer, timeseriesMetaData.dataType));
    if (needChunkMetadata) {
      ByteBuffer byteBuffer = buffer.slice();
      byteBuffer.limit(chunkMetaDataListDataSize);
      timeseriesMetaData.chunkMetadataList = new ArrayList<>();
      while (byteBuffer.hasRemaining()) {
        timeseriesMetaData.chunkMetadataList.add(
            ChunkMetadata.deserializeFrom(byteBuffer, timeseriesMetaData));
      }
      // minimize the storage of an ArrayList instance.
      timeseriesMetaData.chunkMetadataList.trimToSize();
    }
    buffer.position(buffer.position() + chunkMetaDataListDataSize);
    return timeseriesMetaData;
  }

  /**
   * Return timeseries metadata without deserializing chunk metadatas if excludedMeasurements
   * contains the measurementId of this timeseries metadata or needChunkMetadata is false.
   */
  public static TimeseriesMetadata deserializeFrom(
      ByteBuffer buffer, Set<String> excludedMeasurements, boolean needChunkMetadata) {
    byte timeseriesType = ReadWriteIOUtils.readByte(buffer);
    String measurementID = ReadWriteIOUtils.readVarIntString(buffer);
    TSDataType tsDataType = ReadWriteIOUtils.readDataType(buffer);
    int chunkMetaDataListDataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Statistics<? extends Serializable> statistics = Statistics.deserialize(buffer, tsDataType);

    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    timeseriesMetaData.setMeasurementId(measurementID);
    timeseriesMetaData.setTimeSeriesMetadataType(timeseriesType);
    timeseriesMetaData.setTsDataType(tsDataType);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(chunkMetaDataListDataSize);
    timeseriesMetaData.setStatistics(statistics);

    if (!excludedMeasurements.contains(measurementID) && needChunkMetadata) {
      // measurement is not in the excluded set and need chunk metadata
      ByteBuffer byteBuffer = buffer.slice();
      byteBuffer.limit(chunkMetaDataListDataSize);
      timeseriesMetaData.chunkMetadataList = new ArrayList<>();
      while (byteBuffer.hasRemaining()) {
        timeseriesMetaData.chunkMetadataList.add(
            ChunkMetadata.deserializeFrom(byteBuffer, timeseriesMetaData));
      }
      // minimize the storage of an ArrayList instance.
      timeseriesMetaData.chunkMetadataList.trimToSize();
    }
    buffer.position(buffer.position() + chunkMetaDataListDataSize);
    return timeseriesMetaData;
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
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(chunkMetaDataListDataSize, outputStream);
    byteLen += statistics.serialize(outputStream);
    chunkMetadataListBuffer.writeTo(outputStream);
    byteLen += chunkMetadataListBuffer.size();
    return byteLen;
  }

  public byte getTimeSeriesMetadataType() {
    return timeSeriesMetadataType;
  }

  public void setTimeSeriesMetadataType(byte timeSeriesMetadataType) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
  }

  public long getOffsetOfChunkMetaDataList() {
    return startOffsetOfChunkMetaDataList;
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
    return chunkMetaDataListDataSize;
  }

  public void setDataSizeOfChunkMetaDataList(int size) {
    this.chunkMetaDataListDataSize = size;
  }

  public TSDataType getTsDataType() {
    return dataType;
  }

  public void setTsDataType(TSDataType tsDataType) {
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

  @Override
  public List<IChunkMetadata> loadChunkMetadataList() {
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  public List<IChunkMetadata> getCopiedChunkMetadataList() {
    return chunkMetadataList.stream()
        .map(chunkMetadata -> new ChunkMetadata((ChunkMetadata) chunkMetadata))
        .collect(Collectors.toList());
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

  // For Test Only
  public void setChunkMetadataListBuffer(PublicBAOS chunkMetadataListBuffer) {
    this.chunkMetadataListBuffer = chunkMetadataListBuffer;
  }

  // For reading version-2 only
  public void setChunkMetadataList(List<ChunkMetadata> chunkMetadataList) {
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
        + chunkMetaDataListDataSize
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
}
