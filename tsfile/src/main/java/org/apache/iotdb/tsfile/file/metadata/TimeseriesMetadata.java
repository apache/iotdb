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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TimeseriesMetadata implements Accountable {

  /**
   * 0 means this time series has only one chunk, no need to save the statistic again in chunk metadata
   * 1 means this time series has more than one chunk, should save the statistic again in chunk metadata
   */
  private byte timeSeriesMetadataType;

  private long startOffsetOfChunkMetaDataList;
  private int chunkMetaDataListDataSize;

  private String measurementId;
  private TSDataType dataType;

  private Statistics<?> statistics;

  // modified is true when there are modifications of the series, or from unseq file
  private boolean modified;

  protected IChunkMetadataLoader chunkMetadataLoader;

  private long ramSize;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;

  public TimeseriesMetadata() {
  }

  public TimeseriesMetadata(byte timeSeriesMetadataType, long startOffsetOfChunkMetaDataList,
      int chunkMetaDataListDataSize, String measurementId, TSDataType dataType,
      Statistics statistics) {
    this.timeSeriesMetadataType = timeSeriesMetadataType;
    this.startOffsetOfChunkMetaDataList = startOffsetOfChunkMetaDataList;
    this.chunkMetaDataListDataSize = chunkMetaDataListDataSize;
    this.measurementId = measurementId;
    this.dataType = dataType;
    this.statistics = statistics;
  }

  public TimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) {
    this.timeSeriesMetadataType = timeseriesMetadata.timeSeriesMetadataType;
    this.startOffsetOfChunkMetaDataList = timeseriesMetadata.startOffsetOfChunkMetaDataList;
    this.chunkMetaDataListDataSize = timeseriesMetadata.chunkMetaDataListDataSize;
    this.measurementId = timeseriesMetadata.measurementId;
    this.dataType = timeseriesMetadata.dataType;
    this.statistics = timeseriesMetadata.statistics;
    this.modified = timeseriesMetadata.modified;
  }

  public static TimeseriesMetadata deserializeFrom(ByteBuffer buffer) {
    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    timeseriesMetaData.setTimeSeriesMetadataType(ReadWriteIOUtils.readByte(buffer));
    timeseriesMetaData.setMeasurementId(ReadWriteIOUtils.readVarIntString(buffer));
    timeseriesMetaData.setTSDataType(ReadWriteIOUtils.readDataType(buffer));
    timeseriesMetaData.setOffsetOfChunkMetaDataList(ReadWriteIOUtils.readLong(buffer));
    timeseriesMetaData
        .setDataSizeOfChunkMetaDataList(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    timeseriesMetaData.setStatistics(Statistics.deserialize(buffer, timeseriesMetaData.dataType));
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
    byteLen += ReadWriteIOUtils.write(startOffsetOfChunkMetaDataList, outputStream);
    byteLen += ReadWriteForEncodingUtils
        .writeUnsignedVarInt(chunkMetaDataListDataSize, outputStream);
    byteLen += statistics.serialize(outputStream);
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

  public TSDataType getTSDataType() {
    return dataType;
  }

  public void setTSDataType(TSDataType tsDataType) {
    this.dataType = tsDataType;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public void setStatistics(Statistics statistics) {
    this.statistics = statistics;
  }

  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    this.chunkMetadataLoader = chunkMetadataLoader;
  }

  public List<ChunkMetadata> loadChunkMetadataList() throws IOException {
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public void setRamSize(long size) {
    this.ramSize = size;
  }

  @Override
  public long getRamSize() {
    return ramSize;
  }

  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  public boolean isSeq() {
    return isSeq;
  }
}
