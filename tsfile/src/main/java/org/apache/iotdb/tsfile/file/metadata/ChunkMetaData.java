/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaData of one chunk.
 */
public class ChunkMetaData {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkMetaData.class);

  private String measurementUid;

  /**
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader;

  private long numOfPoints;

  private long startTime;

  private long endTime;

  private TSDataType tsDataType;

  /**
   * The maximum time of the tombstones that take effect on this chunk. Only data with larger.
   * timestamps than this should be exposed to user.
   */
  private long maxTombstoneTime;

  private TsDigest valuesStatistics;

  private ChunkMetaData() {
  }

  /**
   * constructor of ChunkMetaData.
   *
   * @param measurementUid measurement id
   * @param tsDataType time series data type
   * @param fileOffset file offset
   * @param startTime chunk start time
   * @param endTime chunk end time
   */
  public ChunkMetaData(String measurementUid, TSDataType tsDataType, long fileOffset,
      long startTime, long endTime) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * deserialize from InputStream.
   *
   * @param inputStream InputStream
   * @return ChunkMetaData object
   * @throws IOException IOException
   */
  public static ChunkMetaData deserializeFrom(InputStream inputStream) throws IOException {
    ChunkMetaData chunkMetaData = new ChunkMetaData();

    chunkMetaData.measurementUid = ReadWriteIOUtils.readString(inputStream);

    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(inputStream);

    chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(inputStream);
    chunkMetaData.startTime = ReadWriteIOUtils.readLong(inputStream);
    chunkMetaData.endTime = ReadWriteIOUtils.readLong(inputStream);

    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(inputStream);

    chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(inputStream);

    return chunkMetaData;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetaData deserializeFrom(ByteBuffer buffer) {
    ChunkMetaData chunkMetaData = new ChunkMetaData();

    chunkMetaData.measurementUid = ReadWriteIOUtils.readString(buffer);
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.startTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.endTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

    chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(buffer);

    return chunkMetaData;
  }

  /**
   * get serialized size.
   *
   * @return serialized size (int type)
   */
  public int getSerializedSize() {
    return (Integer.BYTES + measurementUid.length()) + // measurementUid
        4 * Long.BYTES + // 4 long: offsetOfChunkHeader, numOfPoints, startTime, endTime
        TSDataType.getSerializedSize() + // TSDataType
        (valuesStatistics == null ? TsDigest.getNullDigestSize()
            : valuesStatistics.getSerializedSize());

  }

  @Override
  public String toString() {
    return String.format("numPoints %d", numOfPoints);
  }

  public long getNumOfPoints() {
    return numOfPoints;
  }

  public void setNumOfPoints(long numRows) {
    this.numOfPoints = numRows;
  }

  /**
   * get offset of chunk header.
   *
   * @return Byte offset of header of this chunk (includes the marker)
   */
  public long getOffsetOfChunkHeader() {
    return offsetOfChunkHeader;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }

  public TsDigest getDigest() {
    return valuesStatistics;
  }

  public void setDigest(TsDigest digest) {
    this.valuesStatistics = digest;

  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public void setTsDataType(TSDataType tsDataType) {
    this.tsDataType = tsDataType;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementUid, outputStream);
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
    byteLen += ReadWriteIOUtils.write(numOfPoints, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    byteLen += ReadWriteIOUtils.write(tsDataType, outputStream);

    if (valuesStatistics == null) {
      byteLen += TsDigest.serializeNullTo(outputStream);
    } else {
      byteLen += valuesStatistics.serializeTo(outputStream);
    }

    assert byteLen == getSerializedSize();
    return byteLen;
  }

  /**
   * serialize to ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return length
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementUid, buffer);
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, buffer);
    byteLen += ReadWriteIOUtils.write(numOfPoints, buffer);
    byteLen += ReadWriteIOUtils.write(startTime, buffer);
    byteLen += ReadWriteIOUtils.write(endTime, buffer);
    byteLen += ReadWriteIOUtils.write(tsDataType, buffer);

    if (valuesStatistics == null) {
      byteLen += TsDigest.serializeNullTo(buffer);
    } else {
      byteLen += valuesStatistics.serializeTo(buffer);
    }

    assert byteLen == getSerializedSize();
    return byteLen;
  }

  public long getMaxTombstoneTime() {
    return maxTombstoneTime;
  }

  public void setMaxTombstoneTime(long maxTombstoneTime) {
    this.maxTombstoneTime = maxTombstoneTime;
  }

}
