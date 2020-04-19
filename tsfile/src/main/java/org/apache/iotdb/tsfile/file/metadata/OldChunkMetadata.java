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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
/**
 * MetaData of one chunk.
 */
public class OldChunkMetadata {


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
   * version is used to define the order of operations(insertion, deletion, update). version is set
   * according to its belonging ChunkGroup only when being queried, so it is not persisted.
   */
  private long version;

  /**
   * All data with timestamp <= deletedAt are considered deleted.
   */
  private long deletedAt = Long.MIN_VALUE;

  private TsDigest valuesStatistics;

  private OldChunkMetadata() {
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
  public OldChunkMetadata(String measurementUid, TSDataType tsDataType, long fileOffset,
      long startTime, long endTime) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static OldChunkMetadata deserializeFrom(ByteBuffer buffer) {
    OldChunkMetadata chunkMetaData = new OldChunkMetadata();

    chunkMetaData.measurementUid = ReadWriteIOUtils.readString(buffer);
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.startTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.endTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

    chunkMetaData.valuesStatistics = TsDigest.deserializeFrom(buffer);

    return chunkMetaData;
  }

  public long getNumOfPoints() {
    return numOfPoints;
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

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getDeletedAt() {
    return deletedAt;
  }
}