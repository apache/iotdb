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
import java.util.Objects;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Metadata of one chunk.
 */
public class ChunkMetadata implements Accountable {

  private String measurementUid;

  /**
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader;

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

  private boolean modified;

  /**
   * ChunkLoader of metadata, used to create ChunkReaderWrap
   */
  private IChunkLoader chunkLoader;

  private Statistics statistics;

  private long ramSize;

  private static final int CHUNK_METADATA_FIXED_RAM_SIZE = 80;


  private ChunkMetadata() {
  }

  /**
   * constructor of ChunkMetaData.
   *
   * @param measurementUid measurement id
   * @param tsDataType     time series data type
   * @param fileOffset     file offset
   * @param statistics     value statistics
   */
  public ChunkMetadata(String measurementUid, TSDataType tsDataType, long fileOffset,
      Statistics statistics) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return String.format("measurementId: %s, datatype: %s, version: %d, deletedAt: %d, "
        + "Statistics: %s", measurementUid, tsDataType, version, deletedAt, statistics);
  }

  public long getNumOfPoints() {
    return statistics.getCount();
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

  public Statistics getStatistics() {
    return statistics;
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public TSDataType getDataType() {
    return tsDataType;
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
    byteLen += ReadWriteIOUtils.write(tsDataType, outputStream);
    byteLen += statistics.serialize(outputStream);
    return byteLen;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetadata deserializeFrom(ByteBuffer buffer) {
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    chunkMetaData.measurementUid = ReadWriteIOUtils.readString(buffer);
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

    chunkMetaData.statistics = Statistics.deserialize(buffer, chunkMetaData.tsDataType);

    return chunkMetaData;
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

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }

  public IChunkLoader getChunkLoader() {
    return chunkLoader;
  }

  public void setChunkLoader(IChunkLoader chunkLoader) {
    this.chunkLoader = chunkLoader;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkMetadata that = (ChunkMetadata) o;
    return offsetOfChunkHeader == that.offsetOfChunkHeader &&
        version == that.version &&
        deletedAt == that.deletedAt &&
        Objects.equals(measurementUid, that.measurementUid) &&
        tsDataType == that.tsDataType &&
        Objects.equals(statistics, that.statistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(measurementUid, deletedAt, tsDataType, statistics,
        version, offsetOfChunkHeader);
  }

  public boolean isModified() {
    return modified;
  }

  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public long calculateRamSize() {
    return CHUNK_METADATA_FIXED_RAM_SIZE + RamUsageEstimator.sizeOf(measurementUid) + statistics
        .calculateRamSize();
  }

  public void setRamSize(long size) {
    this.ramSize = size;
  }

  @Override
  public long getRamSize() {
    return ramSize;
  }
}