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

package org.apache.iotdb.tsfile.v1.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v1.file.metadata.statistics.StatisticsV1;

import java.nio.ByteBuffer;
/**
 * MetaData of one chunk.
 */
public class ChunkMetadataV1 {


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

  private TsDigestV1 valuesStatistics;

  private ChunkMetadataV1() {
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetadataV1 deserializeFrom(ByteBuffer buffer) {
    ChunkMetadataV1 chunkMetaData = new ChunkMetadataV1();

    chunkMetaData.measurementUid = ReadWriteIOUtils.readString(buffer);
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.numOfPoints = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.startTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.endTime = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

    chunkMetaData.valuesStatistics = TsDigestV1.deserializeFrom(buffer);

    return chunkMetaData;
  }

  public long getNumOfPoints() {
    return numOfPoints;
  }
  
  public ChunkMetadata upgradeToChunkMetadata() {
    Statistics<?> statistics = StatisticsV1
        .constructStatisticsFromOldChunkMetadata(this);
    ChunkMetadata chunkMetadata = new ChunkMetadata(this.measurementUid, this.tsDataType,
        this.offsetOfChunkHeader, statistics);
    chunkMetadata.setFromOldTsFile(true);
    return chunkMetadata;
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

  public TsDigestV1 getDigest() {
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
}
