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
package org.apache.iotdb.tsfile.v2.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v2.file.metadata.statistics.StatisticsV2;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ChunkMetadataV2 {

  private ChunkMetadataV2() {}

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetadata deserializeFrom(ByteBuffer buffer) {

    String measurementUid = ReadWriteIOUtils.readString(buffer);
    long offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    TSDataType tsDataType = TSDataType.deserialize((byte) ReadWriteIOUtils.readShort(buffer));

    Statistics<? extends Serializable> statistics = StatisticsV2.deserialize(buffer, tsDataType);
    ChunkMetadata chunkMetaData =
        new ChunkMetadata(measurementUid, tsDataType, offsetOfChunkHeader, statistics);
    chunkMetaData.setFromOldTsFile(true);
    return chunkMetaData;
  }
}
