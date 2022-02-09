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

import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsFileMetadataV2 {

  private TsFileMetadataV2() {}

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a pair of TsFileMetaData and VersionInfo
   */
  public static Pair<TsFileMetadata, List<Pair<Long, Long>>> deserializeFrom(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    List<Pair<Long, Long>> versionInfo = new ArrayList<>();
    // metadataIndex
    fileMetaData.setMetadataIndex(MetadataIndexNodeV2.deserializeFrom(buffer));
    // totalChunkNum
    ReadWriteIOUtils.readInt(buffer);
    // invalidChunkNum
    ReadWriteIOUtils.readInt(buffer);

    // versionInfo
    int versionSize = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < versionSize; i++) {
      long versionPos = ReadWriteIOUtils.readLong(buffer);
      long version = ReadWriteIOUtils.readLong(buffer);
      versionInfo.add(new Pair<>(versionPos, version));
    }

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      int byteLength = ReadWriteIOUtils.readInt(buffer);
      byte[] bytes = new byte[byteLength];
      buffer.get(bytes);
      int filterSize = ReadWriteIOUtils.readInt(buffer);
      int hashFunctionSize = ReadWriteIOUtils.readInt(buffer);
      fileMetaData.setBloomFilter(
          BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize));
    }

    return new Pair<>(fileMetaData, versionInfo);
  }
}
