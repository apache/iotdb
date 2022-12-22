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

package org.apache.iotdb.db.engine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public interface ChunkData extends TsFileData {
  String getDevice();

  TTimePartitionSlot getTimePartitionSlot();

  void setNotDecode();

  boolean isAligned();

  void writeEntireChunk(ByteBuffer chunkData, IChunkMetadata chunkMetadata) throws IOException;

  void writeEntirePage(PageHeader pageHeader, ByteBuffer pageData) throws IOException;

  void writeDecodePage(long[] times, Object[] values, int satisfiedLength) throws IOException;

  @Override
  default boolean isModification() {
    return false;
  }

  static ChunkData deserialize(InputStream stream) throws PageException, IOException {
    boolean isAligned = ReadWriteIOUtils.readBool(stream);
    return isAligned
        ? AlignedChunkData.deserialize(stream)
        : NonAlignedChunkData.deserialize(stream);
  }

  static ChunkData createChunkData(
      boolean isAligned,
      String device,
      ChunkHeader chunkHeader,
      TTimePartitionSlot timePartitionSlot) {
    return isAligned
        ? new AlignedChunkData(device, chunkHeader, timePartitionSlot)
        : new NonAlignedChunkData(device, chunkHeader, timePartitionSlot);
  }
}
