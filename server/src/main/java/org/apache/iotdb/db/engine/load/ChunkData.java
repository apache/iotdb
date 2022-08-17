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
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public interface ChunkData {
  String getDevice();

  TTimePartitionSlot getTimePartitionSlot();

  long getDataSize();

  void addDataSize(long pageSize);

  void setHeadPageNeedDecode(boolean headPageNeedDecode);

  void setTailPageNeedDecode(boolean tailPageNeedDecode);

  void setTimePartitionSlot(TTimePartitionSlot timePartitionSlot);

  IChunkWriter getChunkWriter(File tsFile) throws IOException, PageException;

  void serialize(DataOutputStream stream, File tsFile) throws IOException;

  static ChunkData createChunkData(
      boolean isAligned, long offset, String device, ChunkHeader chunkHeader) {
    return isAligned
        ? new AlignedChunkData(offset, device, chunkHeader)
        : new NonAlignedChunkData(offset, device, chunkHeader);
  }
}
