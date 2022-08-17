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
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class AlignedChunkData implements ChunkData {
  private List<Long> offset;
  private List<Long> dataSize;
  private boolean isHeadPageNeedDecode;
  private boolean isTailPageNeedDecode;

  private TTimePartitionSlot timePartitionSlot;
  private String device;
  private List<ChunkHeader> chunkHeaderList;

  public AlignedChunkData(long timeOffset, String device, ChunkHeader chunkHeader) {
    this.offset = new ArrayList<>();
    this.dataSize = new ArrayList<>();
    this.isHeadPageNeedDecode = false;
    this.isTailPageNeedDecode = false;
    this.device = device;
    this.chunkHeaderList = new ArrayList<>();

    offset.add(timeOffset);
    dataSize.add(0L);
    chunkHeaderList.add(chunkHeader);
  }

  @Override
  public String getDevice() {
    return device;
  }

  @Override
  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  @Override
  public long getDataSize() {
    return dataSize.stream().mapToLong(o -> o).sum();
  }

  @Override
  public void addDataSize(long pageSize) {
    dataSize.set(0, dataSize.get(0) + pageSize);
  }

  @Override
  public void setHeadPageNeedDecode(boolean headPageNeedDecode) {
    isHeadPageNeedDecode = headPageNeedDecode;
  }

  @Override
  public void setTailPageNeedDecode(boolean tailPageNeedDecode) {
    isTailPageNeedDecode = tailPageNeedDecode;
  }

  @Override
  public void setTimePartitionSlot(TTimePartitionSlot timePartitionSlot) {
    this.timePartitionSlot = timePartitionSlot;
  }

  @Override
  public boolean isAligned() {
    return true;
  }

  public void addValueChunk(long offset, ChunkHeader chunkHeader) {
    this.offset.add(offset);
    this.dataSize.add(0L);
    this.chunkHeaderList.add(chunkHeader);
  }

  public void addValueChunkDataSize(long dataSize) {
    int lastIndex = this.dataSize.size() - 1;
    this.dataSize.set(lastIndex, this.dataSize.get(lastIndex) + dataSize);
  }

  @Override
  public IChunkWriter getChunkWriter() {
    return null;
  }

  @Override
  public void serialize(DataOutputStream stream, File tsFile) throws IOException {

  }

  public static AlignedChunkData deserialize(InputStream stream) {
    return null;
  }

  @Override
  public String toString() {
    return "AlignedChunkData{"
        + "offset="
        + offset
        + ", dataSize="
        + dataSize
        + ", isHeadPageNeedDecode="
        + isHeadPageNeedDecode
        + ", isTailPageNeedDecode="
        + isTailPageNeedDecode
        + ", timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeaderList="
        + chunkHeaderList
        + '}';
  }
}
