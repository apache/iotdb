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

package org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint;

import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * MemTableInfo records brief info of one memTable, including memTable id, tsFile path, and .wal
 * file version id of its first {@link WALEntry}.
 */
public class MemTableInfo implements WALEntryValue {
  // memTable id 8 bytes, first version id 8 bytes
  private static final int FIXED_SERIALIZED_SIZE = Long.BYTES * 2;
  // memTable id
  private long memTableId;
  // path of the tsFile which this memTable will be flushed to
  private String tsFilePath;
  // version id of the file where this memTable's first WALEntry is located
  private volatile long firstFileVersionId;

  // memTable
  private IMemTable memTable;

  // memTable is flushed or not
  private boolean flushed;
  // data region id
  private int dataRegionId;
  // total wal usage of this memTable
  private long walDiskUsage;

  private MemTableInfo() {}

  public MemTableInfo(IMemTable memTable, String tsFilePath, long firstFileVersionId) {
    this.memTable = memTable;
    this.memTableId = memTable.getMemTableId();
    this.tsFilePath = tsFilePath;
    this.firstFileVersionId = firstFileVersionId;
    this.dataRegionId = Integer.parseInt(memTable.getDataRegionId());
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE + ReadWriteIOUtils.sizeToWrite(tsFilePath);
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    throw new UnsupportedOperationException();
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putLong(memTableId);
    ReadWriteIOUtils.write(tsFilePath, buffer);
    buffer.putLong(firstFileVersionId);
  }

  public static MemTableInfo deserialize(DataInputStream stream) throws IOException {
    MemTableInfo memTableInfo = new MemTableInfo();
    memTableInfo.memTableId = stream.readLong();
    memTableInfo.tsFilePath = ReadWriteIOUtils.readString(stream);
    memTableInfo.firstFileVersionId = stream.readLong();
    return memTableInfo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(memTableId, tsFilePath, firstFileVersionId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof MemTableInfo)) {
      return false;
    }
    MemTableInfo other = (MemTableInfo) obj;
    return this.memTableId == other.memTableId
        && Objects.equals(this.tsFilePath, other.tsFilePath)
        && this.firstFileVersionId == other.firstFileVersionId;
  }

  public IMemTable getMemTable() {
    return memTable;
  }

  public boolean isFlushed() {
    return flushed;
  }

  public void setFlushed() {
    // avoid memory leak
    this.memTable = null;
    this.flushed = true;
  }

  public long getMemTableId() {
    return memTableId;
  }

  public int getDataRegionId() {
    return dataRegionId;
  }

  public String getTsFilePath() {
    return tsFilePath;
  }

  public long getFirstFileVersionId() {
    return firstFileVersionId;
  }

  public void setFirstFileVersionId(long firstFileVersionId) {
    this.firstFileVersionId = firstFileVersionId;
  }

  public long getWalDiskUsage() {
    return walDiskUsage;
  }

  public void addWalDiskUsage(long size) {
    walDiskUsage += size;
  }
}
