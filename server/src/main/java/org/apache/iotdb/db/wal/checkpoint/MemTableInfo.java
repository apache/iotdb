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
package org.apache.iotdb.db.wal.checkpoint;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.utils.SerializedSize;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * MemTableInfo records brief info of one memtable, including memTable id, tsFile path, and .wal
 * file version id of its first {@link WALEntry}.
 */
public class MemTableInfo implements SerializedSize {
  /** memTable id 8 bytes, first version id 8 bytes */
  private static final int FIXED_SERIALIZED_SIZE = Long.BYTES * 2;

  /** memTable */
  private IMemTable memTable;
  /** memTable id */
  private long memTableId;
  /** path of the tsFile which this memTable will be flushed to */
  private String tsFilePath;
  /** version id of the file where this memTable's first WALEntry is located */
  private volatile long firstFileVersionId;

  private MemTableInfo() {}

  public MemTableInfo(IMemTable memTable, String tsFilePath, long firstFileVersionId) {
    this.memTable = memTable;
    this.memTableId = memTable.getMemTableId();
    this.tsFilePath = tsFilePath;
    this.firstFileVersionId = firstFileVersionId;
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE + ReadWriteIOUtils.sizeToWrite(tsFilePath);
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

  public long getMemTableId() {
    return memTableId;
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
}
