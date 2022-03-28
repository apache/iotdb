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

import org.apache.iotdb.db.wal.utils.SerializedSize;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Checkpoint is the basic element of .checkpoint file, including type, number of memTables, and
 * brief information of each memTable.
 */
public class Checkpoint implements SerializedSize {
  /** checkpoint type 1 byte, checkpoint number 4 bytes */
  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Integer.BYTES;

  /** checkpoint type */
  private final CheckpointType type;
  /** memTable information */
  private final List<MemTableInfo> memTableInfos;

  public Checkpoint(CheckpointType type, List<MemTableInfo> memTableInfos) {
    this.type = type;
    this.memTableInfos = memTableInfos;
  }

  @Override
  public int serializedSize() {
    int size = FIXED_SERIALIZED_SIZE;
    for (MemTableInfo memTableInfo : memTableInfos) {
      size += memTableInfo.serializedSize();
    }
    return size;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.put(type.getCode());
    buffer.putInt(memTableInfos.size());
    for (MemTableInfo memTableInfo : memTableInfos) {
      memTableInfo.serialize(buffer);
    }
  }

  public static Checkpoint deserialize(DataInputStream stream) throws IOException {
    CheckpointType type = CheckpointType.valueOf(stream.readByte());
    int cnt = stream.readInt();
    List<MemTableInfo> memTableInfos = new ArrayList<>(cnt);
    for (int i = 0; i < cnt; ++i) {
      MemTableInfo memTableInfo = MemTableInfo.deserialize(stream);
      memTableInfos.add(memTableInfo);
    }
    return new Checkpoint(type, memTableInfos);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Checkpoint)) {
      return false;
    }
    Checkpoint other = (Checkpoint) obj;
    return this.type == other.type && Objects.equals(this.memTableInfos, other.memTableInfos);
  }

  public CheckpointType getType() {
    return type;
  }

  public List<MemTableInfo> getMemTableInfos() {
    return memTableInfos;
  }
}
