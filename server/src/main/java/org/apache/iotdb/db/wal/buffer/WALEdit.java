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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.wal.utils.SerializedSize;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * WALEdit is the basic element of .wal file, including type, memTable id, and specific
 * value(physical plan or memTable snapshot).
 */
public class WALEdit implements SerializedSize {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** wal edit type 1 byte, memTable id 4 bytes */
  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Integer.BYTES;

  /** type of value */
  private final WALEditType type;
  /** memTable id */
  private final int memTableId;
  /** value(physical plan or memTable snapshot) */
  private final WALEditValue value;

  /**
   * listen whether this WALEdit has been written to the filesystem, null iff this WALEdit is
   * deserialized from .wal file
   */
  private final WALFlushListener walFlushListener;

  public WALEdit(int memTableId, WALEditValue value) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
  }

  public WALEdit(int memTableId, WALEditValue value, boolean wait) {
    this.memTableId = memTableId;
    this.value = value;
    if (value instanceof InsertPlan) {
      this.type = WALEditType.INSERT_PLAN;
    } else if (value instanceof DeletePlan) {
      this.type = WALEditType.DELETE_PLAN;
    } else if (value instanceof IMemTable) {
      this.type = WALEditType.MEMORY_TABLE_SNAPSHOT;
    } else {
      throw new RuntimeException("Unknown WALEdit type");
    }
    walFlushListener = new WALFlushListener(wait);
  }

  private WALEdit(WALEditType type, int memTableId, WALEditValue value) {
    this.type = type;
    this.memTableId = memTableId;
    this.value = value;
    this.walFlushListener = null;
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE + value.serializedSize();
  }

  public void serialize(IWALByteBufferView buffer) {
    buffer.put(type.getCode());
    buffer.putInt(memTableId);
    value.serializeToWAL(buffer);
  }

  public static WALEdit deserialize(DataInputStream stream)
      throws IllegalPathException, IOException {
    WALEditType type = WALEditType.valueOf(stream.readByte());
    int memTableId = stream.readInt();
    WALEditValue value = null;
    switch (Objects.requireNonNull(type)) {
      case INSERT_PLAN:
        value = (InsertPlan) PhysicalPlan.Factory.create(stream);
        break;
      case DELETE_PLAN:
        value = (DeletePlan) PhysicalPlan.Factory.create(stream);
        break;
      case MEMORY_TABLE_SNAPSHOT:
        value = AbstractMemTable.Factory.create(stream);
        break;
    }
    return new WALEdit(type, memTableId, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof WALEdit)) {
      return false;
    }
    WALEdit other = (WALEdit) obj;
    return this.type == other.type
        && this.memTableId == other.memTableId
        && Objects.equals(this.value, other.value);
  }

  public WALEditType getType() {
    return type;
  }

  public int getMemTableId() {
    return memTableId;
  }

  public WALEditValue getValue() {
    return value;
  }

  public WALFlushListener getWalFlushListener() {
    return walFlushListener;
  }
}
