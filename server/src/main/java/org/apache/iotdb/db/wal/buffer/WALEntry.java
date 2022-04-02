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
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.SerializedSize;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * WALEntry is the basic element of .wal file, including type, memTable id, and specific
 * value(physical plan or memTable snapshot).
 */
public class WALEntry implements SerializedSize {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** wal entry type 1 byte, memTable id 4 bytes */
  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Integer.BYTES;

  /** type of value */
  private final WALEntryType type;
  /** memTable id */
  private final int memTableId;
  /** value(physical plan or memTable snapshot) */
  private final WALEntryValue value;
  /** extra info for InsertTabletPlan type value */
  private TabletInfo tabletInfo;

  /**
   * listen whether this WALEntry has been written to the filesystem, null iff this WALEntry is
   * deserialized from .wal file
   */
  private final WALFlushListener walFlushListener;

  public WALEntry(int memTableId, WALEntryValue value) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    if (value instanceof InsertTabletPlan) {
      tabletInfo = new TabletInfo(0, ((InsertTabletPlan) value).getRowCount());
    }
  }

  public WALEntry(int memTableId, InsertTabletPlan value, int tabletStart, int tabletEnd) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    tabletInfo = new TabletInfo(tabletStart, tabletEnd);
  }

  public WALEntry(int memTableId, WALEntryValue value, boolean wait) {
    this.memTableId = memTableId;
    this.value = value;
    if (value instanceof InsertRowPlan) {
      this.type = WALEntryType.INSERT_ROW_PLAN;
    } else if (value instanceof InsertTabletPlan) {
      this.type = WALEntryType.INSERT_TABLET_PLAN;
    } else if (value instanceof DeletePlan) {
      this.type = WALEntryType.DELETE_PLAN;
    } else if (value instanceof IMemTable) {
      this.type = WALEntryType.MEMORY_TABLE_SNAPSHOT;
    } else {
      throw new RuntimeException("Unknown WALEntry type");
    }
    walFlushListener = new WALFlushListener(wait);
  }

  private WALEntry(WALEntryType type, int memTableId, WALEntryValue value) {
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
    switch (type) {
      case INSERT_TABLET_PLAN:
        ((InsertTabletPlan) value)
            .serializeToWAL(buffer, tabletInfo.tabletStart, tabletInfo.tabletEnd);
        break;
      case INSERT_ROW_PLAN:
      case DELETE_PLAN:
      case MEMORY_TABLE_SNAPSHOT:
        value.serializeToWAL(buffer);
        break;
    }
  }

  public static WALEntry deserialize(DataInputStream stream)
      throws IllegalPathException, IOException {
    byte typeNum = stream.readByte();
    WALEntryType type = WALEntryType.valueOf(typeNum);
    if (type == null) {
      throw new IOException("unrecognized wal entry type " + typeNum);
    }

    int memTableId = stream.readInt();
    WALEntryValue value = null;
    switch (type) {
      case INSERT_ROW_PLAN:
        value = (InsertRowPlan) PhysicalPlan.Factory.create(stream);
        break;
      case INSERT_TABLET_PLAN:
        value = (InsertTabletPlan) PhysicalPlan.Factory.create(stream);
        break;
      case DELETE_PLAN:
        value = (DeletePlan) PhysicalPlan.Factory.create(stream);
        break;
      case MEMORY_TABLE_SNAPSHOT:
        value = AbstractMemTable.Factory.create(stream);
        break;
    }
    return new WALEntry(type, memTableId, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof WALEntry)) {
      return false;
    }
    WALEntry other = (WALEntry) obj;
    return this.type == other.type
        && this.memTableId == other.memTableId
        && Objects.equals(this.value, other.value);
  }

  public WALEntryType getType() {
    return type;
  }

  public int getMemTableId() {
    return memTableId;
  }

  public WALEntryValue getValue() {
    return value;
  }

  public WALFlushListener getWalFlushListener() {
    return walFlushListener;
  }

  private static class TabletInfo {
    /** start row of InsertTabletPlan */
    private final int tabletStart;
    /** end row of InsertTabletPlan */
    private final int tabletEnd;

    public TabletInfo(int tabletStart, int tabletEnd) {
      this.tabletStart = tabletStart;
      this.tabletEnd = tabletEnd;
    }
  }
}
