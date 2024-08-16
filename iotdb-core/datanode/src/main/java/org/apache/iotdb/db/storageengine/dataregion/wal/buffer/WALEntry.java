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

package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.utils.SerializedSize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * {@link WALEntry} is the basic element of .wal file, including type, memTable id, and specific
 * value(physical plan or memTable snapshot).
 */
public abstract class WALEntry implements SerializedSize {
  private static final Logger logger = LoggerFactory.getLogger(WALEntry.class);

  // type of value
  protected final WALEntryType type;
  // memTable id
  protected final long memTableId;
  // value(physical plan or memTable snapshot)
  protected final WALEntryValue value;
  // listen whether this WALEntry has been written to the filesystem
  // null iff this WALEntry is deserialized from .wal file
  protected final WALFlushListener walFlushListener;

  protected WALEntry(long memTableId, WALEntryValue value, boolean wait) {
    this.memTableId = memTableId;
    this.value = value;
    if (value instanceof IMemTable) {
      this.type = WALEntryType.MEMORY_TABLE_SNAPSHOT;
    } else if (value instanceof InsertRowNode) {
      this.type = WALEntryType.INSERT_ROW_NODE;
    } else if (value instanceof InsertTabletNode) {
      this.type = WALEntryType.INSERT_TABLET_NODE;
    } else if (value instanceof InsertRowsNode) {
      this.type = WALEntryType.INSERT_ROWS_NODE;
    } else if (value instanceof DeleteDataNode) {
      this.type = WALEntryType.DELETE_DATA_NODE;
    } else if (value instanceof Checkpoint) {
      this.type = WALEntryType.MEMORY_TABLE_CHECKPOINT;
    } else if (value instanceof ContinuousSameSearchIndexSeparatorNode) {
      this.type = WALEntryType.CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE;
    } else {
      throw new RuntimeException("Unknown WALEntry type");
    }
    walFlushListener = new WALFlushListener(wait, value);
  }

  protected WALEntry(WALEntryType type, long memTableId, WALEntryValue value, boolean wait) {
    this.type = type;
    this.memTableId = memTableId;
    this.value = value;
    this.walFlushListener = new WALFlushListener(wait, value);
  }

  public abstract void serialize(IWALByteBufferView buffer);

  public static WALEntry deserialize(DataInputStream stream) throws IOException {
    byte typeNum = stream.readByte();
    WALEntryType type = WALEntryType.valueOf(typeNum);

    // handle signal
    switch (type) {
      case CLOSE_SIGNAL:
      case ROLL_WAL_LOG_WRITER_SIGNAL:
      case WAL_FILE_INFO_END_MARKER:
        return new WALSignalEntry(type);
      default:
        break;
    }

    // handle info
    long memTableId = stream.readLong();
    WALEntryValue value = null;
    switch (type) {
      case MEMORY_TABLE_SNAPSHOT:
        value = AbstractMemTable.Factory.create(stream);
        break;
      case INSERT_ROW_NODE:
        value = (InsertRowNode) PlanNodeType.deserializeFromWAL(stream);
        break;
      case INSERT_TABLET_NODE:
        value = (InsertTabletNode) PlanNodeType.deserializeFromWAL(stream);
        break;
      case INSERT_ROWS_NODE:
        value = (InsertRowsNode) PlanNodeType.deserializeFromWAL(stream);
        break;
      case DELETE_DATA_NODE:
        value = (DeleteDataNode) PlanNodeType.deserializeFromWAL(stream);
        break;
      default:
        throw new RuntimeException("Unknown WALEntry type " + type);
    }
    return new WALInfoEntry(type, memTableId, value);
  }

  /**
   * This deserialization method is only for iot consensus and just deserializes InsertRowNode and
   * InsertTabletNode.
   */
  public static PlanNode deserializeForConsensus(ByteBuffer buffer) {
    logger.debug(
        "buffer capacity is: {}, limit is: {}, position is: {}",
        buffer.capacity(),
        buffer.limit(),
        buffer.position());
    // wal entry type
    buffer.get();
    // memTable id
    buffer.getLong();
    return PlanNodeType.deserializeFromWAL(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, memTableId, value);
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

  public long getMemTableId() {
    return memTableId;
  }

  public WALEntryValue getValue() {
    return value;
  }

  public WALFlushListener getWalFlushListener() {
    return walFlushListener;
  }

  public abstract boolean isSignal();
}
