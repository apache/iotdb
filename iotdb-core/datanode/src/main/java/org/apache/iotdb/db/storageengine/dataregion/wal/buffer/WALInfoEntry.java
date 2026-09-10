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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** This entry class stores info for persistence. */
public class WALInfoEntry extends WALEntry {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // wal entry type 1 byte, memTable id 8 bytes
  public static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Long.BYTES;

  // extra info for InsertTablet type value
  private TabletInfo tabletInfo;
  private final Long encodedSearchIndex;

  public WALInfoEntry(long memTableId, WALEntryValue value, boolean wait) {
    super(memTableId, value, wait);
    encodedSearchIndex = freezeEncodedSearchIndex(value);
  }

  public WALInfoEntry(long memTableId, WALEntryValue value) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    if (value instanceof InsertTabletNode) {
      tabletInfo =
          new TabletInfo(
              Collections.singletonList(new int[] {0, ((InsertTabletNode) value).getRowCount()}));
    }
  }

  public WALInfoEntry(long memTableId, InsertTabletNode value, List<int[]> tabletRangeList) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    tabletInfo = new TabletInfo(tabletRangeList);
  }

  WALInfoEntry(WALEntryType type, long memTableId, WALEntryValue value) {
    super(type, memTableId, value, false);
    encodedSearchIndex = freezeEncodedSearchIndex(value);
    if (value instanceof InsertTabletNode) {
      tabletInfo =
          new TabletInfo(
              Collections.singletonList(new int[] {0, ((InsertTabletNode) value).getRowCount()}));
    }
  }

  @Override
  public int serializedSize() {
    if (value == null) {
      return FIXED_SERIALIZED_SIZE;
    }
    if (value instanceof InsertTabletNode && tabletInfo != null) {
      return FIXED_SERIALIZED_SIZE
          + ((InsertTabletNode) value).serializedSize(tabletInfo.tabletRangeList);
    }
    return FIXED_SERIALIZED_SIZE + value.serializedSize();
  }

  @Override
  public void serialize(IWALByteBufferView buffer) {
    buffer.put(type.getCode());
    buffer.putLong(memTableId);
    switch (type) {
      case INSERT_TABLET_NODE:
        ((InsertTabletNode) value)
            .serializeToWAL(buffer, tabletInfo.tabletRangeList, encodedSearchIndex);
        break;
      case INSERT_ROW_NODE:
        ((InsertRowNode) value).serializeToWAL(buffer, encodedSearchIndex);
        break;
      case INSERT_ROWS_NODE:
        ((InsertRowsNode) value).serializeToWAL(buffer, encodedSearchIndex);
        break;
      case DELETE_DATA_NODE:
        ((DeleteDataNode) value).serializeToWAL(buffer, encodedSearchIndex);
        break;
      case RELATIONAL_DELETE_DATA_NODE:
        ((RelationalDeleteDataNode) value).serializeToWAL(buffer, encodedSearchIndex);
        break;
      case OBJECT_FILE_NODE:
        ((ObjectNode) value).serializeToWAL(buffer, encodedSearchIndex);
        break;
      case MEMORY_TABLE_SNAPSHOT:
      case CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE:
        value.serializeToWAL(buffer);
        break;
      case MEMORY_TABLE_CHECKPOINT:
        throw new RuntimeException(StorageEngineMessages.CANNOT_SERIALIZE_CHECKPOINT_TO_WAL);
      default:
        throw new RuntimeException(StorageEngineMessages.UNSUPPORTED_WAL_ENTRY_TYPE + type);
    }
  }

  public long getSearchIndex() {
    return encodedSearchIndex == null
        ? SearchNode.NO_CONSENSUS_INDEX
        : SearchNode.extractSearchIndex(encodedSearchIndex);
  }

  private static Long freezeEncodedSearchIndex(WALEntryValue value) {
    return value instanceof SearchNode ? ((SearchNode) value).getEncodedSearchIndex() : null;
  }

  private static class TabletInfo {
    // ranges of insert tablet
    private final List<int[]> tabletRangeList;

    public TabletInfo(List<int[]> tabletRangeList) {
      this.tabletRangeList = new ArrayList<>(tabletRangeList.size());
      for (int[] range : tabletRangeList) {
        this.tabletRangeList.add(Arrays.copyOf(range, range.length));
      }
    }

    public int getRangeRowCount() {
      int count = 0;
      for (int[] range : tabletRangeList) {
        count += range[1] - range[0];
      }
      return count;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tabletRangeList);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TabletInfo)) {
        return false;
      }
      TabletInfo that = (TabletInfo) obj;
      if (this.tabletRangeList.size() != that.tabletRangeList.size()) {
        return false;
      }

      for (int i = 0; i < tabletRangeList.size(); i++) {
        if (!Arrays.equals(this.tabletRangeList.get(i), that.tabletRangeList.get(i))) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public boolean isSignal() {
    return false;
  }

  @Override
  public long getMemorySize() {
    switch (type) {
      case INSERT_TABLET_NODE:
        return ((InsertNode) value).getMemorySize()
            / ((InsertTabletNode) value).getRowCount()
            * tabletInfo.getRangeRowCount();
      case INSERT_ROW_NODE:
      case INSERT_ROWS_NODE:
        return ((InsertNode) value).getMemorySize();
      case MEMORY_TABLE_SNAPSHOT:
        return ((IMemTable) value).getTVListsRamCost();
      case DELETE_DATA_NODE:
      case RELATIONAL_DELETE_DATA_NODE:
      case CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE:
      case MEMORY_TABLE_CHECKPOINT:
        return RamUsageEstimator.sizeOfObject(value);
      case OBJECT_FILE_NODE:
        return ((ObjectNode) value).serializedSize();
      default:
        throw new RuntimeException(StorageEngineMessages.UNSUPPORTED_WAL_ENTRY_TYPE + type);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), tabletInfo, encodedSearchIndex);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    WALInfoEntry other = (WALInfoEntry) obj;
    return Objects.equals(this.tabletInfo, other.tabletInfo)
        && Objects.equals(this.encodedSearchIndex, other.encodedSearchIndex);
  }
}
