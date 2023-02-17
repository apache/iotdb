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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.utils.WALMode;

/** This entry class stores info for persistence */
public class WALInfoEntry extends WALEntry {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** wal entry type 1 byte, memTable id 8 bytes */
  public static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + Long.BYTES;

  /** extra info for InsertTablet type value */
  private TabletInfo tabletInfo;

  public WALInfoEntry(long memTableId, WALEntryValue value, boolean wait) {
    super(memTableId, value, wait);
  }

  public WALInfoEntry(long memTableId, WALEntryValue value) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    if (value instanceof InsertTabletNode) {
      tabletInfo = new TabletInfo(0, ((InsertTabletNode) value).getRowCount());
    }
  }

  public WALInfoEntry(long memTableId, InsertTabletNode value, int tabletStart, int tabletEnd) {
    this(memTableId, value, config.getWalMode() == WALMode.SYNC);
    tabletInfo = new TabletInfo(tabletStart, tabletEnd);
  }

  WALInfoEntry(WALEntryType type, long memTableId, WALEntryValue value) {
    super(type, memTableId, value, false);
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE + (value == null ? 0 : value.serializedSize());
  }

  @Override
  public void serialize(IWALByteBufferView buffer) {
    buffer.put(type.getCode());
    buffer.putLong(memTableId);
    switch (type) {
      case INSERT_TABLET_NODE:
        ((InsertTabletNode) value)
            .serializeToWAL(buffer, tabletInfo.tabletStart, tabletInfo.tabletEnd);
        break;
      case INSERT_ROW_NODE:
      case DELETE_DATA_NODE:
      case MEMORY_TABLE_SNAPSHOT:
        value.serializeToWAL(buffer);
        break;
      default:
        throw new RuntimeException("Unsupported wal entry type " + type);
    }
  }

  private static class TabletInfo {
    /** start row of insert tablet */
    private final int tabletStart;
    /** end row of insert tablet */
    private final int tabletEnd;

    public TabletInfo(int tabletStart, int tabletEnd) {
      this.tabletStart = tabletStart;
      this.tabletEnd = tabletEnd;
    }
  }

  @Override
  public boolean isSignal() {
    return false;
  }
}
