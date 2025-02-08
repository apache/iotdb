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

/** Type of {@link WALEntry}, including info type and signal type. */
public enum WALEntryType {
  // region info entry type
  @Deprecated
  INSERT_ROW_PLAN((byte) 0),
  @Deprecated
  INSERT_TABLET_PLAN((byte) 1),
  @Deprecated
  DELETE_PLAN((byte) 2),
  @Deprecated
  // memory tablet snapshot from 1.3 or lower version
  OLD_MEMORY_TABLE_SNAPSHOT((byte) 3),
  /** {@link org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode} */
  INSERT_ROW_NODE((byte) 4),
  /** {@link org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode} */
  INSERT_TABLET_NODE((byte) 5),
  /** {@link org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode} */
  DELETE_DATA_NODE((byte) 6),
  /** {@link org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint} */
  MEMORY_TABLE_CHECKPOINT((byte) 7),
  /** {@link org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode} */
  INSERT_ROWS_NODE((byte) 8),
  CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR_NODE((byte) 9),
  /** {@link org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable} */
  MEMORY_TABLE_SNAPSHOT((byte) 10),
  RELATIONAL_DELETE_DATA_NODE((byte) 11),
  // endregion
  // region signal entry type
  // signal wal buffer has been closed
  CLOSE_SIGNAL(Byte.MIN_VALUE),
  // signal wal buffer to roll wal log writer
  ROLL_WAL_LOG_WRITER_SIGNAL((byte) (Byte.MIN_VALUE + 1)),
  // mark the wal file info part ends
  WAL_FILE_INFO_END_MARKER((byte) (Byte.MIN_VALUE + 2));
  // endregion

  private final byte code;

  WALEntryType(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return code;
  }

  /** Returns true when this type should be searched. */
  public boolean needSearch() {
    return this == INSERT_TABLET_NODE
        || this == INSERT_ROW_NODE
        || this == INSERT_ROWS_NODE
        || this == DELETE_DATA_NODE
        || this == RELATIONAL_DELETE_DATA_NODE;
  }

  public static WALEntryType valueOf(byte code) {
    for (WALEntryType type : WALEntryType.values()) {
      if (type.code == code) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid WALEntryType code: " + code);
  }
}
