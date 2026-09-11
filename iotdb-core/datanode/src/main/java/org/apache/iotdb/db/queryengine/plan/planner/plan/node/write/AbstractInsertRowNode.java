/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.LastCacheUpdateSource;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.enums.TSDataType;

/**
 * Storage-facing contract for single-row writes. Concrete nodes own their value representation and
 * wire format; sharing this contract never converts a primitive row to an object row.
 */
public abstract class AbstractInsertRowNode extends InsertNode
    implements WALEntryValue, LastCacheUpdateSource {
  protected AbstractInsertRowNode(PlanNodeId id) {
    super(id);
  }

  protected AbstractInsertRowNode(
      PlanNodeId id,
      PartialPath path,
      boolean aligned,
      String[] measurements,
      TSDataType[] dataTypes) {
    super(id, path, aligned, measurements, dataTypes);
  }

  public abstract long getTime();

  public abstract int getValueCount();

  public abstract boolean isNull(int index);

  /** Boxes only the requested cell for cache and Tablet consumers. */
  public abstract Object getValue(int index);

  /** Returns the original backing storage for allocation accounting, without copying it. */
  public abstract Object getValuesForMemoryAccounting();

  public abstract void updateLastCache(String databaseName);

  public abstract void serializeToWAL(IWALByteBufferView buffer, long encodedSearchIndex);
}
