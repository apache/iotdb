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
package org.apache.iotdb.db.wal.node;

import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

/** This interface provides uniform interface for writing wal and making checkpoints. */
public interface IWALNode extends FlushListener, AutoCloseable, ConsensusReqReader, DataSet {

  /** Log InsertRowNode */
  WALFlushListener log(long memTableId, InsertRowNode insertRowNode);

  /** Log InsertTabletNode */
  WALFlushListener log(long memTableId, InsertTabletNode insertTabletNode, int start, int end);

  /** Log DeleteDataNode */
  WALFlushListener log(long memTableId, DeleteDataNode deleteDataNode);

  /** Callback when memTable created */
  void onMemTableCreated(IMemTable memTable, String targetTsFile);

  @Override
  void close();
}
