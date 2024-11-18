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

package org.apache.iotdb.db.storageengine.dataregion.wal.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener.Status;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;

import java.util.List;

/** This class provides fake wal node when wal is disabled or exception happens. */
public class WALFakeNode implements IWALNode {
  private final Status status;
  private final WALFlushListener successListener;
  private final WALFlushListener failListener;

  private WALFakeNode(Status status) {
    this(status, null);
  }

  public WALFakeNode(Status status, Exception cause) {
    this.status = status;
    this.successListener = new WALFlushListener(false, null);
    this.successListener.succeed();
    this.failListener = new WALFlushListener(false, null);
    this.failListener.fail(cause);
  }

  @Override
  public WALFlushListener log(long memTableId, InsertRowNode insertRowNode) {
    return getResult();
  }

  @Override
  public WALFlushListener log(long memTableId, InsertRowsNode insertRowsNode) {
    return getResult();
  }

  @Override
  public WALFlushListener log(
      long memTableId, InsertTabletNode insertTabletNode, List<int[]> rangeList) {
    return getResult();
  }

  @Override
  public WALFlushListener log(long memTableId, DeleteDataNode deleteDataNode) {
    return getResult();
  }

  @Override
  public WALFlushListener log(long memTableId, RelationalDeleteDataNode deleteDataNode) {
    return getResult();
  }

  @Override
  public WALFlushListener log(
      long memTableId, ContinuousSameSearchIndexSeparatorNode separatorNode) {
    return getResult();
  }

  private WALFlushListener getResult() {
    switch (status) {
      case SUCCESS:
        return successListener;
      case FAILURE:
      default:
        return failListener;
    }
  }

  @Override
  public void onMemTableFlushStarted(IMemTable memTable) {
    // do nothing
  }

  @Override
  public void onMemTableFlushed(IMemTable memTable) {
    // do nothing
  }

  @Override
  public void onMemTableCreated(IMemTable memTable, String targetTsFile) {
    // do nothing
  }

  @Override
  public void setSafelyDeletedSearchIndex(long safelyDeletedSearchIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReqIterator getReqIterator(long startIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public long getCurrentSearchIndex() {
    return 0;
  }

  @Override
  public long getCurrentWALFileVersion() {
    return 0;
  }

  @Override
  public long getTotalSize() {
    return 0;
  }

  public static WALFakeNode getFailureInstance(Exception e) {
    return new WALFakeNode(
        Status.FAILURE, new WALException("Cannot write wal into a fake node. ", e));
  }

  public static WALFakeNode getSuccessInstance() {
    return WALFakeNodeHolder.SUCCESS_INSTANCE;
  }

  private static class WALFakeNodeHolder {
    private static final WALFakeNode SUCCESS_INSTANCE = new WALFakeNode(Status.SUCCESS);
  }
}
