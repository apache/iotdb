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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

/**
 * Reserves the memory of a pending result fragment under an account owned by the query. The account
 * outlives the result handle that produced the block, so a block waiting for the following RPCs
 * stays accounted from the moment it becomes pending until the last fragment is sent. Reservations
 * are best effort: a block larger than the budget is still transferred.
 */
public class QueryOwnedFragmentAccount implements PendingResultFragment.Account {

  private static final String PLAN_NODE_ID = "pendingResultFragment";

  private final String queryId;
  private final String fragmentInstanceId;
  private LocalMemoryManager localMemoryManager;
  private boolean closed;

  /** Resolves the local memory manager lazily, so constructing a query does not start it. */
  public QueryOwnedFragmentAccount(String queryId) {
    this.queryId = queryId;
    this.fragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
            new TFragmentInstanceId(queryId, 0, PLAN_NODE_ID));
  }

  public QueryOwnedFragmentAccount(LocalMemoryManager localMemoryManager, String queryId) {
    this(queryId);
    this.localMemoryManager = localMemoryManager;
  }

  private LocalMemoryManager getLocalMemoryManager() {
    if (localMemoryManager == null) {
      localMemoryManager =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager().getLocalMemoryManager();
    }
    return localMemoryManager;
  }

  @Override
  public synchronized long reserve(long bytes) {
    if (closed) {
      return 0;
    }
    MemoryPool queryPool = getLocalMemoryManager().getQueryPool();
    long maxBytesCanReserve = queryPool.getMaxBytes();
    long bytesToReserve = Math.min(bytes, maxBytesCanReserve);
    if (bytesToReserve <= 0) {
      return 0;
    }
    queryPool.registerPlanNodeIdToQueryMemoryMap(
        queryId, fragmentInstanceId, PLAN_NODE_ID, MemorySide.OUTPUT);
    if (queryPool.tryReserve(
        queryId, fragmentInstanceId, PLAN_NODE_ID, bytesToReserve, maxBytesCanReserve)) {
      return bytesToReserve;
    }
    return 0;
  }

  @Override
  public synchronized void free(long bytes) {
    getLocalMemoryManager().getQueryPool().free(queryId, fragmentInstanceId, PLAN_NODE_ID, bytes);
  }

  /** Stops accepting reservations and removes the account; call once the query is cleaned up. */
  public synchronized void deregister() {
    closed = true;
    getLocalMemoryManager()
        .getQueryPool()
        .deRegisterFragmentInstanceFromQueryMemoryMap(queryId, fragmentInstanceId, false);
  }
}
