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
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.Futures;

/**
 * Reserves the memory of a pending result fragment under an account owned by the query, so that a
 * block waiting for the following RPCs stays accounted after its result handle released it. When
 * the query pool is short of memory, the reservation waits until other queries release theirs; only
 * the part of a block that exceeds the whole pool cannot be accounted.
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
    queryPool.registerPlanNodeIdToQueryMemoryMap(
        queryId, fragmentInstanceId, PLAN_NODE_ID, MemorySide.OUTPUT);
    MemoryReservationResult result =
        queryPool.reserveWithPriority(
            queryId, fragmentInstanceId, PLAN_NODE_ID, bytesToReserve, maxBytesCanReserve, false);
    if (!result.isReserveSuccess()) {
      // the block is already in memory, so waiting for the pool is the only way to account it
      Futures.getUnchecked(result.getFuture());
    }
    return result.getReservedBytes();
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
