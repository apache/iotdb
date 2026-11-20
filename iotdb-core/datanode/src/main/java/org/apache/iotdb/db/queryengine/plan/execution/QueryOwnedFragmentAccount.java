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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Reserves the memory of a pending result fragment under an account owned by the query, so that a
 * block waiting for the following RPCs stays accounted after its result handle released it. A
 * reservation under memory pressure waits for other queries to release memory; the wait is
 * cancelled when the fragment is released, so it can never block the cleanup that would free the
 * memory it waits for.
 */
public class QueryOwnedFragmentAccount implements PendingResultFragment.Account {

  private static final String PLAN_NODE_ID = "pendingResultFragment";

  private final String queryId;
  private final String fragmentInstanceId;
  private LocalMemoryManager localMemoryManager;

  // guarded by this
  private long reservedBytes;
  private MemoryReservationResult waitingReservation;
  private boolean released;
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
  public void reserve(long bytes) {
    MemoryReservationResult result;
    synchronized (this) {
      if (closed) {
        throw aborted();
      }
      released = false;
      MemoryPool queryPool = getLocalMemoryManager().getQueryPool();
      long maxBytesCanReserve = queryPool.getMaxBytes();
      long bytesToReserve = Math.min(bytes, maxBytesCanReserve);
      queryPool.registerPlanNodeIdToQueryMemoryMap(
          queryId, fragmentInstanceId, PLAN_NODE_ID, MemorySide.OUTPUT);
      result =
          queryPool.reserveWithPriority(
              queryId, fragmentInstanceId, PLAN_NODE_ID, bytesToReserve, maxBytesCanReserve, false);
      if (result.isReserveSuccess()) {
        reservedBytes += result.getReservedBytes();
        return;
      }
      waitingReservation = result;
    }
    // wait outside the lock, so that release() can cancel it
    try {
      result.getFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      abandon(result);
      throw aborted();
    } catch (CancellationException | ExecutionException e) {
      abandon(result);
      throw aborted();
    }
    boolean keep;
    synchronized (this) {
      waitingReservation = null;
      keep = !released && !closed;
      if (keep) {
        reservedBytes += result.getReservedBytes();
      }
      notifyAll();
    }
    if (!keep) {
      // the fragment was released while the reservation waited
      freePool(result.getReservedBytes());
      throw aborted();
    }
  }

  /** Stops waiting for a reservation that failed or was interrupted; nothing was reserved. */
  private void abandon(MemoryReservationResult result) {
    synchronized (this) {
      if (waitingReservation == result) {
        waitingReservation = null;
      }
      notifyAll();
    }
  }

  @Override
  public void release() {
    long toFree;
    MemoryReservationResult waiting;
    synchronized (this) {
      released = true;
      toFree = reservedBytes;
      reservedBytes = 0;
      waiting = waitingReservation;
      waitingReservation = null;
    }
    if (toFree > 0) {
      freePool(toFree);
    }
    if (waiting != null) {
      // wake the waiting reservation; it frees its bytes itself if it completed in the meantime
      getLocalMemoryManager().getQueryPool().tryCancel(waiting.getFuture());
      awaitWaitingReservationSettled();
    }
  }

  private synchronized void awaitWaitingReservationSettled() {
    while (waitingReservation != null) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Stops accepting reservations and removes the account; call once the query is cleaned up. */
  public void deregister() {
    synchronized (this) {
      closed = true;
    }
    release();
    getLocalMemoryManager()
        .getQueryPool()
        .deRegisterFragmentInstanceFromQueryMemoryMap(queryId, fragmentInstanceId, false);
  }

  private void freePool(long bytes) {
    getLocalMemoryManager().getQueryPool().free(queryId, fragmentInstanceId, PLAN_NODE_ID, bytes);
  }

  private static IllegalStateException aborted() {
    return new IllegalStateException(
        DataNodeQueryMessages
            .EXCEPTION_WAITING_FOR_THE_MEMORY_OF_A_PENDING_RESULT_FRAGMENT_WAS_ABORTED_BECAUSE_THE_QUERY_WAS_CLEANED_UP_D51EC67C);
  }
}
