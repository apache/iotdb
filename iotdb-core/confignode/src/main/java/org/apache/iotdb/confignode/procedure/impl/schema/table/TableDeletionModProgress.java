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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressReq;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressResp;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Tracks mod-file writes for table and field-column deletion using one replica per region. */
class TableDeletionModProgress {
  private final LongSupplier procedureId;
  private final BooleanSupplier deletionRunning;
  // Track the replica actually executing each region, including changes caused by retries.
  // Count each region once; followers may have different local TsFiles and apply asynchronously.
  private final Map<TConsensusGroupId, TDataNodeLocation> modProgressTargets =
      new ConcurrentHashMap<>();
  private final Object modProgressRefreshMonitor = new Object();
  private CompletableFuture<Void> modProgressRefreshFuture;
  private volatile int modProgressRegionCount;
  private volatile TDropTableModProgress modFileProgress;
  private volatile boolean modProgressFinished;

  TableDeletionModProgress(final LongSupplier procedureId, final BooleanSupplier deletionRunning) {
    this.procedureId = procedureId;
    this.deletionRunning = deletionRunning;
  }

  void setRegionCount(final int regionCount) {
    modProgressRegionCount = regionCount;
  }

  void trackRegions(final TDataNodeLocation location, final List<TConsensusGroupId> regions) {
    regions.forEach(region -> modProgressTargets.put(region, location));
  }

  void finish(final Runnable advanceState) {
    // Publish the final snapshot together with the state transition, after any in-flight query.
    runModProgressRefresh(
        () -> {
          refreshModFileProgress();
          modProgressFinished = true;
          advanceState.run();
        },
        true);
  }

  void finishWithoutModFiles() {
    modFileProgress = new TDropTableModProgress(0).setRemainingModFileCount(0);
    modProgressFinished = true;
  }

  TDropTableModProgress snapshot() {
    final TDropTableModProgress progress = modFileProgress;
    return progress == null ? null : progress.deepCopy();
  }

  /** Shares an in-flight counter refresh among concurrent observability requests. */
  void update() {
    runModProgressRefresh(this::refreshModFileProgress, false);
  }

  private void runModProgressRefresh(final Runnable action, final boolean retryAfterWait) {
    while (true) {
      final CompletableFuture<Void> refreshFuture;
      final boolean refreshOwner;
      synchronized (modProgressRefreshMonitor) {
        if (modProgressRefreshFuture == null) {
          refreshFuture = new CompletableFuture<>();
          modProgressRefreshFuture = refreshFuture;
          refreshOwner = true;
        } else {
          refreshFuture = modProgressRefreshFuture;
          refreshOwner = false;
        }
      }
      if (!refreshOwner) {
        refreshFuture.join();
        if (retryAfterWait) {
          continue;
        }
        return;
      }
      try {
        action.run();
        return;
      } catch (final RuntimeException | Error e) {
        refreshFuture.completeExceptionally(e);
        throw e;
      } finally {
        synchronized (modProgressRefreshMonitor) {
          if (modProgressRefreshFuture == refreshFuture) {
            modProgressRefreshFuture = null;
          }
          refreshFuture.complete(null);
        }
      }
    }
  }

  private void refreshModFileProgress() {
    if (modProgressFinished
        || !deletionRunning.getAsBoolean()
        || modProgressTargets.isEmpty()
        || modProgressTargets.size() != modProgressRegionCount) {
      return;
    }
    final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
        requestContext =
            new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.GET_DROP_TABLE_MOD_PROGRESS);
    final Map<Integer, List<TConsensusGroupId>> regionsByNode = new HashMap<>();
    final Map<TConsensusGroupId, TDataNodeLocation> targets = new HashMap<>(modProgressTargets);
    targets.forEach(
        (region, location) -> {
          requestContext.putNodeLocation(location.getDataNodeId(), location);
          regionsByNode
              .computeIfAbsent(location.getDataNodeId(), ignored -> new ArrayList<>())
              .add(region);
        });
    for (final Map.Entry<Integer, List<TConsensusGroupId>> entry : regionsByNode.entrySet()) {
      requestContext.putRequest(
          entry.getKey(),
          new TGetDropTableModProgressReq(procedureId.getAsLong(), entry.getValue()));
    }
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(requestContext, 1, 2_000L, true);

    // A deletion retry may have selected a different replica while these RPCs were in flight.
    if (targets.equals(modProgressTargets)) {
      // A failed refresh is unknown, including the final refresh. Do not freeze stale counters
      // with remaining files after deletion has finished.
      modFileProgress = aggregateModFileProgress(targets, requestContext.getResponseMap());
    }
  }

  static TDropTableModProgress aggregateModFileProgress(
      final Map<TConsensusGroupId, TDataNodeLocation> targets,
      final Map<Integer, TGetDropTableModProgressResp> responses) {
    long written = 0;
    long remaining = 0;
    boolean hasUnknown = false;
    for (final Map.Entry<TConsensusGroupId, TDataNodeLocation> target : targets.entrySet()) {
      final TGetDropTableModProgressResp response =
          responses.get(target.getValue().getDataNodeId());
      if (response == null
          || response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || !response.isSetRegionProgress()
          || !response.getRegionProgress().containsKey(target.getKey().getId())) {
        // Unknown is different from zero. In particular, an old DataNode does not report counts.
        return null;
      }
      final TDropTableModProgress progress =
          response.getRegionProgress().get(target.getKey().getId());
      written += progress.getWrittenModFileCount();
      if (progress.isSetRemainingModFileCount()) {
        remaining += progress.getRemainingModFileCount();
      } else {
        hasUnknown = true;
      }
    }
    final TDropTableModProgress progress = new TDropTableModProgress(written);
    if (!hasUnknown) {
      progress.setRemainingModFileCount(remaining);
    }
    return progress;
  }

  void serialize(final DataOutputStream stream) throws IOException {
    final TDropTableModProgress progress = modFileProgress;
    stream.writeBoolean(progress != null);
    if (progress != null) {
      stream.writeLong(progress.getWrittenModFileCount());
      stream.writeLong(
          progress.isSetRemainingModFileCount() ? progress.getRemainingModFileCount() : -1);
    }
  }

  void deserialize(final ByteBuffer buffer) {
    if (buffer.hasRemaining() && buffer.get() != 0) {
      final TDropTableModProgress progress = new TDropTableModProgress(buffer.getLong());
      final long remaining = buffer.getLong();
      if (remaining >= 0) {
        progress.setRemainingModFileCount(remaining);
      }
      modFileProgress = progress;
    }
  }
}
