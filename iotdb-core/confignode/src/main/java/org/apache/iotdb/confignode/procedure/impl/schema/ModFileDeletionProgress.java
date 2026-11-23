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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Tracks mod-file writes for a schema deletion using one replica per region. */
public class ModFileDeletionProgress {
  private final LongSupplier procedureId;
  private final BooleanSupplier deletionRunning;
  private final Consumer<
          DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>>
      progressRequestSender;
  // Track the replica actually executing each region, including changes caused by retries.
  // Count each region once; followers may have different local TsFiles and apply asynchronously.
  private final Map<TConsensusGroupId, TDataNodeLocation> modProgressTargets =
      new ConcurrentHashMap<>();
  private final Map<TConsensusGroupId, List<TDataNodeLocation>> modProgressReplicaTargets =
      new ConcurrentHashMap<>();
  private final Object modProgressRefreshMonitor = new Object();
  private CompletableFuture<Void> modProgressRefreshFuture;
  private volatile int modProgressRegionCount;
  private volatile TDropTableModProgress modFileProgress;
  private volatile boolean modProgressFinished;
  private volatile boolean modProgressCleanupPending;

  public ModFileDeletionProgress(
      final LongSupplier procedureId, final BooleanSupplier deletionRunning) {
    this(
        procedureId,
        deletionRunning,
        requestContext ->
            CnToDnInternalServiceAsyncRequestManager.getInstance()
                .sendAsyncRequest(requestContext, 1, 2_000L, true));
  }

  ModFileDeletionProgress(
      final LongSupplier procedureId,
      final BooleanSupplier deletionRunning,
      final Consumer<
              DataNodeAsyncRequestContext<
                  TGetDropTableModProgressReq, TGetDropTableModProgressResp>>
          progressRequestSender) {
    this.procedureId = procedureId;
    this.deletionRunning = deletionRunning;
    this.progressRequestSender = progressRequestSender;
  }

  public void setRegionCount(final int regionCount) {
    modProgressRegionCount = regionCount;
  }

  public void trackRegions(
      final TDataNodeLocation location, final List<TConsensusGroupId> regions) {
    regions.forEach(region -> modProgressTargets.put(region, location));
  }

  public void trackRegionReplicas(
      final Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSets) {
    synchronized (modProgressRefreshMonitor) {
      regionReplicaSets.forEach(
          (region, replicaSet) ->
              modProgressReplicaTargets.put(
                  region, new ArrayList<>(replicaSet.getDataNodeLocations())));
    }
  }

  public void finish(final Runnable advanceState) {
    // Publish the final snapshot together with the state transition, after any in-flight query.
    runModProgressRefresh(
        () -> {
          refreshModFileProgress(true);
          modProgressFinished = true;
          modProgressTargets.clear();
          advanceState.run();
        },
        true);
  }

  public void finishWithoutModFiles() {
    synchronized (modProgressRefreshMonitor) {
      modFileProgress = new TDropTableModProgress(0).setRemainingModFileCount(0);
      modProgressFinished = true;
      modProgressTargets.clear();
      modProgressCleanupPending = false;
      modProgressReplicaTargets.clear();
    }
  }

  public TDropTableModProgress snapshot() {
    final TDropTableModProgress progress = modFileProgress;
    return progress == null ? null : progress.deepCopy();
  }

  public boolean hasPendingCleanup() {
    return modProgressCleanupPending;
  }

  /** Shares an in-flight counter refresh among concurrent observability requests. */
  public void update() {
    runModProgressRefresh(
        () -> {
          if (modProgressFinished) {
            retryCleanup();
          } else {
            refreshModFileProgress(false);
          }
        },
        false);
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

  private boolean refreshModFileProgress(final boolean clearProgress) {
    if (!clearProgress && (modProgressFinished || !deletionRunning.getAsBoolean())) {
      return false;
    }
    final Map<TConsensusGroupId, TDataNodeLocation> targets = new HashMap<>(modProgressTargets);
    final Map<TDataNodeLocation, List<TConsensusGroupId>> refreshTargets =
        getRefreshTargets(clearProgress);
    if (refreshTargets.isEmpty()) {
      if (clearProgress && targets.equals(modProgressTargets)) {
        modFileProgress = null;
      }
      return true;
    }
    if (!clearProgress && (targets.isEmpty() || targets.size() != modProgressRegionCount)) {
      return false;
    }
    final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
        requestContext = sendProgressRequest(refreshTargets, clearProgress);

    // A deletion retry may have selected a different replica while these RPCs were in flight.
    if (targets.equals(modProgressTargets)) {
      modFileProgress =
          targets.size() == modProgressRegionCount
              ? aggregateModFileProgress(targets, requestContext.getResponseMap())
              : null;
    }
    if (clearProgress) {
      final boolean cleanupPending = !allRequestsSucceeded(refreshTargets, requestContext);
      synchronized (modProgressRefreshMonitor) {
        modProgressCleanupPending = cleanupPending;
        if (!cleanupPending) {
          modProgressReplicaTargets.clear();
        }
      }
      return !cleanupPending;
    }
    return true;
  }

  private DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
      sendProgressRequest(
          final Map<TDataNodeLocation, List<TConsensusGroupId>> refreshTargets,
          final boolean clearProgress) {
    final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
        requestContext =
            new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.GET_DROP_TABLE_MOD_PROGRESS);
    final Map<Integer, List<TConsensusGroupId>> regionsByNode = new HashMap<>();
    refreshTargets.forEach(
        (location, regions) -> {
          requestContext.putNodeLocation(location.getDataNodeId(), location);
          regionsByNode
              .computeIfAbsent(location.getDataNodeId(), ignored -> new ArrayList<>())
              .addAll(regions);
        });
    regionsByNode.forEach(
        (nodeId, regions) ->
            requestContext.putRequest(
                nodeId,
                new TGetDropTableModProgressReq(procedureId.getAsLong(), regions)
                    .setClearProgress(clearProgress)));
    progressRequestSender.accept(requestContext);
    return requestContext;
  }

  private void retryCleanup() {
    if (!modProgressCleanupPending) {
      return;
    }
    final Map<TDataNodeLocation, List<TConsensusGroupId>> refreshTargets = getRefreshTargets(true);
    if (refreshTargets.isEmpty()) {
      synchronized (modProgressRefreshMonitor) {
        modProgressCleanupPending = false;
        modProgressReplicaTargets.clear();
      }
      return;
    }
    final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
        requestContext = sendProgressRequest(refreshTargets, true);
    if (allRequestsSucceeded(refreshTargets, requestContext)) {
      synchronized (modProgressRefreshMonitor) {
        modProgressCleanupPending = false;
        modProgressReplicaTargets.clear();
      }
    }
  }

  private static boolean allRequestsSucceeded(
      final Map<TDataNodeLocation, List<TConsensusGroupId>> refreshTargets,
      final DataNodeAsyncRequestContext<TGetDropTableModProgressReq, TGetDropTableModProgressResp>
          requestContext) {
    return refreshTargets.keySet().stream()
        .allMatch(
            location -> {
              final TGetDropTableModProgressResp response =
                  requestContext.getResponseMap().get(location.getDataNodeId());
              return response != null
                  && response.getStatus() != null
                  && response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
            });
  }

  Map<TDataNodeLocation, List<TConsensusGroupId>> getRefreshTargets(final boolean clearProgress) {
    final Map<TDataNodeLocation, List<TConsensusGroupId>> refreshTargets = new HashMap<>();
    if (clearProgress && !modProgressReplicaTargets.isEmpty()) {
      modProgressReplicaTargets.forEach(
          (region, locations) ->
              locations.forEach(
                  location ->
                      refreshTargets
                          .computeIfAbsent(location, ignored -> new ArrayList<>())
                          .add(region)));
    } else {
      modProgressTargets.forEach(
          (region, location) ->
              refreshTargets.computeIfAbsent(location, ignored -> new ArrayList<>()).add(region));
    }
    return refreshTargets;
  }

  public static TDropTableModProgress aggregateModFileProgress(
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

  public void serialize(final DataOutputStream stream) throws IOException {
    final TDropTableModProgress progress;
    final boolean cleanupPending;
    final Map<TConsensusGroupId, List<TDataNodeLocation>> replicaTargets;
    synchronized (modProgressRefreshMonitor) {
      progress = modFileProgress;
      cleanupPending = modProgressCleanupPending;
      replicaTargets =
          cleanupPending ? new HashMap<>(modProgressReplicaTargets) : Collections.emptyMap();
    }
    stream.writeBoolean(progress != null);
    if (progress != null) {
      stream.writeLong(progress.getWrittenModFileCount());
      stream.writeLong(
          progress.isSetRemainingModFileCount() ? progress.getRemainingModFileCount() : -1);
    }
    stream.writeBoolean(cleanupPending);
    if (cleanupPending) {
      stream.writeInt(replicaTargets.size());
      for (final Map.Entry<TConsensusGroupId, List<TDataNodeLocation>> entry :
          replicaTargets.entrySet()) {
        ThriftCommonsSerDeUtils.serializeTConsensusGroupId(entry.getKey(), stream);
        stream.writeInt(entry.getValue().size());
        for (final TDataNodeLocation location : entry.getValue()) {
          ThriftCommonsSerDeUtils.serializeTDataNodeLocation(location, stream);
        }
      }
    }
  }

  public void deserialize(final ByteBuffer buffer) {
    if (buffer.hasRemaining() && buffer.get() != 0) {
      final TDropTableModProgress progress = new TDropTableModProgress(buffer.getLong());
      final long remaining = buffer.getLong();
      if (remaining >= 0) {
        progress.setRemainingModFileCount(remaining);
      }
      modFileProgress = progress;
    }
    if (buffer.hasRemaining()) {
      modProgressCleanupPending = buffer.get() != 0;
      if (modProgressCleanupPending) {
        modProgressFinished = true;
        final int regionCount = buffer.getInt();
        for (int i = 0; i < regionCount; i++) {
          final TConsensusGroupId region =
              ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
          final int locationCount = buffer.getInt();
          final List<TDataNodeLocation> locations = new ArrayList<>(locationCount);
          for (int j = 0; j < locationCount; j++) {
            locations.add(ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer));
          }
          modProgressReplicaTargets.put(region, locations);
        }
      }
    }
  }
}
