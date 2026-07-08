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

package org.apache.iotdb.confignode.manager.lease;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.DataNodeState;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Verdict;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntToLongFunction;
import java.util.function.LongSupplier;

/**
 * Broadcasts one Tier-A cache invalidation and turns the per-DataNode responses into a {@link
 * Verdict}. An unreachable DataNode can be skipped only after it is provably self-fenced.
 */
public class ClusterCachePropagator {

  /**
   * {@code T_proceed = T_fence + margin}. The margin covers heartbeat-recording granularity and
   * scheduling jitter.
   */
  public static final long DEFAULT_PROCEED_MARGIN_MS = 5_000L;

  /** How often to retry while waiting for unacked DataNodes to ack or cross T_proceed. */
  private static final long RETRY_INTERVAL_MS = 1_000L;

  /** Per-attempt timeout for cache-invalidation RPCs broadcast to DataNodes. */
  public static final long BROADCAST_RPC_TIMEOUT_MS = 5_000L;

  public static final int BROADCAST_RPC_RETRY = 2;

  /** Broadcasts the cache invalidation to {@code targets} and returns the per-nodeId responses. */
  @FunctionalInterface
  public interface CacheBroadcast {
    Map<Integer, TSStatus> sendTo(Map<Integer, TDataNodeLocation> targets);
  }

  /** Injectable sleep so the retry loop can be driven deterministically in tests. */
  @FunctionalInterface
  interface Sleeper {
    void sleepMs(long ms) throws InterruptedException;
  }

  private final Map<Integer, TDataNodeLocation> registeredDataNodes;
  private final IntToLongFunction elapsedMsSinceLastSuccessfulHeartbeatResponse;
  private final LongSupplier fenceTimeoutMs;
  private final LongSupplier nanoClock;
  private final Sleeper sleeper;

  public ClusterCachePropagator(final Map<Integer, TDataNodeLocation> registeredDataNodes) {
    this(
        registeredDataNodes,
        nodeId -> DataNodeContactTracker.getInstance().getMillisSinceLastSuccessfulResponse(nodeId),
        () ->
            CommonDescriptor.getInstance().getConfig().getMetadataLeaseFenceMs()
                + DEFAULT_PROCEED_MARGIN_MS,
        System::nanoTime,
        Thread::sleep);
  }

  ClusterCachePropagator(
      final Map<Integer, TDataNodeLocation> registeredDataNodes,
      final IntToLongFunction elapsedMsSinceLastSuccessfulHeartbeatResponse,
      final LongSupplier fenceTimeoutMs,
      final LongSupplier nanoClock,
      final Sleeper sleeper) {
    this.registeredDataNodes = registeredDataNodes;
    this.elapsedMsSinceLastSuccessfulHeartbeatResponse =
        elapsedMsSinceLastSuccessfulHeartbeatResponse;
    this.fenceTimeoutMs = fenceTimeoutMs;
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
  }

  /**
   * Broadcast once and classify the result. {@code waitBudgetExhausted} turns a would-be {@link
   * Verdict#WAIT} into {@link Verdict#FAIL}.
   */
  public Verdict propagateOnce(final CacheBroadcast broadcast, final boolean waitBudgetExhausted) {
    final Map<Integer, TDataNodeLocation> targets = registeredDataNodes;
    final Map<Integer, TSStatus> responses = broadcast.sendTo(targets);
    final long fenceTimeOutsMs = this.fenceTimeoutMs.getAsLong();
    final List<DataNodeState> states = new ArrayList<>(targets.size());
    for (final Integer nodeId : targets.keySet()) {
      final TSStatus status = responses.get(nodeId);
      // if the status code is not TSStatusCode.CAN_NOT_CONNECT_DATANODE,
      //  treat it as a DataNode internal execution exception
      boolean executeSuccess;
      if (status == null) {
        executeSuccess = false;
      } else {
        switch (TSStatusCode.representOf(status.getCode())) {
          case SUCCESS_STATUS:
            executeSuccess = true;
            break;
          case CAN_NOT_CONNECT_DATANODE:
            executeSuccess = false;
            break;
          default:
            // There is a DN executes procedure with internal failure
            return Verdict.FAIL;
        }
      }
      states.add(
          new DataNodeState(
              executeSuccess, elapsedMsSinceLastSuccessfulHeartbeatResponse.applyAsLong(nodeId)));
    }
    return MetadataBroadcastVerdict.decide(states, fenceTimeOutsMs, waitBudgetExhausted);
  }

  /**
   * Broadcast and retry until the verdict is {@link Verdict#PROCEED} or {@link Verdict#FAIL}.
   * Blocks the calling thread for up to {@code T_proceed}.
   */
  public boolean propagate(final CacheBroadcast broadcast) {
    final long deadlineNanos =
        nanoClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(fenceTimeoutMs.getAsLong());
    while (true) {
      final boolean waitBudgetExhausted = nanoClock.getAsLong() >= deadlineNanos;
      final Verdict verdict = propagateOnce(broadcast, waitBudgetExhausted);
      if (verdict == Verdict.PROCEED) {
        return true;
      }
      if (verdict == Verdict.FAIL) {
        return false;
      }
      try {
        sleeper.sleepMs(RETRY_INTERVAL_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }
}
