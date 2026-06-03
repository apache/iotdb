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
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.DataNodeState;
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Verdict;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Drives one Tier-A metadata cache-invalidation broadcast to the cluster and turns "which DataNodes
 * acknowledged" into a {@link Verdict} via {@link MetadataBroadcastVerdict}. Instead of the legacy
 * "any unreachable DataNode fails the operation", a DataNode that is provably self-fenced (out of
 * ConfigNode contact for at least {@code T_proceed} and known to support fencing) is treated as
 * safe to proceed past, delivering availability without risking dirty data (see the design doc).
 *
 * <p>The caller supplies a {@link CacheBroadcast} closure wrapping its specific RPC (table
 * pre-release, schema-cache invalidation, permission-cache invalidation, ...); this class is
 * agnostic to the request type and only interprets the per-DataNode {@link TSStatus} responses.
 *
 * <p>Stateless and cheap to construct per operation. Clock and sleep are injectable for testing.
 */
public class ClusterCachePropagator {

  /**
   * {@code T_proceed = T_fence + margin}. The margin (default 5s) covers heartbeat-recording
   * granularity and scheduling jitter; see design §2.6. Kept internal (not a user knob).
   */
  private static final long DEFAULT_PROCEED_MARGIN_MS = 5_000L;

  /** How often to re-broadcast while waiting for unacked DataNodes to ack or to cross T_proceed. */
  private static final long RETRY_INTERVAL_MS = 1_000L;

  /**
   * Extra slack on top of T_proceed before giving up, so a just-died DataNode can cross T_proceed.
   */
  private static final long WAIT_BUDGET_BUFFER_MS = 5_000L;

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

  private final Supplier<Map<Integer, TDataNodeLocation>> registeredDataNodes;
  private final IntPredicate supportsFencing;
  private final IntToLongFunction hbAgeMs;
  private final LongSupplier tProceedMs;
  private final LongSupplier nanoClock;
  private final Sleeper sleeper;

  public ClusterCachePropagator(final IManager configManager) {
    this(
        () -> configManager.getNodeManager().getRegisteredDataNodeLocations(),
        nodeId -> DataNodeContactTracker.getInstance().supportsFencing(nodeId),
        nodeId -> DataNodeContactTracker.getInstance().getMillisSinceLastSuccessfulResponse(nodeId),
        () ->
            CommonDescriptor.getInstance().getConfig().getMetadataLeaseFenceMs()
                + DEFAULT_PROCEED_MARGIN_MS,
        System::nanoTime,
        Thread::sleep);
  }

  ClusterCachePropagator(
      final Supplier<Map<Integer, TDataNodeLocation>> registeredDataNodes,
      final IntPredicate supportsFencing,
      final IntToLongFunction hbAgeMs,
      final LongSupplier tProceedMs,
      final LongSupplier nanoClock,
      final Sleeper sleeper) {
    this.registeredDataNodes = registeredDataNodes;
    this.supportsFencing = supportsFencing;
    this.hbAgeMs = hbAgeMs;
    this.tProceedMs = tProceedMs;
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
  }

  /**
   * Broadcast once and classify the result. {@code waitBudgetExhausted} turns a would-be {@link
   * Verdict#WAIT} into {@link Verdict#FAIL} (the caller's retry budget ran out).
   */
  public Verdict propagateOnce(final CacheBroadcast broadcast, final boolean waitBudgetExhausted) {
    final Map<Integer, TDataNodeLocation> targets = registeredDataNodes.get();
    final Map<Integer, TSStatus> responses = broadcast.sendTo(targets);
    final long tProceed = tProceedMs.getAsLong();
    final int successCode = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    final List<DataNodeState> states = new ArrayList<>(targets.size());
    for (final Integer nodeId : targets.keySet()) {
      final TSStatus status = responses.get(nodeId);
      final boolean acked = status != null && status.getCode() == successCode;
      // retiredOrFenceAcked is left false: there is no explicit fence-ack signal yet, and a
      // Removing DataNode may still serve clients, so it must ack or be provably fenced like any
      // other (see MetadataBroadcastVerdict's SAFE_GONE rule).
      states.add(
          new DataNodeState(
              acked, false, supportsFencing.test(nodeId), hbAgeMs.applyAsLong(nodeId)));
    }
    return MetadataBroadcastVerdict.decide(states, tProceed, waitBudgetExhausted);
  }

  /**
   * Broadcast and retry until the verdict is {@link Verdict#PROCEED} (returns {@code true}) or the
   * wait budget is exhausted with at least one DataNode still unsafe ({@link Verdict#FAIL}, returns
   * {@code false}). Blocks the calling (procedure) thread for up to {@code T_proceed + buffer}.
   */
  public boolean propagate(final CacheBroadcast broadcast) {
    final long deadlineNanos =
        nanoClock.getAsLong()
            + TimeUnit.MILLISECONDS.toNanos(tProceedMs.getAsLong() + WAIT_BUDGET_BUFFER_MS);
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
