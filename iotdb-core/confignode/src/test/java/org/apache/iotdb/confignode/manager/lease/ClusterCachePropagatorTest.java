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
import org.apache.iotdb.confignode.manager.lease.MetadataBroadcastVerdict.Verdict;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntToLongFunction;

public class ClusterCachePropagatorTest {

  private final long FENCE_TIMEOUT_MS = 25_000L;

  private TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus error() {
    return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
  }

  private TSStatus canNotConnect() {
    return new TSStatus(TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode());
  }

  private Map<Integer, TDataNodeLocation> twoDataNodes() {
    final Map<Integer, TDataNodeLocation> map = new HashMap<>();
    map.put(1, new TDataNodeLocation().setDataNodeId(1));
    map.put(2, new TDataNodeLocation().setDataNodeId(2));
    return map;
  }

  /** Build a propagator whose loop seams are inert (only propagateOnce is exercised). */
  private ClusterCachePropagator propagator(final IntToLongFunction hbAgeMs) {
    return new ClusterCachePropagator(
        twoDataNodes(), hbAgeMs, () -> FENCE_TIMEOUT_MS, () -> 0L, ms -> {});
  }

  @Test
  public void allAckedProceeds() {
    final ClusterCachePropagator p = propagator(id -> 0L);
    final Verdict v =
        p.propagateOnce(
            targets -> {
              final Map<Integer, TSStatus> r = new HashMap<>();
              r.put(1, success());
              r.put(2, success());
              return r;
            },
            false);
    Assert.assertEquals(Verdict.PROCEED, v);
  }

  @Test
  public void unreachableButProvablyFencedProceeds() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? FENCE_TIMEOUT_MS + 1 : 0L);
    final Verdict v = p.propagateOnce(targets -> ackOnly(1), false);
    Assert.assertEquals(Verdict.PROCEED, v);
  }

  @Test
  public void unreachableNotYetFencedWaits() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? 10_000L : 0L);
    Assert.assertEquals(Verdict.WAIT, p.propagateOnce(targets -> ackOnly(1), false));
  }

  @Test
  public void unreachableNotYetFencedFailsWhenBudgetExhausted() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? 10_000L : 0L);
    Assert.assertEquals(Verdict.FAIL, p.propagateOnce(targets -> ackOnly(1), true));
  }

  @Test
  public void canNotConnectResponseIsUnackedAndCanProceedAfterFenceTimeout() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? 999_999L : 0L);
    final Verdict v =
        p.propagateOnce(
            targets -> {
              final Map<Integer, TSStatus> r = new HashMap<>();
              r.put(1, success());
              r.put(2, canNotConnect());
              return r;
            },
            false);
    Assert.assertEquals(Verdict.PROCEED, v);
  }

  @Test
  public void canNotConnectResponseWaitsBeforeFenceTimeout() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? 1_000L : 0L);
    final Verdict v =
        p.propagateOnce(
            targets -> {
              final Map<Integer, TSStatus> r = new HashMap<>();
              r.put(1, success());
              r.put(2, canNotConnect());
              return r;
            },
            false);
    Assert.assertEquals(Verdict.WAIT, v);
  }

  @Test
  public void internalFailureFailsImmediately() {
    final ClusterCachePropagator p = propagator(id -> id == 2 ? FENCE_TIMEOUT_MS + 1 : 0L);
    final Verdict v =
        p.propagateOnce(
            targets -> {
              final Map<Integer, TSStatus> r = new HashMap<>();
              r.put(1, success());
              r.put(2, error());
              return r;
            },
            false);
    Assert.assertEquals(Verdict.FAIL, v);
  }

  @Test
  public void loopReturnsTrueWhenItEventuallyProceeds() {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicLong nanos = new AtomicLong();
    final ClusterCachePropagator p =
        new ClusterCachePropagator(
            twoDataNodes(),
            id -> id == 2 ? 10_000L : 0L, // DN2 not fenced, so round 1 must WAIT
            () -> FENCE_TIMEOUT_MS,
            nanos::get,
            ms -> nanos.addAndGet(ms * 1_000_000L));
    // Round 1: DN2 unreachable -> WAIT. Round 2: DN2 acks -> PROCEED.
    final boolean proceeded =
        p.propagate(targets -> calls.incrementAndGet() == 1 ? ackOnly(1) : ackBoth());
    Assert.assertTrue(proceeded);
    Assert.assertEquals(2, calls.get());
  }

  @Test
  public void loopReturnsFalseWhenBudgetExhausted() {
    final AtomicLong nanos = new AtomicLong();
    final ClusterCachePropagator p =
        new ClusterCachePropagator(
            twoDataNodes(),
            id -> id == 2 ? 10_000L : 0L, // DN2 never fenced (alive but not acking) -> WAIT forever
            () -> FENCE_TIMEOUT_MS,
            nanos::get,
            ms -> nanos.addAndGet(ms * 1_000_000L));
    // DN2 keeps failing to ack; the fake clock advances on each sleep until the wait budget runs
    // out, at which point the loop must give up with FAIL.
    Assert.assertFalse(p.propagate(targets -> ackOnly(1)));
  }

  private Map<Integer, TSStatus> ackOnly(final int nodeId) {
    final Map<Integer, TSStatus> r = new HashMap<>();
    r.put(nodeId, success());
    return r;
  }

  private Map<Integer, TSStatus> ackBoth() {
    final Map<Integer, TSStatus> r = new HashMap<>();
    r.put(1, success());
    r.put(2, success());
    return r;
  }
}
