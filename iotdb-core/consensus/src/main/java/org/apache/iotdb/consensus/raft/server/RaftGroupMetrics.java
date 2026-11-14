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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * The production {@link ProtocolListener}: the same event face the invariant checker consumes,
 * turned into counters ({@code raft_consensus{type=<event>Total}}) and auto-gauges from the group
 * status ({@code term, commitIndex, lastApplied, lastLogIndex, contiguousEnd, role,
 * workingPartition, rhoMax, pendingSize}). Names go through the string API only: the {@code Metric}
 * enum is untouched.
 *
 * <p>An out-of-order protocol (ParallelRaft) also counts its recovery, catch-up, sync-number and
 * apply events ({@link #outOfOrderCounted()}) and gauges {@code syncNumber, maxCommitted,
 * pendingApply, appliedFrontier}. The gauges of one report share one status (computed on the group
 * thread, reused for {@link #STATUS_REUSE_MS}).
 */
public final class RaftGroupMetrics implements IMetricSet, ProtocolListener {

  public static final String METRIC = "raft_consensus";

  /** Counter of the recovery / catch-up pushes a node received (counted by the server). */
  public static final String PUSH = "push";

  /** How long the gauges reuse one status (one metrics report reads every gauge). */
  static final long STATUS_REUSE_MS = 250;

  /** Events worth a counter (everything else is ignored). */
  private static final String[] COUNTED = {
    "reject",
    "retransmit",
    "nack",
    "requeue",
    "rcv",
    "ackset",
    "quorum_ready",
    "wp_retreat",
    "resp_ignored",
    "truncate_beyond_frontier",
    "suffix_repair",
    "fence_done",
    "need_fence",
    "rollback",
    "stepdown",
    "snapshot_install",
    "snapshot_loaded",
    "assert",
    "client_ack",
    "config"
  };

  private static final String[] GAUGES = {
    "term",
    "commitIndex",
    "lastApplied",
    "lastLogIndex",
    "contiguousEnd",
    "role",
    "workingPartition",
    "rhoMax",
    "pendingSize"
  };

  /**
   * Out-of-order protocols (ParallelRaft) also count these: term recovery (start, pages, merge,
   * quorum, upgrade, done, step-down), sync numbers (follower upgrades and refusals, the leader's
   * UpdateSync sends and results), per-follower catch-up (rounds, completion, stalls, compacted
   * terms, pipelines, snapshots needed / deferred / re-planned), application (blocked, issued), and
   * the follower side of pushes (received, promises persisted, truncations, refused requests, dated
   * overwrites, duplicates, recovered truncations).
   */
  private static final String[] OUT_OF_ORDER_COUNTED = {
    "recovery_start",
    "recovery_page",
    "recovery_merge",
    "recovery_merge_retry",
    "recovery_quorum",
    "recovery_upgrade",
    "recovery_done",
    "recovery_stepdown",
    "sync_upgrade",
    "sync_upgrade_refused",
    "update_sync_send",
    "update_sync_result",
    "catchup_round",
    "catchup_done",
    "catchup_stalled",
    "catchup_compacted",
    "pipeline_start",
    "snapshot_needed",
    "snapshot_deferred",
    "snapshot_replanned",
    "snapshot_compacted",
    "apply_blocked",
    "apply_issue",
    PUSH,
    "push_promise",
    "push_truncate",
    "request_nack",
    "dated_overwrite",
    "dup_receive",
    "recovered_truncate"
  };

  /** Out-of-order gauges: status fields 12-14 and the applied frontier (= lastApplied there). */
  private static final String[] OUT_OF_ORDER_GAUGES = {
    "syncNumber", "maxCommitted", "pendingApply", "appliedFrontier"
  };

  private final String protocol;
  private final String region;
  private final Supplier<TGroupStatusRes> status;
  private final String[] counted;
  private final String[] gauges;
  private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private volatile TGroupStatusRes reused;
  private volatile long reusedAtNanos;

  public RaftGroupMetrics(String protocol, String region, Supplier<TGroupStatusRes> status) {
    this(protocol, region, status, false);
  }

  /** {@code outOfOrder}: also the counters and gauges of out-of-order protocols. */
  public RaftGroupMetrics(
      String protocol, String region, Supplier<TGroupStatusRes> status, boolean outOfOrder) {
    this.protocol = protocol;
    this.region = region;
    this.status = status;
    this.counted = outOfOrder ? concat(COUNTED, OUT_OF_ORDER_COUNTED) : COUNTED;
    this.gauges = outOfOrder ? concat(GAUGES, OUT_OF_ORDER_GAUGES) : GAUGES;
    for (String c : counted) {
      counts.put(c, new LongAdder());
    }
  }

  /** The events counted only for out-of-order protocols. */
  public static List<String> outOfOrderCounted() {
    return Collections.unmodifiableList(Arrays.asList(OUT_OF_ORDER_COUNTED.clone()));
  }

  private static String[] concat(String[] a, String[] b) {
    String[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  @Override
  public void onEvent(ListenerEvent ev) {
    increment(ev.name);
  }

  /** Count one {@code name} event, if it is one this set counts. */
  public void increment(String name) {
    LongAdder a = counts.get(name);
    if (a == null) {
      return;
    }
    a.increment();
    Counter c = counters.get(name);
    if (c != null) {
      c.inc();
    }
  }

  /** Current counts (for status pages and tests). */
  public Map<String, Long> counts() {
    Map<String, Long> out = new LinkedHashMap<>();
    for (String c : counted) {
      out.put(c, counts.get(c).sum());
    }
    return Collections.unmodifiableMap(out);
  }

  public long count(String event) {
    LongAdder a = counts.get(event);
    return a == null ? 0 : a.sum();
  }

  private String[] tags(String type) {
    return new String[] {
      Tag.NAME.toString(), protocol, Tag.REGION.toString(), region, Tag.TYPE.toString(), type
    };
  }

  private static double gauge(TGroupStatusRes s, String name) {
    if (s == null) {
      return -1;
    }
    switch (name) {
      case "term":
        return s.getTerm();
      case "commitIndex":
        return s.getCommitIndex();
      case "lastApplied":
        return s.getLastApplied();
      case "lastLogIndex":
        return s.getLastLogIndex();
      case "contiguousEnd":
        return s.getContiguousEnd();
      case "role":
        return s.getRole();
      case "workingPartition":
        return s.isSetWorkingPartition() ? s.getWorkingPartition() : -1;
      case "rhoMax":
        return s.isSetRhoMax() ? s.getRhoMax() : -1;
      case "pendingSize":
        return s.isSetPendingSize() ? s.getPendingSize() : -1;
      case "syncNumber":
        return s.isSetSyncNumber() ? s.getSyncNumber() : -1;
      case "maxCommitted":
        return s.isSetMaxCommitted() ? s.getMaxCommitted() : -1;
      case "pendingApply":
        return s.isSetPendingApply() ? s.getPendingApply() : -1;
      case "appliedFrontier":
        return s.getLastApplied(); // out-of-order apply: the kernel's lastApplied is the frontier
      default:
        return -1;
    }
  }

  /** The status for one report: fetched once, reused by the other gauges for a moment. */
  private TGroupStatusRes reportStatus() {
    long now = System.nanoTime();
    TGroupStatusRes s = reused;
    if (s != null && now - reusedAtNanos < STATUS_REUSE_MS * 1_000_000L) {
      return s;
    }
    try {
      s = status.get();
    } catch (RuntimeException e) {
      s = null;
    }
    reused = s;
    reusedAtNanos = now;
    return s;
  }

  /** The value of gauge {@code name} now (-1 if unknown). */
  public double gaugeValue(String name) {
    return gauge(reportStatus(), name);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (String c : counted) {
      counters.put(
          c, metricService.getOrCreateCounter(METRIC, MetricLevel.IMPORTANT, tags(c + "Total")));
    }
    for (String g : gauges) {
      ToDoubleFunction<RaftGroupMetrics> f = m -> m.gaugeValue(g);
      metricService.createAutoGauge(METRIC, MetricLevel.IMPORTANT, this, f, tags(g));
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String c : counted) {
      metricService.remove(MetricType.COUNTER, METRIC, tags(c + "Total"));
    }
    for (String g : gauges) {
      metricService.remove(MetricType.AUTO_GAUGE, METRIC, tags(g));
    }
    counters.clear();
  }
}
