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

package org.apache.iotdb.consensus.raft.core.replication;

import java.util.TreeSet;

/** Leader-side per-follower replication state (Raft); TRaft's FollowerView extends it. */
public class FollowerProgress {

  public enum Mode {
    /** Looking for the match point (one probe at a time). */
    PROBE,
    /** Raft: streaming from nextIndex. */
    PIPELINE,
    /** TRaft: entry-centric partition scheduling. */
    PARTITION,
    /** Catching up through a snapshot. */
    SNAPSHOT
  }

  public final int nodeId;
  public Mode mode = Mode.PROBE;
  public long nextIndex;
  public long matchIndex = -1;

  /** Raft: last index of each in-flight batch; TRaft: in-flight entry indexes (v.flight). */
  public final TreeSet<Long> inflight = new TreeSet<>();

  public long backoffUntilNanos;
  public int consecutiveFailures;
  public long lastAppliedReported = -1;

  /** Lost transport with an unknown compacted prefix: verify it with an empty AppendEntries. */
  public boolean probeCompactedPrefix;

  /**
   * Production Raft: detect an in-flight window with no ACK advancement across heartbeat rounds.
   */
  public boolean heartbeatHadInflight;

  public long heartbeatMatchIndex = -1;

  public FollowerProgress(int nodeId, long nextIndex) {
    this.nodeId = nodeId;
    this.nextIndex = nextIndex;
  }

  /** One more RPC failure: exponential wait (policy.basicRetryWaitMs .. maxRetryWaitMs). */
  public void backoff(SchedulerContext ctx) {
    consecutiveFailures++;
    long base = ctx.policy().basicRetryWaitMs;
    if (base <= 0) {
      backoffUntilNanos = 0;
      return;
    }
    long max = Math.max(base, ctx.policy().maxRetryWaitMs);
    long wait = Math.min(max, base << Math.min(16, consecutiveFailures - 1));
    backoffUntilNanos = ctx.nowNanos() + wait * 1_000_000L;
  }

  public boolean backedOff(SchedulerContext ctx) {
    return backoffUntilNanos > ctx.nowNanos();
  }

  /** The follower answered: forget the failures. */
  public void contacted() {
    consecutiveFailures = 0;
    backoffUntilNanos = 0;
  }

  @Override
  public String toString() {
    return "FP(N"
        + nodeId
        + ","
        + mode
        + ",next="
        + nextIndex
        + ",match="
        + matchIndex
        + ",inflight="
        + inflight
        + ")";
  }
}
