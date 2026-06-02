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

import java.util.Collection;

/**
 * Pure decision logic for the ConfigNode's metadata-broadcast verdict: after broadcasting a
 * cache-invalidation/update to all DataNodes, decide whether it is safe to commit the metadata
 * change given which DataNodes acknowledged and the state of those that did not.
 *
 * <p>Rules (design v5):
 *
 * <ul>
 *   <li>A DataNode that acknowledged is {@code ACKED}.
 *   <li>A DataNode removed from routing (no longer serving clients) or that explicitly acked
 *       fence/shutdown is {@code SAFE_GONE}. ({@code Removing} alone does NOT qualify — it may
 *       still serve clients.)
 *   <li>A DataNode that does not support self-fencing (not yet upgraded) is {@code UNSAFE},
 *       <em>checked before</em> any liveness/timing test — it can never be treated as fenced.
 *   <li>A fencing-capable DataNode out of contact for at least {@code T_proceed} is {@code
 *       FENCED_SAFE} (it has provably self-fenced).
 *   <li>Otherwise {@code UNSAFE} (still possibly serving, or only a transient error).
 * </ul>
 *
 * <p>The overall verdict is {@code PROCEED} when no DataNode is {@code UNSAFE}; otherwise {@code
 * WAIT} until the wait budget is exhausted, then {@code FAIL}. There is no "additive fast-path":
 * every Tier-A operation follows the same rule (so a Running-but-unacked DataNode is never
 * skipped).
 */
public final class MetadataBroadcastVerdict {

  public enum Verdict {
    PROCEED,
    WAIT,
    FAIL
  }

  public enum Disposition {
    ACKED,
    SAFE_GONE,
    FENCED_SAFE,
    UNSAFE
  }

  private MetadataBroadcastVerdict() {}

  /** Per-DataNode inputs for one broadcast round. */
  public static final class DataNodeState {
    private final boolean acked;
    private final boolean retiredOrFenceAcked;
    private final boolean supportsFencing;
    private final long hbAgeMs;

    public DataNodeState(
        final boolean acked,
        final boolean retiredOrFenceAcked,
        final boolean supportsFencing,
        final long hbAgeMs) {
      this.acked = acked;
      this.retiredOrFenceAcked = retiredOrFenceAcked;
      this.supportsFencing = supportsFencing;
      this.hbAgeMs = hbAgeMs;
    }
  }

  public static Disposition classify(final DataNodeState state, final long tProceedMs) {
    if (state.acked) {
      return Disposition.ACKED;
    }
    if (state.retiredOrFenceAcked) {
      return Disposition.SAFE_GONE;
    }
    if (!state.supportsFencing) {
      // Capability is checked before any timing test: a DataNode that cannot self-fence can never
      // be assumed fenced, no matter how long it has been silent.
      return Disposition.UNSAFE;
    }
    if (state.hbAgeMs >= tProceedMs) {
      return Disposition.FENCED_SAFE;
    }
    return Disposition.UNSAFE;
  }

  public static Verdict decide(
      final Collection<DataNodeState> states,
      final long tProceedMs,
      final boolean waitBudgetExhausted) {
    for (final DataNodeState state : states) {
      if (classify(state, tProceedMs) == Disposition.UNSAFE) {
        return waitBudgetExhausted ? Verdict.FAIL : Verdict.WAIT;
      }
    }
    return Verdict.PROCEED;
  }
}
