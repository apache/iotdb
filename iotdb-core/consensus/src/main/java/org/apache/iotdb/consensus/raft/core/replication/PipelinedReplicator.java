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

import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Classic Raft replication: per-follower nextIndex/matchIndex, (prevLogIndex, prevLogTerm)
 * pointers, PROBE/PIPELINE modes and the reject/backtrack rule {@code nextIndex = min(hint,
 * nextIndex - 1)} (which always makes progress, unlike {@code raft.py:131-135}).
 */
public class PipelinedReplicator implements Replicator, LeaderStateView {

  private final TreeMap<Integer, FollowerProgress> progress = new TreeMap<>();
  private long rejectCount;
  private long retransmissions;
  private long lastLogIndex = -1;

  public long rejectCount() {
    return rejectCount;
  }

  public long retransmissions() {
    return retransmissions;
  }

  @Override
  public void onBecomeLeader(
      LogView log, Membership membership, long commitIndex, long lastApplied) {
    progress.clear();
    onMembershipChanged(membership, log);
  }

  @Override
  public void onMembershipChanged(Membership membership, LogView log) {
    progress.keySet().retainAll(membership.peersOf(self));
    for (int p : membership.peersOf(self)) {
      progress.computeIfAbsent(p, id -> new FollowerProgress(id, log.lastIndex() + 1));
    }
  }

  private int self = -1;

  /** The kernel tells the replicator who it is before {@link #onBecomeLeader}. */
  public void bindSelf(int self) {
    this.self = self;
  }

  @Override
  public List<Action> onEntriesDurable(List<LogEntry> entries, SchedulerContext ctx) {
    lastLogIndex = Math.max(lastLogIndex, ctx.log().lastIndex());
    return Collections.emptyList(); // matchIndex of self is implicit (log.isDurable)
  }

  // ------------------------------------------------------------ introspection

  @Override
  public Map<Long, Set<Integer>> pendingFollowers() {
    Map<Long, Set<Integer>> out = new TreeMap<>();
    for (FollowerProgress fp : progress.values()) {
      for (long i = fp.matchIndex + 1; i <= lastLogIndex; i++) {
        out.computeIfAbsent(i, k -> new TreeSet<>()).add(fp.nodeId);
      }
    }
    return out;
  }

  /**
   * Every follower lacks {@code (matchIndex, lastLogIndex]}: the union of these ranges, all ending
   * at {@code lastLogIndex}, is the range above the smallest match index.
   */
  @Override
  public int pendingCount() {
    long minMatch = Long.MAX_VALUE;
    for (FollowerProgress fp : progress.values()) {
      minMatch = Math.min(minMatch, fp.matchIndex);
    }
    if (minMatch == Long.MAX_VALUE || minMatch >= lastLogIndex) {
      return 0;
    }
    return (int) Math.min(Integer.MAX_VALUE, lastLogIndex - minMatch);
  }

  @Override
  public Set<Integer> rcv(long index) {
    Set<Integer> s = new TreeSet<>();
    if (self >= 0) {
      s.add(self);
    }
    for (FollowerProgress fp : progress.values()) {
      if (fp.matchIndex >= index) {
        s.add(fp.nodeId);
      }
    }
    return s;
  }

  @Override
  public Set<Integer> ackset(long index) {
    return rcv(index);
  }

  @Override
  public List<Action> schedule(SchedulerContext ctx) {
    List<Action> out = new ArrayList<>();
    LogView log = ctx.log();
    // only durable entries leave the leader: the store holds them and the leader's own vote counts
    long sendable = log.durableContiguousEnd();
    boolean probeMode = ctx.policy().probeMode;
    for (FollowerProgress fp : progress.values()) {
      if (!ctx.membership().isMember(fp.nodeId)) {
        continue;
      }
      if (fp.mode == FollowerProgress.Mode.SNAPSHOT || fp.backedOff(ctx)) {
        continue;
      }
      if (fp.probeCompactedPrefix) {
        fp.probeCompactedPrefix = false;
        long prev = log.durableContiguousEnd();
        fp.nextIndex = prev + 1;
        out.add(new Action.Send(fp.nodeId, ctx.template(prev, termAt(log, prev), false, 0)));
        continue;
      }
      if (fp.nextIndex < log.firstIndex() || preferSnapshot(fp, ctx)) {
        fp.inflight.clear();
        fp.heartbeatHadInflight = false;
        fp.mode = FollowerProgress.Mode.SNAPSHOT;
        out.add(new Action.InstallSnapshotTo(fp.nodeId));
        continue;
      }
      if (!probeMode && fp.mode == FollowerProgress.Mode.PROBE) {
        fp.mode = FollowerProgress.Mode.PIPELINE;
      }
      if (fp.mode == FollowerProgress.Mode.PROBE) {
        if (!fp.inflight.isEmpty()) {
          continue;
        }
        long prev = fp.nextIndex - 1;
        AppendEntriesReq tpl = ctx.template(prev, termAt(log, prev), false, 0);
        long[] idx = fp.nextIndex <= sendable ? new long[] {fp.nextIndex} : new long[0];
        fp.inflight.add(fp.nextIndex);
        out.add(new Action.SendAppend(Collections.singletonList(fp.nodeId), tpl, idx, null));
        continue;
      }
      int maxBatch = Math.max(1, ctx.policy().maxBatchEntries);
      int maxInflight = ctx.policy().maxInflightRpcs;
      while (fp.nextIndex <= sendable && fp.inflight.size() < maxInflight) {
        long from = fp.nextIndex;
        long to = Math.min(sendable, from + maxBatch - 1);
        while (to > from && !log.has(to)) {
          to--;
        }
        long prev = from - 1;
        AppendEntriesReq tpl = ctx.template(prev, termAt(log, prev), false, 0);
        long[] idx = new long[(int) (to - from + 1)];
        for (int i = 0; i < idx.length; i++) {
          idx[i] = from + i;
        }
        fp.inflight.add(to);
        fp.nextIndex = to + 1;
        out.add(new Action.SendAppend(Collections.singletonList(fp.nodeId), tpl, idx, null));
      }
    }
    return out;
  }

  /** Optional file-replication strategy; ordinary Raft only installs after log purging. */
  protected boolean preferSnapshot(FollowerProgress follower, SchedulerContext ctx) {
    return false;
  }

  private static long termAt(LogView log, long index) {
    if (index < log.firstIndex()) {
      return index == log.firstIndex() - 1 ? log.snapshotTerm() : 0;
    }
    return log.has(index) ? log.termAt(index) : 0;
  }

  @Override
  public List<Action> onAck(int from, EntryAck ack, AppendEntriesRes res, SchedulerContext ctx) {
    FollowerProgress fp = progress.get(from);
    if (fp == null || fp.mode == FollowerProgress.Mode.SNAPSHOT) {
      return Collections.emptyList();
    }
    if (ack.status == AckStatus.ACK) {
      fp.matchIndex = Math.max(fp.matchIndex, ack.index);
      fp.inflight.remove(ack.index);
      fp.inflight.headSet(ack.index, true).clear();
      fp.nextIndex = Math.max(fp.nextIndex, ack.index + 1);
      fp.mode = FollowerProgress.Mode.PIPELINE;
      fp.contacted();
    } else if (ack.status == AckStatus.REJECT) {
      rejectCount++;
      long ni =
          ack.hasHint() ? Math.min(ack.hintIndex, fp.nextIndex - 1) : Math.max(0, ack.index - 1);
      ni = Math.max(ni, 0); // may fall below firstIndex: the next round installs a snapshot
      if (ack.hintTerm != EntryAck.UNSET && ack.hintTerm > 0) {
        long lastOfTerm = lastIndexOfTerm(ctx.log(), ack.hintTerm);
        if (lastOfTerm >= 0) {
          ni = Math.min(ni, lastOfTerm + 1);
        }
      }
      if (ni < fp.nextIndex) {
        retransmissions += fp.nextIndex - ni;
        fp.nextIndex = ni;
      }
      fp.inflight.clear();
      fp.heartbeatHadInflight = false;
      fp.mode =
          ctx.policy().probeMode ? FollowerProgress.Mode.PROBE : FollowerProgress.Mode.PIPELINE;
      ctx.listener()
          .onEvent(
              ListenerEvent.of("reject", ctx.self())
                  .with("follower", from)
                  .with("hint", ack.hintIndex)
                  .with("next_index", fp.nextIndex)
                  .build());
    }
    return Collections.emptyList();
  }

  private static long lastIndexOfTerm(LogView log, long term) {
    for (long i = log.lastIndex(); i >= log.firstIndex(); i--) {
      if (log.has(i) && log.termAt(i) == term) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public void onTruncated(int from, long truncatedFrom, SchedulerContext ctx) {
    FollowerProgress fp = progress.get(from);
    if (fp != null && fp.matchIndex >= truncatedFrom) {
      fp.heartbeatHadInflight = false;
      fp.matchIndex = truncatedFrom - 1;
      fp.nextIndex = Math.min(fp.nextIndex, truncatedFrom);
    }
  }

  @Override
  public void onAppliedReported(int from, long appliedIndex, SchedulerContext ctx) {
    FollowerProgress fp = progress.get(from);
    if (fp != null) {
      fp.lastAppliedReported = Math.max(fp.lastAppliedReported, appliedIndex);
    }
  }

  @Override
  public void onRpcFailed(int to, long[] indexes, SchedulerContext ctx) {
    FollowerProgress fp = progress.get(to);
    if (fp == null || fp.mode == FollowerProgress.Mode.SNAPSHOT) {
      return;
    }
    rewindToKnownMatch(fp, ctx);
    fp.backoff(ctx);
  }

  private void rewindToKnownMatch(FollowerProgress fp, SchedulerContext ctx) {
    fp.inflight.clear();
    fp.heartbeatHadInflight = false;
    fp.nextIndex = fp.matchIndex + 1;
    fp.probeCompactedPrefix = false;
    if (fp.nextIndex < ctx.log().firstIndex()) {
      // A lost RPC is not evidence that the peer lost its prefix. In a new term matchIndex
      // starts at -1 even for a caught-up peer. Probe a retained tail entry before inferring
      // that it needs our snapshot, which may be older than the peer's own checkpoint.
      fp.nextIndex = ctx.log().durableContiguousEnd() + 1;
      fp.probeCompactedPrefix = true;
    }
  }

  @Override
  public List<Action> onHeartbeat(SchedulerContext ctx) {
    if (progress.isEmpty() || !ctx.policy().rpcTimeoutAsRetransmit) {
      return Collections.emptyList(); // keep the reference profile's message behavior
    }
    for (FollowerProgress fp : progress.values()) {
      if (fp.mode == FollowerProgress.Mode.SNAPSHOT || fp.backedOff(ctx) || fp.inflight.isEmpty()) {
        fp.heartbeatHadInflight = false;
        continue;
      }
      if (fp.heartbeatHadInflight && fp.matchIndex == fp.heartbeatMatchIndex) {
        // RPC completion is not proof of ACK progress. A reply without a usable ACK must not
        // hold the window forever and suppress all later data/commit heartbeats to this peer.
        long next = fp.nextIndex;
        retransmissions += Math.max(0, next - fp.matchIndex - 1);
        rewindToKnownMatch(fp, ctx);
        ctx.listener()
            .onEvent(
                ListenerEvent.of("retransmit", ctx.self())
                    .with("follower", fp.nodeId)
                    .with("match_index", fp.matchIndex)
                    .with("next_index", next)
                    .with("reason", "no-ack-progress-across-heartbeats")
                    .build());
      } else {
        fp.heartbeatHadInflight = true;
        fp.heartbeatMatchIndex = fp.matchIndex;
      }
    }
    return Collections.emptyList();
  }

  @Override
  public boolean isAcked(long index, int follower) {
    FollowerProgress fp = progress.get(follower);
    return fp != null && fp.matchIndex >= index;
  }

  @Override
  public boolean isDurableAt(long index, int follower) {
    return isAcked(index, follower);
  }

  @Override
  public boolean hasWork(SchedulerContext ctx) {
    LogView log = ctx.log();
    long sendable = log.durableContiguousEnd();
    for (FollowerProgress fp : progress.values()) {
      if (fp.mode == FollowerProgress.Mode.SNAPSHOT || fp.backedOff(ctx)) {
        continue;
      }
      if (fp.probeCompactedPrefix) {
        return true;
      }
      if (fp.nextIndex < log.firstIndex() || preferSnapshot(fp, ctx)) {
        return true;
      }
      if (fp.mode == FollowerProgress.Mode.PROBE && ctx.policy().probeMode) {
        if (fp.inflight.isEmpty()) {
          return true;
        }
        continue;
      }
      if (fp.nextIndex <= sendable && fp.inflight.size() < ctx.policy().maxInflightRpcs) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void onCommitted(long index, LogView log) {}

  @Override
  public void onSnapshotSent(int follower, long lastIncludedIndex, SnapshotMetaExt ext) {
    FollowerProgress fp = progress.get(follower);
    if (fp == null) {
      return;
    }
    fp.inflight.clear();
    fp.mode = FollowerProgress.Mode.PROBE;
    if (lastIncludedIndex >= 0) {
      fp.matchIndex = Math.max(fp.matchIndex, lastIncludedIndex);
      fp.nextIndex = lastIncludedIndex + 1;
    } else {
      fp.consecutiveFailures++; // transfer failed: the next round retries the install
    }
  }

  @Override
  public FollowerProgress progress(int follower) {
    return progress.get(follower);
  }

  @Override
  public Collection<? extends FollowerProgress> progressAll() {
    return progress.values();
  }
}
