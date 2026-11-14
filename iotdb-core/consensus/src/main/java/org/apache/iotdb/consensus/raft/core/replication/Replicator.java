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
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Strategy point: the leader's sending policy. Raft: per-follower pipeline with prefix pointers.
 * TRaft: the entry-centric shared scheduler. One instance lives for one leadership.
 */
public interface Replicator {

  /** Fresh leadership: initialise per-follower state from the leader's canonical log. */
  void onBecomeLeader(LogView log, Membership membership, long commitIndex, long lastApplied);

  /** The leader's own entries became durable: self-register and enqueue. */
  List<Action> onEntriesDurable(List<LogEntry> entries, SchedulerContext ctx);

  /** One scheduling round: returns the send actions (empty = nothing to do). */
  List<Action> schedule(SchedulerContext ctx);

  /** One identity-checked ack from {@code from}. */
  List<Action> onAck(int from, EntryAck ack, AppendEntriesRes res, SchedulerContext ctx);

  /** {@code from} reported that it removed every slot >= truncatedFrom. */
  void onTruncated(int from, long truncatedFrom, SchedulerContext ctx);

  /** {@code from} reported its applied index (TRaft: per-partition applied counters). */
  void onAppliedReported(int from, long appliedIndex, SchedulerContext ctx);

  /** RPC failure / timeout: clear in-flight state so the entries are retried. */
  void onRpcFailed(int to, long[] indexes, SchedulerContext ctx);

  boolean isAcked(long index, int follower);

  boolean isDurableAt(long index, int follower);

  /** True if a scheduling round could send something. */
  boolean hasWork(SchedulerContext ctx);

  /** An entry was committed (TRaft: sigma[rho]++). */
  void onCommitted(long index, LogView log);

  void onMembershipChanged(Membership membership, LogView log);

  void onSnapshotSent(int follower, long lastIncludedIndex, SnapshotMetaExt ext);

  FollowerProgress progress(int follower);

  Collection<? extends FollowerProgress> progressAll();

  /**
   * A response of the current term from {@code from}, before its acks are handed to {@link #onAck}
   * (ParallelRaft: the follower's sync number, contiguous end and first index).
   */
  default List<Action> onResponse(int from, AppendEntriesRes res, SchedulerContext ctx) {
    return Collections.emptyList();
  }

  /** The heartbeats of this round were sent (ParallelRaft: resend unanswered recovery RPCs). */
  default List<Action> onHeartbeat(SchedulerContext ctx) {
    return Collections.emptyList();
  }

  /**
   * An answer of the current term to one of the protocol's own requests ({@code RecoverTermRes},
   * {@code UpdateSyncRes}); the kernel re-evaluates commitment afterwards.
   */
  default List<Action> onProtocolResponse(int from, Message m, SchedulerContext ctx) {
    return Collections.emptyList();
  }
}
