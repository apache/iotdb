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

import org.apache.iotdb.consensus.raft.core.FaultInjector;
import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.NodeEnv;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderIngestState;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** What a {@link Replicator} may read from the leader (provided by the kernel). */
public interface SchedulerContext extends NodeEnv {

  long currentTerm();

  long commitIndex();

  /** Term of the entry at {@link #commitIndex()} (0 when nothing is committed). */
  long commitTerm();

  LogView log();

  /**
   * Last completed state-machine checkpoint. It can cover retained log entries, whereas {@link
   * LogView#firstIndex()} only identifies the purged boundary. The default preserves existing
   * scheduler contexts that do not expose unpurged checkpoints.
   */
  default long snapshotIndex() {
    return log().firstIndex() - 1;
  }

  Membership membership();

  /** Members other than self (voters and learners). */
  Set<Integer> peers();

  FaultInjector faults();

  LeaderIngestState ingestState();

  /** Protocol summary of the compacted prefix ({@code SnapshotMetaExt.NONE} if none). */
  SnapshotMetaExt snapshotExt();

  /** Monotonic clock in nanoseconds (0 in the deterministic simulation). */
  long nowNanos();

  /** Fresh message id for correlation. */
  long nextMsgId();

  /** Fresh scheduling round number (TRaft shared scheduler). */
  long nextSchedRound();

  /** The replicator saw {@code index} reach an ack quorum (quorum_ready; not a commit). */
  void onQuorumReady(long index);

  /** A request template for entries starting at {@code prev+1} with this leader's commit info. */
  AppendEntriesReq template(long prevLogIndex, long prevLogTerm, boolean fence, long schedRound);

  // ------------------------------------------------------------------ leader recovery
  // (ParallelRaft term recovery; plain Raft / TRaft replicators never call these)

  /**
   * True while this leader recovers its predecessors' log (capability {@code leaderRecovery}):
   * client writes, membership changes and leadership transfers are refused and the election entry
   * waits for {@link #recoveryComplete()}.
   */
  boolean recovering();

  /**
   * Install recovered entries: remove every slot at or beyond {@code truncateFrom} (nothing if it
   * is negative or beyond the last index), then store {@code entries}, each overwriting whatever
   * its slot holds (a CONFIG entry on either side updates the configuration). {@code onDurable}
   * runs once everything is durable, and only if this leadership still exists; the apply policy is
   * told that the log changed.
   */
  void persistRecovered(List<LogEntry> entries, long truncateFrom, Runnable onDurable);

  /**
   * Persist {@code ProtocolProfile.encodeProfileMeta()} together with term and vote; {@code
   * onDurable} runs once it is durable, and only if this leadership still exists.
   */
  void persistProfileMeta(Runnable onDurable);

  /**
   * Read the entries at {@code indexes} (payloads included) from the log store; {@code callback}
   * gets them in the same order, only if this leadership still exists. Each is the copy its slot
   * holds when the callback runs, or null: the slot is empty, or the store did not return that copy
   * (its Persist still queued; an older copy the store may still hold is never passed on). Only
   * durable slots are guaranteed to load.
   */
  void loadEntries(long[] indexes, Consumer<List<LogEntry>> callback);

  /**
   * Recovery is over: leave the recovering state, rebuild the ingestion state from the recovered
   * log, append the election entry and re-evaluate commitment. No-op unless recovering.
   */
  void recoveryComplete();

  /** Resign the leadership in the current term (back to follower, election timer armed). */
  void stepDown(String reason);
}
