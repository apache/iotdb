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

package org.apache.iotdb.consensus.raft.core.action;

import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.MetaToken;
import org.apache.iotdb.consensus.raft.core.PersistToken;
import org.apache.iotdb.consensus.raft.core.Role;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Everything the kernel asks the driver to do. The kernel itself performs no IO. */
public abstract class Action {

  private Action() {}

  /**
   * Truncate from {@code truncateFrom} (if >= 0), append {@code entries}, fsync, then feed Durable.
   */
  public static final class Persist extends Action {
    public final List<LogEntry> entries;
    public final long truncateFrom;
    public final PersistToken token;

    public Persist(List<LogEntry> entries, long truncateFrom, PersistToken token) {
      this.entries = entries;
      this.truncateFrom = truncateFrom;
      this.token = token;
    }

    @Override
    public String toString() {
      return "Persist("
          + entries
          + (truncateFrom >= 0 ? ",truncateFrom=" + truncateFrom : "")
          + ","
          + token
          + ")";
    }
  }

  /**
   * Write term/votedFor (and the profile's opaque state, if any) atomically, fsync, then feed
   * MetaDurable.
   */
  public static final class PersistMeta extends Action {
    public final long term;
    public final int votedFor;

    /** {@code ProtocolProfile.encodeProfileMeta()}; empty for Raft / TRaft (never null). */
    public final byte[] profileMeta;

    public final MetaToken token;

    public PersistMeta(long term, int votedFor, MetaToken token) {
      this(term, votedFor, new byte[0], token);
    }

    public PersistMeta(long term, int votedFor, byte[] profileMeta, MetaToken token) {
      this.term = term;
      this.votedFor = votedFor;
      this.profileMeta = profileMeta == null ? new byte[0] : profileMeta;
      this.token = token;
    }

    @Override
    public String toString() {
      return "PersistMeta(term="
          + term
          + ",vote="
          + votedFor
          + (profileMeta.length > 0 ? ",profileMeta=" + profileMeta.length + "B" : "")
          + ","
          + token
          + ")";
    }
  }

  /**
   * Read the entries at {@code indexes} from the log store, then feed {@code EntriesLoaded(token,
   * entries)} (same order; null for an index that is absent or unreadable). The driver reports what
   * its store holds when it executes the action: the server's log writer persists asynchronously,
   * so a slot whose {@code Persist} is still queued reads as nothing yet or as the slot's older
   * copy (the simulation persists at once). The kernel passes on only the copies its slots still
   * hold.
   */
  public static final class LoadEntries extends Action {
    public final long[] indexes;
    public final long token;

    public LoadEntries(long[] indexes, long token) {
      this.indexes = indexes;
      this.token = token;
    }

    @Override
    public String toString() {
      return "LoadEntries(" + java.util.Arrays.toString(indexes) + ",token=" + token + ")";
    }
  }

  /**
   * Load the entries named by {@code indexes} once and send {@code template} (with those entries)
   * to every target; {@code metaByTarget} carries per-target speculation and window info.
   */
  public static final class SendAppend extends Action {
    public final List<Integer> targets;
    public final AppendEntriesReq template;
    public final long[] indexes;
    public final Map<Integer, SendMeta> metaByTarget;

    public SendAppend(
        List<Integer> targets,
        AppendEntriesReq template,
        long[] indexes,
        Map<Integer, SendMeta> metaByTarget) {
      this.targets = targets;
      this.template = template;
      this.indexes = indexes;
      this.metaByTarget = metaByTarget == null ? Collections.emptyMap() : metaByTarget;
    }

    @Override
    public String toString() {
      return "SendAppend(to=" + targets + ",indexes=" + java.util.Arrays.toString(indexes) + ")";
    }
  }

  public static final class Send extends Action {
    public final int to;
    public final Message msg;

    public Send(int to, Message msg) {
      this.to = to;
      this.msg = msg;
    }

    @Override
    public String toString() {
      return "Send(N" + to + "," + msg.brief() + ")";
    }
  }

  public static final class Reply extends Action {
    public final ReplyHandle handle;
    public final Message res;

    public Reply(ReplyHandle handle, Message res) {
      this.handle = handle;
      this.res = res;
    }

    @Override
    public String toString() {
      return "Reply(N" + handle.requester + "," + res.brief() + ")";
    }
  }

  /** Apply entries [from, to] to the state machine in order, feeding Applied per index. */
  public static final class Apply extends Action {
    public final long fromInclusive;
    public final long toInclusive;

    public Apply(long fromInclusive, long toInclusive) {
      this.fromInclusive = fromInclusive;
      this.toInclusive = toInclusive;
    }

    @Override
    public String toString() {
      return "Apply(" + fromInclusive + ".." + toInclusive + ")";
    }
  }

  public enum Outcome {
    COMMITTED,
    NOT_LEADER,
    TIMEOUT,
    READ_ONLY,
    REJECTED
  }

  public static final class CompleteClient extends Action {
    public final long reqId;
    public final Outcome outcome;
    public final int leaderHint;

    public CompleteClient(long reqId, Outcome outcome, int leaderHint) {
      this.reqId = reqId;
      this.outcome = outcome;
      this.leaderHint = leaderHint;
    }

    @Override
    public String toString() {
      return "CompleteClient(" + reqId + "," + outcome + ")";
    }
  }

  public enum TimerKind {
    ELECTION,
    HEARTBEAT,
    LEASE
  }

  public static final class ArmTimer extends Action {
    public final TimerKind kind;
    public final long delayMs;

    public ArmTimer(TimerKind kind, long delayMs) {
      this.kind = kind;
      this.delayMs = delayMs;
    }

    @Override
    public String toString() {
      return "ArmTimer(" + kind + "," + delayMs + "ms)";
    }
  }

  public static final class CancelTimer extends Action {
    public final TimerKind kind;

    public CancelTimer(TimerKind kind) {
      this.kind = kind;
    }

    @Override
    public String toString() {
      return "CancelTimer(" + kind + ")";
    }
  }

  public static final class TakeSnapshot extends Action {
    public final long lastApplied;
    public final long term;
    public final Membership membership;
    public final SnapshotMetaExt ext;
    public final boolean force;

    public TakeSnapshot(
        long lastApplied, long term, Membership membership, SnapshotMetaExt ext, boolean force) {
      this.lastApplied = lastApplied;
      this.term = term;
      this.membership = membership;
      this.ext = ext;
      this.force = force;
    }
  }

  /** Log compaction after a snapshot: the store may drop everything below newFirstIndex. */
  public static final class CompactLog extends Action {
    public final long newFirstIndex;
    public final long snapshotTerm;
    public final byte[] profileSummary;

    public CompactLog(long newFirstIndex, long snapshotTerm, byte[] profileSummary) {
      this.newFirstIndex = newFirstIndex;
      this.snapshotTerm = snapshotTerm;
      this.profileSummary = profileSummary;
    }

    @Override
    public String toString() {
      return "CompactLog(firstIndex=" + newFirstIndex + ")";
    }
  }

  /** Persist the applied checkpoint (only after a snapshot / install, see AppliedCheckpoint). */
  public static final class SaveCheckpoint extends Action {
    public final long lastApplied;

    public SaveCheckpoint(long lastApplied) {
      this.lastApplied = lastApplied;
    }
  }

  public static final class InstallSnapshotTo extends Action {
    public final int follower;

    public InstallSnapshotTo(int follower) {
      this.follower = follower;
    }

    @Override
    public String toString() {
      return "InstallSnapshotTo(N" + follower + ")";
    }
  }

  public static final class UpdateMembership extends Action {
    public final Membership membership;
    public final boolean committed;

    public UpdateMembership(Membership membership, boolean committed) {
      this.membership = membership;
      this.committed = committed;
    }
  }

  /** Role / leader changed; the server forwards it to the state machine's event API. */
  public static final class RoleChanged extends Action {
    public final Role role;
    public final int leaderId;
    public final long term;

    public RoleChanged(Role role, int leaderId, long term) {
      this.role = role;
      this.leaderId = leaderId;
      this.term = term;
    }

    @Override
    public String toString() {
      return "RoleChanged(" + role + ",leader=N" + leaderId + ",t" + term + ")";
    }
  }

  /** The leader's first entry of the term was applied: {@code isLeaderReady} becomes true. */
  public static final class LeaderReady extends Action {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
