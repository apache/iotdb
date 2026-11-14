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

package org.apache.iotdb.consensus.raft.core.event;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.raft.core.MetaToken;
import org.apache.iotdb.consensus.raft.core.PersistToken;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Everything that can happen to the kernel. The driver feeds these; the kernel returns actions. */
public abstract class Event {

  private Event() {}

  /** A client request accepted by the facade. */
  public static final class ClientWrite extends Event {
    public final long reqId;
    public final ByteBuffer payload;
    public final RequestMeta meta;
    public final Object liveRequest;

    public ClientWrite(long reqId, ByteBuffer payload, RequestMeta meta, Object liveRequest) {
      this.reqId = reqId;
      this.payload = payload;
      this.meta = meta;
      this.liveRequest = liveRequest;
    }
  }

  /** A message arrived from {@code from}; {@code reply} is where a synchronous response goes. */
  public static final class Inbound extends Event {
    public final int from;
    public final Message msg;
    public final ReplyHandle reply;

    public Inbound(int from, Message msg, ReplyHandle reply) {
      this.from = from;
      this.msg = msg;
      this.reply = reply;
    }
  }

  /** The log writer finished fsync for a {@code Persist}. */
  public static final class Durable extends Event {
    public final PersistToken token;

    public Durable(PersistToken token) {
      this.token = token;
    }
  }

  /** The meta store finished fsync for a {@code PersistMeta}. */
  public static final class MetaDurable extends Event {
    public final MetaToken token;

    public MetaDurable(MetaToken token) {
      this.token = token;
    }
  }

  /**
   * The entries a {@code LoadEntries} action asked for, in the order of its indexes; null for an
   * index that is absent from the log store (or could not be read).
   */
  public static final class EntriesLoaded extends Event {
    public final long token;
    public final List<LogEntry> entries;

    public EntriesLoaded(long token, List<LogEntry> entries) {
      this.token = token;
      this.entries = entries;
    }

    @Override
    public String toString() {
      return "EntriesLoaded(token=" + token + ",n=" + entries.size() + ")";
    }
  }

  public static final class ElectionTimeout extends Event {}

  public static final class HeartbeatTick extends Event {}

  public static final class RunScheduler extends Event {}

  public static final class LeaseCheck extends Event {
    public final long nowNanos;

    /** When a majority of voters last answered this leader (-1 = unknown: the driver fills it). */
    public final long quorumContactNanos;

    public LeaseCheck(long nowNanos) {
      this(nowNanos, -1);
    }

    public LeaseCheck(long nowNanos, long quorumContactNanos) {
      this.nowNanos = nowNanos;
      this.quorumContactNanos = quorumContactNanos;
    }
  }

  /** An outbound RPC failed or timed out. */
  public static final class RpcFailed extends Event {
    public final int to;
    public final long msgId;
    public final long[] indexes;

    public RpcFailed(int to, long msgId, long[] indexes) {
      this.to = to;
      this.msgId = msgId;
      this.indexes = indexes;
    }
  }

  /** The applier finished index. */
  public static final class Applied extends Event {
    public final long index;
    public final Object status;

    public Applied(long index, Object status) {
      this.index = index;
      this.status = status;
    }
  }

  public static final class SnapshotDone extends Event {
    public final long lastIncludedIndex;
    public final long lastIncludedTerm;
    public final SnapshotMetaExt ext;

    public SnapshotDone(long lastIncludedIndex, long lastIncludedTerm, SnapshotMetaExt ext) {
      this.lastIncludedIndex = lastIncludedIndex;
      this.lastIncludedTerm = lastIncludedTerm;
      this.ext = ext;
    }
  }

  /** Ask the leader/follower kernel to take a snapshot now (facade triggerSnapshot). */
  public static final class TakeSnapshotReq extends Event {
    public final boolean force;

    public TakeSnapshotReq(boolean force) {
      this.force = force;
    }
  }

  /** A follower installed a snapshot received from the leader: reset log and state. */
  public static final class SnapshotLoaded extends Event {
    public final long lastIncludedIndex;
    public final long lastIncludedTerm;
    public final org.apache.iotdb.consensus.raft.core.Membership membership;
    public final SnapshotMetaExt ext;

    /**
     * The state machine did not load the snapshot, because it already holds everything in it
     * (RaftCore#snapshotCoveredByApplied): nothing but the compaction point changes. The answer of
     * the one check made before the load, so that the kernel resets exactly when the state machine
     * was rolled back.
     */
    public final boolean coveredByApplied;

    public SnapshotLoaded(
        long lastIncludedIndex,
        long lastIncludedTerm,
        org.apache.iotdb.consensus.raft.core.Membership membership,
        SnapshotMetaExt ext) {
      this(lastIncludedIndex, lastIncludedTerm, membership, ext, false);
    }

    public SnapshotLoaded(
        long lastIncludedIndex,
        long lastIncludedTerm,
        org.apache.iotdb.consensus.raft.core.Membership membership,
        SnapshotMetaExt ext,
        boolean coveredByApplied) {
      this.lastIncludedIndex = lastIncludedIndex;
      this.lastIncludedTerm = lastIncludedTerm;
      this.membership = membership;
      this.ext = ext;
      this.coveredByApplied = coveredByApplied;
    }
  }

  public static final class SnapshotInstalled extends Event {
    public final int follower;
    public final long lastIncludedIndex;
    public final SnapshotMetaExt ext;

    public SnapshotInstalled(int follower, long lastIncludedIndex, SnapshotMetaExt ext) {
      this.follower = follower;
      this.lastIncludedIndex = lastIncludedIndex;
      this.ext = ext;
    }
  }

  public enum ConfigOp {
    ADD_LEARNER,
    PROMOTE,
    REMOVE,
    RESET
  }

  public static final class ConfigChangeReq extends Event {
    public final ConfigOp op;
    public final int peer;
    public final TEndPoint endpoint;
    public final java.util.Map<Integer, TEndPoint> peers; // RESET
    public final CompletableFuture<Void> done;

    public ConfigChangeReq(
        ConfigOp op,
        int peer,
        TEndPoint endpoint,
        java.util.Map<Integer, TEndPoint> peers,
        CompletableFuture<Void> done) {
      this.op = op;
      this.peer = peer;
      this.endpoint = endpoint;
      this.peers = peers;
      this.done = done;
    }
  }

  public static final class TransferLeadershipReq extends Event {
    public final int target;
    public final CompletableFuture<Void> done;

    public TransferLeadershipReq(int target, CompletableFuture<Void> done) {
      this.target = target;
      this.done = done;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
