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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.apply.ApplyContext;
import org.apache.iotdb.consensus.raft.core.apply.ApplyPolicy;
import org.apache.iotdb.consensus.raft.core.commit.CommitDecorator;
import org.apache.iotdb.consensus.raft.core.commit.CommitPolicy;
import org.apache.iotdb.consensus.raft.core.election.RaftVoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.election.VoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.ingest.EntryFactory;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderIngestState;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderTransition;
import org.apache.iotdb.consensus.raft.core.ingest.RaftLeaderTransition;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermReq;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.profile.Capabilities;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;
import org.apache.iotdb.consensus.raft.core.replication.AcceptGate;
import org.apache.iotdb.consensus.raft.core.replication.FollowerProgress;
import org.apache.iotdb.consensus.raft.core.replication.ProtocolContext;
import org.apache.iotdb.consensus.raft.core.replication.ProtocolRequestHandler;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;
import org.apache.iotdb.consensus.raft.core.replication.SchedulerContext;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Test-only profile that turns every kernel capability on with minimal strategies, so the kernel
 * hooks can be exercised before the ParallelRaft classes exist (KernelHooksTest):
 *
 * <ul>
 *   <li>a gate accepting every entry of the request's term (of its sync term for a push), with a
 *       sync number and a push promise as profile state, answering RecoverTerm / UpdateSync;
 *   <li>a minimal out-of-order apply policy: whatever is committed and held is applied at once;
 *   <li>a commit policy committing current-term entries acked by a quorum, in any order;
 *   <li>a replicator sending every durable current-term entry on its own, without a prefix;
 *   <li>a recovery stub: the new leader persists its profile state, then completes recovery.
 * </ul>
 */
final class StubOooProfile implements ProtocolProfile {

  static final Capabilities ALL_ON = new Capabilities(true, true, true, true, 64);

  /** Shared by every node (and every restart) of one simulated cluster. */
  static final class Control {
    /** While true, a new leader stays recovering. */
    boolean holdRecovery;

    /** If set, the leader's replicator resigns (stepDown) on the next AppendEntries response. */
    boolean resignOnResponse;

    /** If set, the gate claims a prefix check (a combination the kernel refuses). */
    boolean prefixGate;

    /** If set, the gate reverses the dated overwrite: the copy with the smaller date wins. */
    boolean lowerDateWins;

    /** Every hook call, as "N1 heartbeat", "N2 response N1 ..." etc. */
    final List<String> calls = new ArrayList<>();

    /** The current profile instance of each node (replaced on restart). */
    final Map<Integer, StubOooProfile> profiles = new HashMap<>();

    /** Answers to the protocol's own requests received by leaders. */
    final List<Message> protocolResponses = new ArrayList<>();

    /** Tasks run (in order) inside the next heartbeat round of a leader. */
    final Map<Integer, Deque<Consumer<SchedulerContext>>> leaderTasks = new HashMap<>();

    void onNextHeartbeat(int node, Consumer<SchedulerContext> task) {
      leaderTasks.computeIfAbsent(node, k -> new ArrayDeque<>()).add(task);
    }

    StubOooProfile profile(int node) {
      return profiles.get(node);
    }

    long count(String prefix) {
      return calls.stream().filter(s -> s.startsWith(prefix)).count();
    }
  }

  /** Entry metadata carrying only a date (the leader stamps date = term). */
  static final class StubMeta implements EntryMeta.Dated {
    final long date;

    StubMeta(long date) {
      this.date = date;
    }

    @Override
    public long date() {
      return date;
    }

    @Override
    public EntryMeta withDate(long newDate) {
      return new StubMeta(newDate);
    }

    @Override
    public String toString() {
      return ",d" + date;
    }
  }

  static final int META_BYTES = 32;

  final Control ctl;
  int self = -1;
  // ---- profile state (persisted through encodeProfileMeta / restoreProfileMeta)
  long sync;
  long promiseSync = -1;
  long promiseBallot = -1;
  long promiseEnd = -1;

  final StubApply apply = new StubApply();
  private final StubCommit commit = new StubCommit(apply);
  private final StubFactory factory = new StubFactory();
  private final RaftVoteKeyPolicy voteKeyPolicy = new RaftVoteKeyPolicy();
  private final RaftLeaderTransition leaderTransition = new RaftLeaderTransition();

  StubOooProfile(Control ctl) {
    this.ctl = ctl;
  }

  // ------------------------------------------------------------------ profile

  @Override
  public String name() {
    return "StubOoo";
  }

  @Override
  public EntryMetaCodec metaCodec() {
    return EntryMetaCodec.EMPTY; // the simulation keeps entries as objects
  }

  @Override
  public EntryFactory entryFactory() {
    return factory;
  }

  @Override
  public VoteKeyPolicy voteKeyPolicy() {
    return voteKeyPolicy;
  }

  @Override
  public AcceptGate acceptGate(LogIndex log, NodeEnv env) {
    self = env.self();
    ctl.profiles.put(self, this);
    return new StubGate(log);
  }

  @Override
  public Replicator replicator(SchedulerContext ctx) {
    return new StubReplicator(ctx);
  }

  @Override
  public CommitDecorator commitDecorator() {
    return CommitDecorator.NONE;
  }

  @Override
  public LeaderTransition leaderTransition() {
    return leaderTransition;
  }

  @Override
  public Capabilities capabilities() {
    return ALL_ON;
  }

  @Override
  public CommitPolicy commitPolicy() {
    return commit;
  }

  @Override
  public ApplyPolicy applyPolicy() {
    return apply;
  }

  @Override
  public byte[] encodeProfileMeta() {
    return ByteBuffer.allocate(META_BYTES)
        .putLong(sync)
        .putLong(promiseSync)
        .putLong(promiseBallot)
        .putLong(promiseEnd)
        .array();
  }

  @Override
  public void restoreProfileMeta(byte[] bytes) {
    ctl.calls.add("restore " + bytes.length + "B");
    if (bytes.length == META_BYTES) {
      ByteBuffer b = ByteBuffer.wrap(bytes);
      sync = b.getLong();
      promiseSync = b.getLong();
      promiseBallot = b.getLong();
      promiseEnd = b.getLong();
    }
  }

  /** The sync number a profile-meta image carries (the first field). */
  static long syncOf(byte[] meta) {
    return meta.length == META_BYTES ? ByteBuffer.wrap(meta).getLong(0) : -1;
  }

  /** (promiseSync, promiseBallot, promiseEnd) of a profile-meta image. */
  static long[] promiseOf(byte[] meta) {
    ByteBuffer b = ByteBuffer.wrap(meta);
    return new long[] {b.getLong(8), b.getLong(16), b.getLong(24)};
  }

  private void clearPromise() {
    promiseSync = -1;
    promiseBallot = -1;
    promiseEnd = -1;
  }

  // ---------------------------------------------------------------- factory

  private final class StubFactory implements EntryFactory {
    @Override
    public LogEntry create(
        long index,
        long term,
        EntryType type,
        RequestMeta req,
        LeaderIngestState state,
        ByteBuffer payload,
        long reqId,
        Object liveRequest) {
      return new LogEntry(index, term, type, new StubMeta(term), payload, reqId, liveRequest);
    }

    @Override
    public LeaderIngestState rebuild(LogView log, SnapshotMetaExt ext, NodeEnv env) {
      ctl.calls.add("N" + env.self() + " rebuild");
      return LeaderIngestState.NONE;
    }
  }

  // ------------------------------------------------------------------- gate

  private final class StubGate implements AcceptGate, ProtocolRequestHandler {
    private final LogIndex log;

    StubGate(LogIndex log) {
      this.log = log;
    }

    @Override
    public boolean prefixCheck() {
      return ctl.prefixGate;
    }

    @Override
    public Decision check(LogEntry entry, AppendEntriesReq req, LogView log) {
      long want = req.isPush() ? req.ext.syncTerm : req.term;
      return entry.term == want ? Decision.ACCEPT : Decision.nack(EntryAck.UNSET);
    }

    @Override
    public Decision checkRequest(AppendEntriesReq req, LogView log) {
      return req.isPush() && req.ext.syncTerm != sync
          ? Decision.nack(EntryAck.UNSET)
          : Decision.ACCEPT;
    }

    @Override
    public boolean overwrites(long incomingDate, long heldDate) {
      return ctl.lowerDateWins
          ? incomingDate < heldDate
          : AcceptGate.super.overwrites(incomingDate, heldDate);
    }

    @Override
    public boolean onPushPromise(AppendEntriesReq req) {
      long end = req.ext.truncateFrom - 1;
      if (promiseSync == req.ext.syncTerm && promiseBallot == req.term && promiseEnd == end) {
        return false;
      }
      promiseSync = req.ext.syncTerm;
      promiseBallot = req.term;
      promiseEnd = end;
      return true;
    }

    @Override
    public long syncNumber() {
      return sync;
    }

    @Override
    public boolean onSnapshotInstalled(long index, long term) {
      if (term <= sync) {
        return false;
      }
      sync = term;
      clearPromise();
      return true;
    }

    @Override
    public boolean handles(Message m) {
      return m instanceof RecoverTermReq || m instanceof UpdateSyncReq;
    }

    @Override
    public Message staleReply(Message m, long currentTerm) {
      if (m instanceof RecoverTermReq) {
        return new RecoverTermRes(
            currentTerm,
            false,
            sync,
            log.contiguousEnd(),
            log.firstIndex(),
            RecoverTermRes.UNSET,
            RecoverTermRes.UNSET,
            null,
            null,
            false,
            -1);
      }
      return new UpdateSyncRes(currentTerm, false, sync, log.contiguousEnd());
    }

    @Override
    public void onRequest(Message m, int from, ReplyHandle h, ProtocolContext ctx) {
      ctl.calls.add(
          "N" + self + " request " + m.kind() + " t" + ctx.currentTerm() + " from N" + from);
      if (m instanceof RecoverTermReq) {
        RecoverTermReq rt = (RecoverTermReq) m;
        LogView lv = ctx.log();
        List<Long> held = new ArrayList<>();
        for (long i : lv.slots().tailSet(Math.max(rt.fromIndex, lv.firstIndex()), true)) {
          if (held.size() < rt.maxEntries && lv.termAt(i) == rt.syncTerm) {
            held.add(i);
          }
        }
        long[] idx = new long[held.size()];
        for (int i = 0; i < idx.length; i++) {
          idx[i] = held.get(i);
        }
        ctx.loadEntries(
            idx,
            entries -> {
              List<LogEntry> found = new ArrayList<>();
              for (LogEntry e : entries) {
                if (e != null) {
                  found.add(e);
                }
              }
              ctx.reply(
                  h,
                  new RecoverTermRes(
                      ctx.currentTerm(),
                      true,
                      sync,
                      ctx.log().contiguousEnd(),
                      ctx.log().firstIndex(),
                      promiseSync == rt.syncTerm ? promiseBallot : RecoverTermRes.UNSET,
                      promiseSync == rt.syncTerm ? promiseEnd : RecoverTermRes.UNSET,
                      found,
                      null,
                      false,
                      -1));
            });
        return;
      }
      UpdateSyncReq us = (UpdateSyncReq) m;
      if (us.newSync <= sync) {
        ctx.reply(
            h,
            new UpdateSyncRes(
                ctx.currentTerm(), sync == us.newSync, sync, ctx.log().contiguousEnd()));
        return;
      }
      sync = us.newSync;
      clearPromise();
      ctx.persistProfileMeta(
          () ->
              ctx.reply(
                  h, new UpdateSyncRes(ctx.currentTerm(), true, sync, ctx.log().contiguousEnd())));
    }
  }

  // ------------------------------------------------------------ apply policy

  /** Out-of-order application without conflict gating: committed and held = applied at once. */
  static final class StubApply implements ApplyPolicy {
    long appliedFrontier = -1;
    final TreeSet<Long> appliedBeyond = new TreeSet<>();
    final TreeSet<Long> issued = new TreeSet<>();
    long commitFrontier = -1;
    final TreeSet<Long> committedBeyond = new TreeSet<>();
    // ---- observation
    int resets;
    long lastReset = Long.MIN_VALUE;
    int logChanges;
    int commitInfos;
    final List<Long> issuedOrder = new ArrayList<>();

    /** Names of the threads that read the commit frontiers (the kernel's event thread only). */
    final Set<String> viewCallers = Collections.synchronizedSet(new HashSet<>());

    // ---- fault injection
    /** BUG: apply whatever is held, committed or not (the tests' safety check must catch it). */
    boolean applyUncommitted;

    @Override
    public void reset(long frontier, ApplyContext ctx) {
      resets++;
      lastReset = frontier;
      appliedFrontier = frontier;
      appliedBeyond.clear();
      issued.clear();
      commitFrontier = frontier;
      committedBeyond.clear();
    }

    @Override
    public void onCommitted(List<Long> indexes, ApplyContext ctx) {
      for (long i : indexes) {
        markCommitted(i);
      }
      select(ctx);
    }

    @Override
    public void onCommitInfo(AppendEntriesReq req, ApplyContext ctx) {
      commitInfos++;
      if (req.leaderCommit > commitFrontier) {
        commitFrontier = req.leaderCommit;
        committedBeyond.headSet(commitFrontier, true).clear();
      }
      if (req.ext != null) {
        for (long i : req.ext.committedInBits()) {
          markCommitted(i);
        }
      }
      normalize();
      select(ctx);
    }

    @Override
    public void onLogChanged(ApplyContext ctx) {
      logChanges++;
      select(ctx);
    }

    @Override
    public void onApplied(long index, ApplyContext ctx) {
      issued.remove(index);
      if (index > appliedFrontier) {
        appliedBeyond.add(index);
      }
      while (appliedBeyond.remove(appliedFrontier + 1)) {
        appliedFrontier++;
      }
    }

    private void markCommitted(long i) {
      if (i > commitFrontier) {
        committedBeyond.add(i);
      }
      normalize();
    }

    private void normalize() {
      while (committedBeyond.remove(commitFrontier + 1)) {
        commitFrontier++;
      }
    }

    private void select(ApplyContext ctx) {
      LogView log = ctx.log();
      long last = applyUncommitted ? log.lastIndex() : maxCommitted();
      for (long i = Math.max(appliedFrontier + 1, log.firstIndex()); i <= last; i++) {
        if ((applyUncommitted || isCommitted(i))
            && log.has(i)
            && !appliedBeyond.contains(i)
            && !issued.contains(i)) {
          issued.add(i);
          issuedOrder.add(i);
          ctx.emitApply(i, i);
        }
      }
    }

    @Override
    public long appliedFrontier() {
      return appliedFrontier;
    }

    @Override
    public long commitFrontier() {
      viewCallers.add(Thread.currentThread().getName());
      return commitFrontier;
    }

    @Override
    public long maxCommitted() {
      viewCallers.add(Thread.currentThread().getName());
      return committedBeyond.isEmpty()
          ? commitFrontier
          : Math.max(commitFrontier, committedBeyond.last());
    }

    @Override
    public boolean isCommitted(long index) {
      return index <= commitFrontier || committedBeyond.contains(index);
    }

    @Override
    public boolean appliedAtOrAfter(long index) {
      return appliedFrontier >= index
          || !appliedBeyond.tailSet(index, true).isEmpty()
          || !issued.tailSet(index, true).isEmpty();
    }

    @Override
    public NavigableSet<Long> committedBeyondFrontier() {
      return new TreeSet<>(committedBeyond);
    }
  }

  // ----------------------------------------------------------- commit policy

  /** Current-term entries the leader holds durably and a quorum acked, in any order. */
  private static final class StubCommit implements CommitPolicy {
    private final StubApply apply;

    StubCommit(StubApply apply) {
      this.apply = apply;
    }

    @Override
    public List<Long> advance(
        int self,
        long currentTerm,
        long commitIndex,
        LogView log,
        Replicator replicator,
        CommitDecorator decorator,
        Membership membership,
        Mutations mut) {
      List<Long> out = new ArrayList<>();
      for (long i : log.slots().tailSet(commitIndex, false)) {
        if (log.termAt(i) != currentTerm || !log.isDurable(i) || apply.isCommitted(i)) {
          continue;
        }
        int acks = 0;
        for (int v : membership.voters()) {
          if (v == self ? log.isDurable(i) : replicator.isAcked(i, v)) {
            acks++;
          }
        }
        if (acks >= membership.majority()) {
          out.add(i);
        }
      }
      return out;
    }
  }

  // --------------------------------------------------------------- replicator

  /** One AppendEntries per durable current-term entry and follower, prevLogIndex = -1. */
  private final class StubReplicator implements Replicator {
    private final int me;
    private final Map<Integer, FollowerProgress> views = new TreeMap<>();
    private final Map<Integer, TreeSet<Long>> acked = new HashMap<>();
    private final Map<Integer, TreeSet<Long>> nacked = new HashMap<>();
    private boolean recoveryStarted;

    StubReplicator(SchedulerContext ctx) {
      this.me = ctx.self();
    }

    private TreeSet<Long> acked(int f) {
      return acked.computeIfAbsent(f, k -> new TreeSet<>());
    }

    private TreeSet<Long> nacked(int f) {
      return nacked.computeIfAbsent(f, k -> new TreeSet<>());
    }

    @Override
    public void onBecomeLeader(
        LogView log, Membership membership, long commitIndex, long lastApplied) {
      ctl.calls.add("N" + me + " becomeLeader commit=" + commitIndex);
      onMembershipChanged(membership, log);
    }

    @Override
    public List<Action> onEntriesDurable(List<LogEntry> entries, SchedulerContext ctx) {
      return Collections.emptyList();
    }

    @Override
    public List<Action> schedule(SchedulerContext ctx) {
      List<Action> out = new ArrayList<>();
      if (ctx.recovering()) {
        if (!ctl.holdRecovery && !recoveryStarted) {
          recoveryStarted = true;
          ctl.calls.add("N" + me + " recover");
          sync = ctx.currentTerm(); // the stub's whole "recovery": adopt the own term
          clearPromise();
          ctx.persistProfileMeta(ctx::recoveryComplete);
        }
        return out;
      }
      LogView log = ctx.log();
      for (FollowerProgress fp : views.values()) {
        if (!ctx.membership().isMember(fp.nodeId)) {
          continue;
        }
        for (long i : log.slots()) {
          if (log.termAt(i) != ctx.currentTerm()
              || !log.isDurable(i)
              || fp.inflight.contains(i)
              || acked(fp.nodeId).contains(i)
              || nacked(fp.nodeId).contains(i)) {
            continue;
          }
          fp.inflight.add(i);
          out.add(
              new Action.SendAppend(
                  Collections.singletonList(fp.nodeId),
                  ctx.template(-1, 0, false, 0),
                  new long[] {i},
                  null));
        }
      }
      return out;
    }

    @Override
    public List<Action> onAck(int from, EntryAck ack, AppendEntriesRes res, SchedulerContext ctx) {
      FollowerProgress fp = views.get(from);
      if (fp == null) {
        return Collections.emptyList();
      }
      if (ack.status == AckStatus.ACK) {
        acked(from).add(ack.index);
        fp.inflight.remove(ack.index);
        fp.matchIndex = Math.max(fp.matchIndex, ack.index);
        fp.contacted();
      } else if (ack.status == AckStatus.NACK) {
        fp.inflight.remove(ack.index);
        nacked(from).add(ack.index);
      }
      return Collections.emptyList();
    }

    @Override
    public void onTruncated(int from, long truncatedFrom, SchedulerContext ctx) {
      acked(from).tailSet(truncatedFrom, true).clear();
    }

    @Override
    public void onAppliedReported(int from, long appliedIndex, SchedulerContext ctx) {}

    @Override
    public void onRpcFailed(int to, long[] indexes, SchedulerContext ctx) {
      FollowerProgress fp = views.get(to);
      if (fp != null) {
        for (long i : indexes) {
          fp.inflight.remove(i);
        }
      }
    }

    @Override
    public boolean isAcked(long index, int follower) {
      return acked(follower).contains(index);
    }

    @Override
    public boolean isDurableAt(long index, int follower) {
      return isAcked(index, follower);
    }

    @Override
    public boolean hasWork(SchedulerContext ctx) {
      return false;
    }

    @Override
    public void onCommitted(long index, LogView log) {}

    @Override
    public void onMembershipChanged(Membership membership, LogView log) {
      views.keySet().retainAll(membership.peersOf(me));
      for (int p : membership.peersOf(me)) {
        views.computeIfAbsent(p, id -> new FollowerProgress(id, log.lastIndex() + 1));
      }
    }

    @Override
    public void onSnapshotSent(int follower, long lastIncludedIndex, SnapshotMetaExt ext) {}

    @Override
    public FollowerProgress progress(int follower) {
      return views.get(follower);
    }

    @Override
    public Collection<? extends FollowerProgress> progressAll() {
      return views.values();
    }

    // ---- the new hooks

    @Override
    public List<Action> onResponse(int from, AppendEntriesRes res, SchedulerContext ctx) {
      ctl.calls.add(
          "N"
              + me
              + " response N"
              + from
              + " sync="
              + res.syncNumber
              + " first="
              + res.logFirstIndex);
      if (ctl.resignOnResponse) {
        ctl.resignOnResponse = false;
        ctx.stepDown("resign");
      }
      return Collections.emptyList();
    }

    @Override
    public List<Action> onHeartbeat(SchedulerContext ctx) {
      ctl.calls.add("N" + me + " heartbeat");
      Deque<Consumer<SchedulerContext>> tasks = ctl.leaderTasks.get(me);
      while (tasks != null && !tasks.isEmpty()) {
        tasks.poll().accept(ctx);
      }
      return Collections.emptyList();
    }

    @Override
    public List<Action> onProtocolResponse(int from, Message m, SchedulerContext ctx) {
      ctl.calls.add("N" + me + " protocolResponse " + m.kind() + " from N" + from);
      ctl.protocolResponses.add(m);
      return Collections.emptyList();
    }
  }
}
