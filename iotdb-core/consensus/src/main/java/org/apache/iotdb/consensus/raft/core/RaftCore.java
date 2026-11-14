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
import org.apache.iotdb.consensus.raft.core.commit.CommitPolicy;
import org.apache.iotdb.consensus.raft.core.commit.ContiguousQuorumCommitPolicy;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderIngestState;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.log.MembershipCodec;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.AppendExt;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermReq;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.core.msg.TimeoutNowReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.policy.ProtocolPolicy;
import org.apache.iotdb.consensus.raft.core.profile.Capabilities;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;
import org.apache.iotdb.consensus.raft.core.replication.AcceptGate;
import org.apache.iotdb.consensus.raft.core.replication.PipelinedReplicator;
import org.apache.iotdb.consensus.raft.core.replication.ProtocolContext;
import org.apache.iotdb.consensus.raft.core.replication.ProtocolRequestHandler;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;
import org.apache.iotdb.consensus.raft.core.replication.SchedulerContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * The sans-IO Raft kernel: {@code onEvent(Event) -> List<Action>}. It never blocks, never touches
 * the clock, disk or network; the driver executes the actions and feeds results back as events.
 * Protocol variants (TRaft, ParallelRaft) plug in through {@link ProtocolProfile}; the kernel
 * itself knows nothing about partitions or sync numbers. Every out-of-order mechanism is guarded by
 * a {@link Capabilities} switch or a null {@link ApplyPolicy}, so Raft / TRaft (every switch off,
 * no apply policy) take exactly the original paths.
 */
public final class RaftCore implements SchedulerContext, ApplyContext, ProtocolContext {

  enum MetaKind {
    CAMPAIGN,
    VOTE,
    ADOPT,
    /** The profile's own state ({@code persistProfileMeta}): run the callback once durable. */
    PROFILE
  }

  /** Context remembered between PersistMeta and MetaDurable. */
  static final class MetaCtx {
    final MetaKind kind;
    final long term;
    final int cand;
    final ReplyHandle handle;
    final Event.Inbound resume;
    final Runnable onDurable;

    MetaCtx(MetaKind kind, long term, int cand, ReplyHandle handle, Event.Inbound resume) {
      this(kind, term, cand, handle, resume, null);
    }

    MetaCtx(
        MetaKind kind,
        long term,
        int cand,
        ReplyHandle handle,
        Event.Inbound resume,
        Runnable onDurable) {
      this.kind = kind;
      this.term = term;
      this.cand = cand;
      this.handle = handle;
      this.resume = resume;
      this.onDurable = onDurable;
    }
  }

  /** Context remembered between a leader-side Persist and its Durable. */
  private static final class LeaderPersist {
    final List<LogEntry> entries;
    final boolean alreadyRegistered;
    final boolean truncateOnly;

    LeaderPersist(List<LogEntry> entries, boolean alreadyRegistered, boolean truncateOnly) {
      this.entries = entries;
      this.alreadyRegistered = alreadyRegistered;
      this.truncateOnly = truncateOnly;
    }
  }

  /**
   * Context remembered between the suffix cut of a snapshot install (R13a, out-of-order commit
   * only) and its Durable: the profile state the gate raised is persisted only then.
   */
  private static final class SnapshotCut {
    final boolean persistProfile;

    SnapshotCut(boolean persistProfile) {
      this.persistProfile = persistProfile;
    }
  }

  /** Context remembered between a {@code persistRecovered} Persist and its Durable. */
  private static final class RecoveredPersist {
    final List<LogEntry> entries;
    final Runnable onDurable;

    RecoveredPersist(List<LogEntry> entries, Runnable onDurable) {
      this.entries = entries;
      this.onDurable = onDurable;
    }
  }

  /**
   * Who the node was when a profile callback was registered: its term and, if it was the leader,
   * that leadership. A callback outliving it (term change, step-down) is dropped, so that a
   * finished leadership's recovery can never act on a later one.
   */
  private final class CallbackScope {
    final long term = st.currentTerm;
    final Replicator leadership = st.isLeader() ? replicator : null;

    boolean current() {
      if (st.currentTerm != term) {
        return false;
      }
      return leadership == null || (st.isLeader() && replicator == leadership);
    }
  }

  final int self;
  final RaftState st;
  final LogIndex log;
  final ProtocolProfile profile;
  final ProtocolPolicy policy;
  final RaftCoreConfig cfg;
  final ProtocolListener listener;
  final FaultInjector faults;
  final RandomSource random;
  final Mutations mut;
  final AcceptGate gate;
  private final java.util.function.LongSupplier clock;
  private final ElectionModule election;
  private final AppendReceiver receiver;
  private final CommitPolicy commitPolicy;

  /** Out-of-order apply strategy; null = the built-in prefix application (Raft / TRaft). */
  final ApplyPolicy applyPolicy;

  /** Kernel capability switches of the profile; {@link Capabilities#NONE} for Raft / TRaft. */
  final Capabilities caps;

  private Replicator replicator;
  private LeaderIngestState ingestState = LeaderIngestState.NONE;
  private SnapshotMetaExt snapshotExt = SnapshotMetaExt.NONE;
  private long tokenSeq;
  private long msgSeq;
  private long schedRound;
  private boolean leadershipPending;

  /**
   * The term whose election {@link #leadershipPending} belongs to (its truncation is being
   * persisted).
   */
  private long leadershipPendingTerm = -1;

  private long snapshotIndex = -1;
  private long snapshotTerm;
  private boolean snapshotInProgress;

  /** CONFIG entries still in the log (index -> configuration); membership = last, else base. */
  private final java.util.TreeMap<Long, Membership> configEntries = new java.util.TreeMap<>();

  private Membership baseMembership;
  private long pendingConfigIndex = -1;
  private java.util.concurrent.CompletableFuture<Void> pendingConfigDone;
  private int transferTarget = -1;
  private java.util.concurrent.CompletableFuture<Void> transferDone;
  private final Map<Long, Object> pendingPersist = new HashMap<>();
  private final Map<Long, MetaCtx> pendingMeta = new HashMap<>();

  /** LoadEntries token -> callback awaiting its EntriesLoaded. */
  private final Map<Long, Consumer<List<LogEntry>>> pendingLoads = new HashMap<>();

  /**
   * Out-of-order commit: {@link #commitIndexViewForRole()} and {@link #readIndexView()} as of the
   * end of the last event. Status RPCs, metrics and admin callers read these views on their own
   * threads, while the apply policy and the committed set they derive from belong to the event
   * thread; so the views are computed there and published.
   */
  private volatile long publishedCommitView = -1;

  private volatile long publishedReadView = -1;
  private List<Action> out = new ArrayList<>();

  public RaftCore(
      int self,
      Membership initial,
      LogIndex log,
      long persistedTerm,
      int persistedVote,
      long lastApplied,
      boolean restarted,
      ProtocolProfile profile,
      ProtocolPolicy policy,
      RaftCoreConfig cfg,
      ProtocolListener listener,
      FaultInjector faults,
      RandomSource random,
      Mutations mut) {
    this(
        self,
        initial,
        log,
        persistedTerm,
        persistedVote,
        lastApplied,
        restarted,
        profile,
        policy,
        cfg,
        listener,
        faults,
        random,
        mut,
        () -> 0L);
  }

  public RaftCore(
      int self,
      Membership initial,
      LogIndex log,
      long persistedTerm,
      int persistedVote,
      long lastApplied,
      boolean restarted,
      ProtocolProfile profile,
      ProtocolPolicy policy,
      RaftCoreConfig cfg,
      ProtocolListener listener,
      FaultInjector faults,
      RandomSource random,
      Mutations mut,
      java.util.function.LongSupplier clock) {
    this.clock = clock;
    this.self = self;
    this.st = new RaftState(initial);
    this.log = log;
    this.profile = profile;
    this.policy = policy;
    this.cfg = cfg;
    this.listener = listener;
    this.faults = faults;
    this.random = random;
    this.mut = mut;
    this.st.currentTerm = persistedTerm;
    this.st.votedFor = persistedVote;
    this.st.lastApplied = lastApplied;
    this.st.applyIssuedUpTo = lastApplied;
    this.st.commitIndex = lastApplied;
    this.st.restarted = restarted;
    this.baseMembership = initial;
    Capabilities c = profile.capabilities();
    this.caps = c == null ? Capabilities.NONE : c;
    CommitPolicy cp = profile.commitPolicy();
    this.commitPolicy = cp == null ? ContiguousQuorumCommitPolicy.INSTANCE : cp;
    this.applyPolicy = profile.applyPolicy();
    this.log.setChainValidator(profile.chainValidator());
    this.gate = profile.acceptGate(log, this);
    if (caps.ackBeyondHoles && (gate.prefixCheck() || policy.leaderFence)) {
      // The out-of-order receiver cuts a recovery push's truncation from the in-memory log before
      // the prefix / fence checks run; a request those checks then answered would leave the cut
      // unpersisted (and unreported). They are prefix mechanisms the protocol does not use.
      throw new IllegalArgumentException(
          "capability ackBeyondHoles requires an accept gate without prefix check and"
              + " policy.leaderFence = false");
    }
    this.election = new ElectionModule(this);
    this.receiver = new AppendReceiver(this);
    if (applyPolicy != null) {
      applyPolicy.reset(lastApplied, this); // state only: no event yet to carry actions
    }
    if (caps.outOfOrderCommit) {
      publishViews();
    }
  }

  // ================================================================== entry

  /** The only entry point. Same-group calls must be serialized by the driver. */
  public List<Action> onEvent(Event ev) {
    List<Action> collected = new ArrayList<>();
    List<Action> saved = out;
    out = collected;
    try {
      dispatch(ev);
      if (caps.outOfOrderCommit) {
        publishViews();
      }
    } finally {
      out = saved;
    }
    return collected;
  }

  private void dispatch(Event ev) {
    if (ev instanceof Event.ClientWrite) {
      onClientWrite((Event.ClientWrite) ev);
    } else if (ev instanceof Event.Inbound) {
      onInbound((Event.Inbound) ev);
    } else if (ev instanceof Event.Durable) {
      onDurable(((Event.Durable) ev).token);
    } else if (ev instanceof Event.MetaDurable) {
      onMetaDurable(((Event.MetaDurable) ev).token);
    } else if (ev instanceof Event.ElectionTimeout) {
      election.startElection(false);
    } else if (ev instanceof Event.HeartbeatTick) {
      onHeartbeatTick();
    } else if (ev instanceof Event.RunScheduler) {
      runScheduler();
    } else if (ev instanceof Event.RpcFailed) {
      Event.RpcFailed f = (Event.RpcFailed) ev;
      if (st.isLeader() && replicator != null) {
        replicator.onRpcFailed(f.to, f.indexes, this);
      }
    } else if (ev instanceof Event.Applied) {
      onApplied((Event.Applied) ev);
    } else if (ev instanceof Event.SnapshotDone) {
      onSnapshotDone((Event.SnapshotDone) ev);
    } else if (ev instanceof Event.TakeSnapshotReq) {
      requestSnapshot(((Event.TakeSnapshotReq) ev).force);
    } else if (ev instanceof Event.SnapshotLoaded) {
      onSnapshotLoaded((Event.SnapshotLoaded) ev);
    } else if (ev instanceof Event.SnapshotInstalled) {
      Event.SnapshotInstalled s = (Event.SnapshotInstalled) ev;
      if (st.isLeader() && replicator != null) {
        replicator.onSnapshotSent(s.follower, s.lastIncludedIndex, s.ext);
      }
    } else if (ev instanceof Event.LeaseCheck) {
      onLeaseCheck((Event.LeaseCheck) ev);
    } else if (ev instanceof Event.ConfigChangeReq || ev instanceof Event.TransferLeadershipReq) {
      onAdmin(ev);
    } else if (ev instanceof Event.EntriesLoaded) {
      onEntriesLoaded((Event.EntriesLoaded) ev);
    }
  }

  /** Also re-entered by the receiver to replay a request once the profile state is durable. */
  void onInbound(Event.Inbound in) {
    Message m = in.msg;
    if (m instanceof AppendEntriesReq) {
      receiver.onAppendEntries((AppendEntriesReq) m, in.from, in.reply);
    } else if (m instanceof AppendEntriesRes) {
      onAppendEntriesRes((AppendEntriesRes) m, in.from);
    } else if (m instanceof RequestVoteReq) {
      election.onRequestVote((RequestVoteReq) m, in.from, in.reply);
    } else if (m instanceof RequestVoteRes) {
      election.onRequestVoteRes((RequestVoteRes) m, in.from);
    } else if (m instanceof TimeoutNowReq) {
      if (m.term >= st.currentTerm && st.membership.isVoter(self)) {
        election.startElection(true);
      }
    } else if (m instanceof RecoverTermReq || m instanceof UpdateSyncReq) {
      onProtocolRequest(m, in.from, in.reply);
    } else if (m instanceof RecoverTermRes || m instanceof UpdateSyncRes) {
      onProtocolResponse(m, in.from);
    }
  }

  /**
   * A protocol's own request (ParallelRaft RecoverTerm / UpdateSync) under the Raft term rules: an
   * older term gets the handler's stale answer, a newer term is adopted (durably) before the
   * request is replayed, and the current term makes this node a follower of the sender before the
   * gate handles it. A profile whose gate does not handle it (Raft, TRaft) ignores it, as the
   * kernel always did.
   */
  private void onProtocolRequest(Message m, int from, ReplyHandle h) {
    if (!(gate instanceof ProtocolRequestHandler) || !((ProtocolRequestHandler) gate).handles(m)) {
      return;
    }
    ProtocolRequestHandler handler = (ProtocolRequestHandler) gate;
    if (m.term < st.currentTerm) {
      Message stale = handler.staleReply(m, st.currentTerm);
      if (stale != null) {
        emit(new Action.Reply(h, stale));
      }
      return;
    }
    if (m.term > st.currentTerm) {
      adoptTerm(m.term, new Event.Inbound(from, m, h));
      return;
    }
    if (st.role == Role.LEADER) {
      return; // same term, two leaders: impossible under Election Safety; ignore defensively
    }
    st.role = Role.FOLLOWER;
    if (st.leaderId != from) {
      st.leaderId = from;
      emit(new Action.RoleChanged(Role.FOLLOWER, from, st.currentTerm));
    }
    armElectionTimer();
    handler.onRequest(m, from, h, this);
  }

  /**
   * The answer to one of the protocol's own requests, for the current leadership only. Ignored (as
   * the kernel always did) by a profile that does not speak these messages (Raft, TRaft).
   */
  private void onProtocolResponse(Message m, int from) {
    if (!(gate instanceof ProtocolRequestHandler)) {
      return;
    }
    if (m.term > st.currentTerm) {
      adoptTerm(m.term, null);
      return;
    }
    if (!st.isLeader() || m.term < st.currentTerm || replicator == null) {
      record(
          ListenerEvent.of("resp_ignored", self)
              .with("resp", m.brief())
              .with("reason", "stale-term")
              .build());
      return;
    }
    for (Action a : replicator.onProtocolResponse(from, m, this)) {
      emit(a);
    }
    advanceCommit();
    checkTransfer();
  }

  private void onEntriesLoaded(Event.EntriesLoaded ev) {
    Consumer<List<LogEntry>> callback = pendingLoads.remove(ev.token);
    if (callback == null) {
      return;
    }
    List<LogEntry> held = new ArrayList<>(ev.entries.size());
    for (LogEntry e : ev.entries) {
      held.add(e != null && isHeldCopy(e) ? e : null);
    }
    callback.accept(held);
  }

  /**
   * True if {@code e}, read back from the log store, is the copy its slot holds now: same term,
   * type and, for dated metadata, date (the metadata object itself does not survive the store). The
   * driver reads the store when it executes LoadEntries, but the server's log writer persists
   * asynchronously: a slot overwritten or cut since its last Durable can still read as its older
   * copy, which must not pass for the current one. A profile that rewrites a slot within a term
   * therefore changes its date (ParallelRaft re-dates).
   */
  private boolean isHeldCopy(LogEntry e) {
    if (!log.has(e.index) || log.termAt(e.index) != e.term || log.typeAt(e.index) != e.type) {
      return false;
    }
    EntryMeta held = log.metaAt(e.index);
    boolean dated = held instanceof EntryMeta.Dated;
    if (dated != (e.meta instanceof EntryMeta.Dated)) {
      return false;
    }
    return !dated || ((EntryMeta.Dated) held).date() == ((EntryMeta.Dated) e.meta).date();
  }

  // ============================================================ helpers

  void emit(Action a) {
    out.add(a);
  }

  void record(ListenerEvent ev) {
    listener.onEvent(ev);
  }

  void armElectionTimer() {
    long span = Math.max(1, cfg.electionTimeoutMaxMs - cfg.electionTimeoutMinMs);
    emit(
        new Action.ArmTimer(
            Action.TimerKind.ELECTION, cfg.electionTimeoutMinMs + random.nextLong(span)));
  }

  VoteKey myVoteKey() {
    return profile.voteKeyPolicy().extract(log, snapshotExt, knownCommitBound(), mut, st.restarted);
  }

  private long knownCommitBound() {
    return st.isLeader() ? st.commitIndex : Math.min(st.knownLeaderCommit, log.contiguousEnd());
  }

  void persist(List<LogEntry> entries, long truncateFrom, Object ctx) {
    PersistToken t = new PersistToken(++tokenSeq);
    pendingPersist.put(t.id, ctx);
    emit(new Action.Persist(entries, truncateFrom, t));
  }

  void persistMeta(MetaCtx ctx) {
    MetaToken t = new MetaToken(++tokenSeq);
    pendingMeta.put(t.id, ctx);
    int vote = mut.has("no_persist_votedfor") ? -1 : st.votedFor;
    emit(new Action.PersistMeta(st.currentTerm, vote, profile.encodeProfileMeta(), t));
  }

  /** Move to a newer term as follower; {@code resume} (if any) is re-dispatched once durable. */
  void adoptTerm(long term, Event.Inbound resume) {
    boolean stepping = st.role == Role.LEADER || st.role == Role.CANDIDATE;
    boolean wasLeader = st.role == Role.LEADER;
    if (wasLeader) {
      leaveLeadership();
    }
    st.currentTerm = term;
    st.votedFor = -1;
    st.role = Role.FOLLOWER;
    st.leaderId = -1; // the leader of the new term is learned from its first AppendEntries
    st.votes.clear();
    st.preVotes.clear();
    st.fencedTerm = -1;
    if (stepping) {
      record(ListenerEvent.of("stepdown", self).with("term", term).build());
      emit(new Action.RoleChanged(Role.FOLLOWER, -1, term));
    }
    if (wasLeader && st.membership.isVoter(self)) {
      // becoming leader cancelled the election timer: without it the node would never campaign
      // again unless some message arrives (a candidate and a follower keep theirs running)
      armElectionTimer();
    }
    persistMeta(new MetaCtx(MetaKind.ADOPT, term, -1, null, resume));
  }

  void leaveLeadership() {
    for (Map.Entry<Long, Long> e : st.myReqs.entrySet()) {
      if (!isCommittedIndex(e.getValue())) {
        emit(new Action.CompleteClient(e.getKey(), Action.Outcome.NOT_LEADER, -1));
      }
    }
    st.myReqs.clear();
    st.reqByIndex.clear();
    st.committed.clear();
    st.leaderReady = false;
    st.recovering = false;
    st.leaderFirstIndexOfTerm = -1;
    replicator = null;
    ingestState = LeaderIngestState.NONE;
    if (pendingConfigDone != null) {
      pendingConfigDone.completeExceptionally(new IllegalStateException("leadership lost"));
      pendingConfigDone = null;
    }
    pendingConfigIndex = -1;
    if (transferDone != null) {
      transferDone.complete(null); // a lost leadership is what a transfer wants
      transferDone = null;
    }
    transferTarget = -1;
    emit(new Action.CancelTimer(Action.TimerKind.HEARTBEAT));
    emit(new Action.CancelTimer(Action.TimerKind.LEASE));
  }

  private void onMetaDurable(MetaToken token) {
    MetaCtx c = pendingMeta.remove(token.id);
    if (c == null) {
      return;
    }
    switch (c.kind) {
      case CAMPAIGN:
        election.onCampaignDurable(c);
        break;
      case VOTE:
        election.onVoteDurable(c);
        break;
      case ADOPT:
        if (c.resume != null && c.resume.msg.term == st.currentTerm) {
          onInbound(c.resume);
        }
        break;
      case PROFILE:
        if (c.onDurable != null) {
          c.onDurable.run();
        }
        break;
      default:
        break;
    }
  }

  // ============================================================ leadership

  void becomeLeader() {
    long from = profile.leaderTransition().truncateFrom(log);
    if (from <= log.lastIndex()) {
      List<Long> dropped = log.removeFrom(from);
      onLogTruncated(from);
      record(ListenerEvent.of("truncate_beyond_frontier", self).with("dropped", dropped).build());
      leadershipPending = true;
      leadershipPendingTerm = st.currentTerm;
      persist(new ArrayList<>(), from, new LeaderPersist(Collections.emptyList(), false, true));
      return;
    }
    finishBecomeLeader();
  }

  private void finishBecomeLeader() {
    leadershipPending = false;
    st.role = Role.LEADER;
    st.leaderId = self;
    st.votes.clear();
    st.fencedTerm = -1;
    ingestState = profile.entryFactory().rebuild(log, snapshotExt, this);
    replicator = profile.replicator(this);
    if (replicator instanceof PipelinedReplicator) {
      ((PipelinedReplicator) replicator).bindSelf(self);
    }
    if (caps.outOfOrderCommit) {
      initOutOfOrderCommit();
    } else {
      st.commitIndex = st.lastApplied;
      st.committed.clear();
      for (long i = log.firstIndex(); i <= st.lastApplied; i++) {
        st.committed.add(i);
      }
    }
    st.myReqs.clear();
    st.reqByIndex.clear();
    st.leaderFirstIndexOfTerm = -1;
    boolean recover = caps.leaderRecovery;
    st.recovering = recover;
    st.leaderReady = !recover && !policy.noopOnElection;
    replicator.onBecomeLeader(log, st.membership, st.commitIndex, st.lastApplied);
    VoteKey frontier = myVoteKey();
    record(
        ListenerEvent.of("leader", self)
            .with("term", st.currentTerm)
            .with("frontier", frontier)
            .with("t_hat", frontier.lastTerm)
            .with("rho_hat", frontier.rhoHat)
            .with("sigma_hat", frontier.sigmaHat)
            .build());
    emit(new Action.RoleChanged(Role.LEADER, self, st.currentTerm));
    emit(new Action.CancelTimer(Action.TimerKind.ELECTION));
    emit(new Action.ArmTimer(Action.TimerKind.HEARTBEAT, cfg.heartbeatIntervalMs));
    if (cfg.leaderLeaseCheckMs > 0) {
      emit(new Action.ArmTimer(Action.TimerKind.LEASE, cfg.leaderLeaseCheckMs));
    }
    if (recover) {
      // R1: the election entry (and LeaderReady) wait for recoveryComplete(). A heartbeat goes to
      // every voter at once instead: the followers learn of the new leader without waiting for the
      // heartbeat timer, and their answers count as contact for the lease while the recovery runs
      for (int p : peers()) {
        if (st.membership.isVoter(p)) {
          emit(new Action.Send(p, template(-1, 0, false, 0)));
        }
      }
      return;
    }
    appendElectionEntry();
  }

  /**
   * Out-of-order commit: the new leader starts from what its apply policy knows to be committed,
   * the contiguous prefix as {@code commitIndex} and the indexes beyond it in {@code committed}.
   */
  private void initOutOfOrderCommit() {
    st.committed.clear();
    st.commitIndex = st.lastApplied;
    if (applyPolicy != null) {
      st.commitIndex = Math.max(st.commitIndex, applyPolicy.commitFrontier());
      for (long i : applyPolicy.committedBeyondFrontier()) {
        if (i > st.commitIndex) {
          st.committed.add(i);
        }
      }
      while (st.committed.remove(st.commitIndex + 1)) {
        st.commitIndex++;
      }
    }
  }

  /** The election entry (NOOP, or TRaft's BARRIER) or, if there is none, LeaderReady at once. */
  private void appendElectionEntry() {
    if (policy.noopOnElection) {
      long index = log.lastIndex() + 1;
      LogEntry noop =
          profile
              .entryFactory()
              .create(
                  index,
                  st.currentTerm,
                  profile.leaderTransition().firstEntryType(),
                  RequestMeta.OPAQUE,
                  ingestState,
                  ByteBuffer.allocate(0),
                  LogEntry.NO_REQUEST,
                  null);
      st.leaderFirstIndexOfTerm = index;
      log.put(noop, false);
      persist(
          Collections.singletonList(noop),
          -1,
          new LeaderPersist(Collections.singletonList(noop), false, false));
    } else {
      st.leaderReady = true;
      emit(new Action.LeaderReady());
    }
  }

  // ============================================================ ingestion

  private void onClientWrite(Event.ClientWrite w) {
    if (!st.isLeader() || leadershipPending) {
      emit(new Action.CompleteClient(w.reqId, Action.Outcome.NOT_LEADER, st.leaderId));
      return;
    }
    if (st.recovering) {
      // like a fresh Raft leader whose election entry is not applied yet: retry here later
      emit(new Action.CompleteClient(w.reqId, Action.Outcome.NOT_LEADER, self));
      return;
    }
    if (transferTarget >= 0) {
      emit(new Action.CompleteClient(w.reqId, Action.Outcome.NOT_LEADER, transferTarget));
      return;
    }
    if (caps.outOfOrderCommit && pendingConfigIndex > st.commitIndex) {
      // membership barrier: no new entry while a configuration change is uncommitted
      emit(new Action.CompleteClient(w.reqId, Action.Outcome.REJECTED, self));
      return;
    }
    long index = log.lastIndex() + 1;
    LogEntry e =
        profile
            .entryFactory()
            .create(
                index,
                st.currentTerm,
                EntryType.DATA,
                w.meta,
                ingestState,
                w.payload,
                w.reqId,
                w.liveRequest);
    if (w.reqId != LogEntry.NO_REQUEST) {
      Long previous = st.myReqs.put(w.reqId, index);
      if (previous != null) {
        st.reqByIndex.remove(previous, w.reqId); // a reused id is acknowledged at its latest index
      }
      st.reqByIndex.put(index, w.reqId);
    }
    faults.at(CrashPoint.INGEST_PRE_PERSIST);
    List<LogEntry> one = Collections.singletonList(e);
    if (mut.has("ack_before_persist")) {
      // BUG (A-8): entry registered (commit may advance) before it is durably persisted
      log.put(e, true);
      for (Action a : replicator.onEntriesDurable(one, this)) {
        emit(a);
      }
      advanceCommit();
      faults.at(CrashPoint.INGEST_MID);
      persist(one, -1, new LeaderPersist(one, true, false));
      return;
    }
    log.put(e, false);
    persist(one, -1, new LeaderPersist(one, false, false));
  }

  private void onDurable(PersistToken token) {
    Object ctx = pendingPersist.remove(token.id);
    if (ctx instanceof AppendReceiver.Pending) {
      receiver.onDurable((AppendReceiver.Pending) ctx);
      return;
    }
    if (ctx instanceof RecoveredPersist) {
      onRecoveredDurable((RecoveredPersist) ctx);
      return;
    }
    if (ctx instanceof SnapshotCut) {
      if (((SnapshotCut) ctx).persistProfile) {
        persistProfileMeta(() -> {}); // R13a: the raised sync number only after the cut
      }
      return;
    }
    if (!(ctx instanceof LeaderPersist)) {
      return;
    }
    LeaderPersist lp = (LeaderPersist) ctx;
    if (lp.truncateOnly) {
      // only the election that is still this node's: a newer term (adopted, or a new campaign)
      // since the win voids it, or the late Durable would make it leader of a term it lost
      if (leadershipPending
          && st.role == Role.CANDIDATE
          && st.currentTerm == leadershipPendingTerm) {
        finishBecomeLeader();
      } else {
        leadershipPending = false;
      }
      return;
    }
    for (LogEntry e : lp.entries) {
      if (holds(e)) { // a copy that replaced it while this write was queued has its own write
        log.markDurable(e.index);
      }
    }
    if (!st.isLeader()) {
      if (applyPolicy != null) {
        applyPolicy.onLogChanged(this); // stepped down meanwhile: what it knows committed may apply
      }
      return; // stepped down while persisting
    }
    if (lp.alreadyRegistered) {
      return;
    }
    faults.at(CrashPoint.INGEST_MID);
    for (Action a : replicator.onEntriesDurable(lp.entries, this)) {
      emit(a);
    }
    advanceCommit();
    checkTransfer();
  }

  // ============================================================ replication

  private void runScheduler() {
    if (!st.isLeader() || replicator == null) {
      return;
    }
    for (Action a : replicator.schedule(this)) {
      emit(a);
    }
  }

  private void onHeartbeatTick() {
    if (!st.isLeader() || replicator == null) {
      return;
    }
    for (int p : peers()) {
      long prev = -1;
      long prevTerm = 0;
      if (gate.prefixCheck()) {
        org.apache.iotdb.consensus.raft.core.replication.FollowerProgress fp =
            replicator.progress(p);
        if (fp != null && !fp.inflight.isEmpty()) {
          continue;
        }
        prev = fp == null ? log.lastIndex() : fp.nextIndex - 1;
        prevTerm = termAtOr0(prev);
      }
      emit(new Action.Send(p, template(prev, prevTerm, false, 0)));
    }
    for (Action a : replicator.onHeartbeat(this)) {
      emit(a);
    }
    if (!st.isLeader()) {
      return; // the replicator resigned (stepDown) while handling the round
    }
    emit(new Action.ArmTimer(Action.TimerKind.HEARTBEAT, cfg.heartbeatIntervalMs));
  }

  private void onAppendEntriesRes(AppendEntriesRes res, int from) {
    if (res.term > st.currentTerm) {
      adoptTerm(res.term, null);
      return;
    }
    if (!st.isLeader() || res.term < st.currentTerm || replicator == null) {
      record(
          ListenerEvent.of("resp_ignored", self)
              .with("resp", res.brief())
              .with("reason", "stale-term")
              .build());
      return;
    }
    if (replicator.progress(from) == null) {
      return; // not a member
    }
    if (res.hasTruncatedFrom()) {
      replicator.onTruncated(from, res.truncatedFrom, this);
    }
    if (res.hasAppliedIndex()) {
      replicator.onAppliedReported(from, res.appliedIndex, this);
    }
    for (Action a : replicator.onResponse(from, res, this)) {
      emit(a);
    }
    if (!st.isLeader() || replicator == null) {
      return; // the replicator resigned (stepDown) on this response
    }
    for (EntryAck ack : res.acks) {
      boolean identityFree =
          ack.status == AckStatus.REJECT || ack.status == AckStatus.NEED_FENCE || ack.isFenceAck();
      if (!identityFree && !(log.has(ack.index) && log.termAt(ack.index) == ack.entryTerm)) {
        record(
            ListenerEvent.of("resp_ignored", self)
                .with("resp", ack.brief())
                .with("reason", "unknown-entry")
                .build());
        continue;
      }
      for (Action a : replicator.onAck(from, ack, res, this)) {
        emit(a);
      }
    }
    advanceCommit();
    checkTransfer();
  }

  // ============================================================ commit / apply

  void advanceCommit() {
    if (!st.isLeader() || replicator == null) {
      return;
    }
    List<Long> idxs =
        commitPolicy.advance(
            self,
            st.currentTerm,
            st.commitIndex,
            log,
            replicator,
            profile.commitDecorator(),
            st.membership,
            mut);
    if (idxs.isEmpty()) {
      return;
    }
    if (caps.outOfOrderCommit) {
      commitOutOfOrder(idxs);
      return;
    }
    for (long i : idxs) {
      commitEntry(i);
    }
    if (applyPolicy != null) {
      applyPolicy.onCommitted(idxs, this);
    } else {
      issueApply(idxs);
    }
  }

  /**
   * Out-of-order commit of {@code idxs} (each at most once): commitIndex stays the contiguous
   * committed prefix; one commit notification per batch carries it with the commit bitmap.
   */
  private void commitOutOfOrder(List<Long> idxs) {
    List<Long> fresh = new ArrayList<>(idxs.size());
    for (long i : idxs) {
      if (!st.isLeader()) {
        break; // a committed CONFIG removed this leader (Raft thesis §4.2.2)
      }
      if (!isCommittedIndex(i)) {
        commitEntry(i);
        fresh.add(i);
      }
    }
    if (fresh.isEmpty()) {
      return;
    }
    if (policy.eagerCommitNotify && st.isLeader()) {
      for (int p : peers()) {
        emit(new Action.Send(p, template(-1, 0, false, 0)));
      }
    }
    if (applyPolicy != null) {
      applyPolicy.onCommitted(fresh, this);
    } else if (st.commitIndex > st.applyIssuedUpTo) {
      // no apply policy: entries are applied in index order, up to the commit frontier
      emit(new Action.Apply(st.applyIssuedUpTo + 1, st.commitIndex));
      st.applyIssuedUpTo = st.commitIndex;
    }
  }

  private void commitEntry(long i) {
    if (caps.outOfOrderCommit) {
      st.committed.add(i);
      while (st.committed.remove(st.commitIndex + 1)) {
        st.commitIndex++;
      }
    } else {
      Invariants.check(
          this,
          i == st.commitIndex + 1 || !mut.isEmpty(),
          "INV-P3",
          "commit of e" + i + " skips e" + (st.commitIndex + 1));
      st.committed.add(i);
      st.commitIndex = Math.max(st.commitIndex, i);
    }
    record(
        ListenerEvent.of("commit", self)
            .with("index", i)
            .with("entry_term", log.termAt(i))
            .with("term", st.currentTerm)
            .with("type", log.typeAt(i).name())
            .build());
    replicator.onCommitted(i, log);
    if (log.typeAt(i) == EntryType.CONFIG) {
      onConfigCommitted(i);
    }
    if (!mut.has("client_ack_on_quorum_ready")) {
      ackClient(i, "commit");
    }
    if (policy.eagerCommitNotify && !caps.outOfOrderCommit) {
      for (int p : peers()) {
        emit(
            new Action.Send(
                p,
                new AppendEntriesReq(
                    st.currentTerm,
                    self,
                    -1,
                    0,
                    Collections.emptyList(),
                    i,
                    log.termAt(i),
                    false,
                    false,
                    ++msgSeq,
                    0)));
      }
    }
  }

  /**
   * Acknowledge the client of {@code index}, if a client write of this tenure is waiting for it:
   * O(1) per index, and at most once (the request leaves the index map). A committed request also
   * leaves {@link RaftState#myReqs}, so the leader keeps state only for writes in flight; a request
   * acknowledged before commitment (the {@code client_ack_on_quorum_ready} mutation) stays there
   * and is still completed with NOT_LEADER if the tenure ends before it commits.
   */
  private void ackClient(long index, String reason) {
    Long req = st.reqByIndex.remove(index);
    if (req == null) {
      return;
    }
    if (isCommittedIndex(index)) {
      st.myReqs.remove(req, index);
    }
    record(
        ListenerEvent.of("client_ack", self)
            .with("req", req)
            .with("index", index)
            .with("entry_term", log.termAt(index))
            .with("reason", reason)
            .build());
    emit(new Action.CompleteClient(req, Action.Outcome.COMMITTED, self));
  }

  private void issueApply(List<Long> idxs) {
    long first = idxs.get(0);
    long last = idxs.get(idxs.size() - 1);
    boolean contiguous = last - first + 1 == idxs.size() && first == st.applyIssuedUpTo + 1;
    if (contiguous) {
      emit(new Action.Apply(first, last));
      st.applyIssuedUpTo = last;
      return;
    }
    for (long i : idxs) {
      if (i > st.applyIssuedUpTo || !st.committed.contains(i)) {
        emit(new Action.Apply(i, i));
        st.applyIssuedUpTo = Math.max(st.applyIssuedUpTo, i);
      }
    }
  }

  /**
   * Follower: learn the leader's commit index and apply what is safe: the committed entries of the
   * durable contiguous prefix. The applier reads them from the store, which holds an entry only
   * once the log writer has written it; an entry still queued there is applied when its Durable
   * arrives (the receiver learns the commit index again then).
   */
  void learnCommit(long leaderCommit) {
    if (leaderCommit > st.knownLeaderCommit) {
      st.knownLeaderCommit = leaderCommit;
    }
    if (applyPolicy != null) {
      return; // the apply policy learns commit information itself (onCommitInfo)
    }
    long limit = Math.min(st.knownLeaderCommit, log.durableContiguousEnd());
    if (limit > st.applyIssuedUpTo) {
      emit(new Action.Apply(st.applyIssuedUpTo + 1, limit));
      st.applyIssuedUpTo = limit;
    }
  }

  private void onApplied(Event.Applied a) {
    if (applyPolicy != null) {
      applyPolicy.onApplied(a.index, this);
      st.lastApplied = Math.max(st.lastApplied, applyPolicy.appliedFrontier());
    } else {
      st.lastApplied = Math.max(st.lastApplied, a.index);
    }
    if (st.isLeader()
        && !st.leaderReady
        && st.leaderFirstIndexOfTerm >= 0
        && st.lastApplied >= st.leaderFirstIndexOfTerm) {
      st.leaderReady = true;
      emit(new Action.LeaderReady());
    }
    if (cfg.snapshotTriggerEntries > 0
        && !snapshotInProgress
        && st.lastApplied - Math.max(log.firstIndex() - 1, snapshotIndex) >= cfg.snapshotTriggerEntries) {
      requestSnapshot(false);
    }
  }

  // ============================================================ snapshots

  private void requestSnapshot(boolean force) {
    if (snapshotInProgress) {
      return;
    }
    long idx = st.lastApplied;
    if (idx < 0 || (!force && (idx < log.firstIndex() || idx <= snapshotIndex))) {
      return;
    }
    snapshotInProgress = true;
    emit(
        new Action.TakeSnapshot(
            idx,
            termAtOr0(idx),
            membershipAt(idx),
            profile.snapshotMeta(log, idx, snapshotExt),
            force));
  }

  /** Runtime snapshot barrier: application is paused while this boundary is captured. */
  public Action.TakeSnapshot snapshotAtApplied(boolean force) {
    long index = st.lastApplied;
    return new Action.TakeSnapshot(
        index,
        termAtOr0(index),
        membershipAt(index),
        profile.snapshotMeta(log, index, snapshotExt),
        force);
  }

  /** Metadata captured while the driver holds the state machine's application fence. */
  public Action.TakeSnapshot snapshotAtAppliedBoundary(long index, boolean force) {
    if (!snapshotInProgress || index < 0 || index != st.lastApplied) {
      throw new IllegalStateException("checkpoint boundary is not the applied frontier: " + index);
    }
    return new Action.TakeSnapshot(index, termAtOr0(index), membershipAt(index),
        profile.snapshotMeta(log, index, snapshotExt), force);
  }

  /** Driver install barrier: all writes queued before the barrier must reach durable callbacks. */
  public boolean hasPendingPersistence() {
    return !pendingPersist.isEmpty() || !pendingMeta.isEmpty();
  }

  private Membership membershipAt(long index) {
    Map.Entry<Long, Membership> e = configEntries.floorEntry(index);
    return e != null ? e.getValue() : baseMembership;
  }

  /** The snapshot was written (or failed: lastIncludedIndex < 0); compact the log behind it. */
  private void onSnapshotDone(Event.SnapshotDone s) {
    snapshotInProgress = false;
    if (s.lastIncludedIndex < 0) {
      return;
    }
    snapshotExt = s.ext == null ? SnapshotMetaExt.NONE : s.ext;
    snapshotIndex = Math.max(snapshotIndex, s.lastIncludedIndex);
    snapshotTerm = s.lastIncludedTerm;
    long p = Math.max(log.firstIndex(), s.lastIncludedIndex + 1 - cfg.preserveEntriesAfterSnapshot);
    if (p <= log.firstIndex()) {
      return;
    }
    long termP = termAtOr0(p - 1);
    Map.Entry<Long, Membership> base = configEntries.floorEntry(p - 1);
    if (base != null) {
      baseMembership = base.getValue();
    }
    configEntries.headMap(p, false).clear();
    log.purgeBefore(p, termP);
    gate.onSnapshot(snapshotExt);
    emit(new Action.CompactLog(p, termP, snapshotExt.encode()));
    if (applyPolicy != null) {
      applyPolicy.onLogChanged(this);
    }
  }

  /**
   * A follower installed the leader's snapshot: the log restarts right after it, unless (under
   * out-of-order commit) the gate keeps the slots beyond it ({@link AcceptGate#keepsTailOnInstall};
   * ParallelRaft: an install that does not raise the sync number keeps the node's accepted copies
   * of its open sync term).
   */
  private void onSnapshotLoaded(Event.SnapshotLoaded s) {
    if (s.coveredByApplied) {
      compactBehindReceived(s);
      return;
    }
    if (caps.outOfOrderCommit && st.isLeader()) {
      // elected while the state machine loaded (the group thread stays free meanwhile): the reset
      // below drops the commit state, and an entry's commit is reported to the policy only once,
      // so this leader's election entry would never commit again and it could never become ready
      stepDown("snapshot_loaded");
    }
    boolean keepTail =
        caps.outOfOrderCommit
            ? gate.keepsTailOnInstall(s.lastIncludedIndex, s.lastIncludedTerm)
            : log.has(s.lastIncludedIndex) && log.termAt(s.lastIncludedIndex) == s.lastIncludedTerm;
    if (!keepTail) {
      log.removeFrom(log.firstIndex());
    }
    log.purgeBefore(s.lastIncludedIndex + 1, s.lastIncludedTerm);
    st.lastApplied = s.lastIncludedIndex;
    st.applyIssuedUpTo = s.lastIncludedIndex;
    st.commitIndex = s.lastIncludedIndex;
    st.knownLeaderCommit = Math.max(st.knownLeaderCommit, s.lastIncludedIndex);
    snapshotIndex = s.lastIncludedIndex;
    snapshotTerm = s.lastIncludedTerm;
    snapshotExt = s.ext == null ? SnapshotMetaExt.NONE : s.ext;
    if (keepTail) {
      configEntries.headMap(s.lastIncludedIndex, true).clear();
    } else {
      configEntries.clear();
    }
    boolean wasVoter = st.membership.isVoter(self);
    baseMembership = s.membership;
    st.membership = s.membership;
    emit(new Action.UpdateMembership(s.membership, true));
    if (!configEntries.isEmpty()) {
      // a kept CONFIG entry beyond the snapshot stays in effect (configuration takes effect on
      // append)
      st.membership = configEntries.lastEntry().getValue();
      emit(new Action.UpdateMembership(st.membership, false));
    }
    if (!wasVoter && st.membership.isVoter(self) && !st.isLeader()) {
      armElectionTimer();
    }
    gate.onSnapshot(snapshotExt);
    emit(new Action.CompactLog(s.lastIncludedIndex + 1, s.lastIncludedTerm, snapshotExt.encode()));
    emit(new Action.SaveCheckpoint(s.lastIncludedIndex));
    record(ListenerEvent.of("snapshot_loaded", self).with("index", s.lastIncludedIndex).build());
    boolean raised = gate.onSnapshotInstalled(s.lastIncludedIndex, s.lastIncludedTerm);
    if (!keepTail) {
      // R13a: the log above was emptied in memory only; the store still holds the old suffix,
      // which a restart would bring back. Cut it durably, and persist a sync number the gate
      // raised only once the cut is durable: with the raised sync number, R7 would take a revived
      // older-term entry for committed.
      persist(new ArrayList<>(), s.lastIncludedIndex + 1, new SnapshotCut(raised));
    } else if (raised) {
      persistProfileMeta(() -> {}); // e.g. ParallelRaft: sync := max(sync, lastIncludedTerm)
    }
    if (applyPolicy != null) {
      applyPolicy.reset(s.lastIncludedIndex, this);
    }
  }

  /**
   * Out-of-order commit: a received snapshot whose every index was already applied here (entries at
   * or below the applied frontier are final) and whose install keeps the tail. The state machine
   * lacks nothing the snapshot holds: it is not rolled back to it, nothing is applied again, and
   * the snapshot only moves the compaction point ({@link Event.SnapshotLoaded#coveredByApplied}).
   * Asked before the state machine would load it. Event thread only.
   */
  public boolean snapshotCoveredByApplied(long lastIncludedIndex, long lastIncludedTerm) {
    return caps.outOfOrderCommit
        && lastIncludedIndex <= st.lastApplied
        && gate.keepsTailOnInstall(lastIncludedIndex, lastIncludedTerm);
  }

  /**
   * The compaction of {@link #onSnapshotDone}, up to a snapshot received from the leader: the log
   * starts right behind it (the leader plans from the follower's first index), everything else the
   * node knows stays, the applied state and its commit state in particular. The summary is the
   * received snapshot's: it describes the slots the log is now compacted behind, which are final.
   */
  private void compactBehindReceived(Event.SnapshotLoaded s) {
    long p = s.lastIncludedIndex + 1;
    snapshotIndex = Math.max(snapshotIndex, s.lastIncludedIndex);
    snapshotTerm = s.lastIncludedTerm;
    snapshotExt = s.ext == null ? SnapshotMetaExt.NONE : s.ext;
    if (p > log.firstIndex()) {
      Map.Entry<Long, Membership> base = configEntries.floorEntry(p - 1);
      if (base != null) {
        baseMembership = base.getValue();
      }
      configEntries.headMap(p, false).clear();
      log.purgeBefore(p, s.lastIncludedTerm);
    }
    gate.onSnapshot(snapshotExt);
    emit(new Action.CompactLog(p, s.lastIncludedTerm, snapshotExt.encode()));
    record(ListenerEvent.of("snapshot_compacted", self).with("index", s.lastIncludedIndex).build());
    if (applyPolicy != null) {
      applyPolicy.onLogChanged(this);
    }
  }

  /** Recovery: the protocol summary stored with the last compaction / installed snapshot. */
  public void restoreSnapshotExt(SnapshotMetaExt ext) {
    snapshotExt = ext == null ? SnapshotMetaExt.NONE : ext;
    snapshotIndex = Math.max(snapshotIndex, log.firstIndex() - 1);
    snapshotTerm = log.snapshotTerm();
    gate.onSnapshot(snapshotExt);
  }

  // ============================================================ membership

  /**
   * Restore the CONFIG entries still present in the recovered log (payloads read by the server).
   */
  public void restoreConfigEntries(Map<Long, Membership> entries) {
    configEntries.putAll(entries);
    if (!configEntries.isEmpty()) {
      st.membership = configEntries.lastEntry().getValue();
    }
  }

  /** A CONFIG entry was stored (leader append or follower accept): it takes effect at once. */
  void applyConfig(long index, Membership m) {
    boolean wasVoter = st.membership.isVoter(self);
    Membership eff = m.withConfigIndex(index);
    configEntries.put(index, eff);
    st.membership = eff;
    if (st.isLeader() && replicator != null) {
      replicator.onMembershipChanged(eff, log);
    }
    emit(new Action.UpdateMembership(eff, false));
    if (!wasVoter && eff.isVoter(self) && !st.isLeader()) {
      armElectionTimer();
    }
    record(
        ListenerEvent.of("config", self)
            .with("index", index)
            .with("membership", eff.toString())
            .build());
  }

  void onConfigEntryStored(LogEntry e) {
    applyConfig(
        e.index, org.apache.iotdb.consensus.raft.core.log.MembershipCodec.decode(e.payload()));
  }

  /**
   * Store {@code e} in its slot, overwriting whatever the slot holds (a recovered or re-dated
   * copy). A CONFIG entry on either side of the replacement updates the configuration: the
   * effective one is that of the last CONFIG entry left in the log (else the base).
   */
  void replaceSlot(LogEntry e) {
    boolean wasConfig = log.has(e.index) && log.typeAt(e.index) == EntryType.CONFIG;
    log.put(e, false);
    if (e.type == EntryType.CONFIG) {
      configEntries.put(e.index, MembershipCodec.decode(e.payload()).withConfigIndex(e.index));
    } else if (wasConfig) {
      configEntries.remove(e.index);
    } else {
      return;
    }
    boolean wasVoter = st.membership.isVoter(self);
    Membership m = configEntries.isEmpty() ? baseMembership : configEntries.lastEntry().getValue();
    st.membership = m;
    if (st.isLeader() && replicator != null) {
      replicator.onMembershipChanged(m, log);
    }
    emit(new Action.UpdateMembership(m, false));
    if (!wasVoter && m.isVoter(self) && !st.isLeader()) {
      armElectionTimer();
    }
    record(
        ListenerEvent.of("config", self)
            .with("index", e.index)
            .with("membership", m.toString())
            .build());
  }

  /** Slots >= from were removed: fall back to the latest surviving configuration. */
  void onLogTruncated(long from) {
    if (configEntries.tailMap(from, true).isEmpty()) {
      return;
    }
    configEntries.tailMap(from, true).clear();
    Membership m = configEntries.isEmpty() ? baseMembership : configEntries.lastEntry().getValue();
    st.membership = m;
    if (st.isLeader() && replicator != null) {
      replicator.onMembershipChanged(m, log);
    }
    emit(new Action.UpdateMembership(m, false));
  }

  private void onConfigCommitted(long index) {
    Membership m = configEntries.get(index);
    if (m == null) {
      return;
    }
    emit(new Action.UpdateMembership(m, true));
    if (pendingConfigIndex == index) {
      pendingConfigIndex = -1;
      if (pendingConfigDone != null) {
        pendingConfigDone.complete(null);
        pendingConfigDone = null;
      }
    }
    if (st.isLeader() && !m.isVoter(self)) {
      // Raft §4.2.2: a leader that removed itself steps down once C_new is committed
      leaveLeadership();
      st.role = Role.FOLLOWER;
      st.leaderId = -1;
      record(
          ListenerEvent.of("stepdown", self)
              .with("term", st.currentTerm)
              .with("reason", "removed")
              .build());
      emit(new Action.RoleChanged(Role.FOLLOWER, -1, st.currentTerm));
    }
  }

  private void onConfigChange(Event.ConfigChangeReq c) {
    if (!st.isLeader() && c.op == Event.ConfigOp.RESET && st.leaderId < 0) {
      // recovery override (IConsensus.resetPeerList with no reachable leader): local only
      Membership m =
          new Membership(
              c.peers.keySet(), Collections.emptySet(), c.peers, st.membership.configIndex());
      configEntries.clear();
      baseMembership = m;
      st.membership = m;
      emit(new Action.UpdateMembership(m, true));
      record(
          ListenerEvent.of("config", self)
              .with("index", -1L)
              .with("membership", m.toString())
              .build());
      if (m.isVoter(self)) {
        armElectionTimer();
      }
      c.done.complete(null);
      return;
    }
    if (!st.isLeader() || leadershipPending) {
      fail(c.done, new IllegalStateException("not leader"));
      return;
    }
    if (st.recovering) {
      fail(c.done, new IllegalStateException("leader recovery in progress"));
      return;
    }
    if (pendingConfigIndex > st.commitIndex) {
      fail(c.done, new IllegalStateException("a configuration change is still in progress"));
      return;
    }
    if (st.leaderFirstIndexOfTerm >= 0 && st.commitIndex < st.leaderFirstIndexOfTerm) {
      fail(
          c.done,
          new IllegalStateException("the leader has not committed an entry of its term yet"));
      return;
    }
    Membership cur = st.membership;
    Membership m;
    switch (c.op) {
      case ADD_LEARNER:
        if (cur.isMember(c.peer)) {
          c.done.complete(null);
          return;
        }
        m = cur.withLearner(c.peer, c.endpoint, -1);
        break;
      case PROMOTE:
        if (cur.isVoter(c.peer)) {
          c.done.complete(null);
          return;
        }
        m = cur.withVoter(c.peer, c.endpoint, -1);
        break;
      case REMOVE:
        if (!cur.isMember(c.peer)) {
          c.done.complete(null);
          return;
        }
        m = cur.without(c.peer, -1);
        break;
      case RESET:
        m = new Membership(c.peers.keySet(), Collections.emptySet(), c.peers, -1);
        break;
      default:
        fail(c.done, new IllegalArgumentException("unknown op " + c.op));
        return;
    }
    if (caps.outOfOrderCommit && st.commitIndex < log.lastIndex()) {
      // membership barrier: a CONFIG entry only follows a fully committed log
      fail(c.done, new IllegalStateException("uncommitted entries, retry"));
      return;
    }
    long index = log.lastIndex() + 1;
    m = m.withConfigIndex(index);
    LogEntry e =
        profile
            .entryFactory()
            .create(
                index,
                st.currentTerm,
                EntryType.CONFIG,
                RequestMeta.OPAQUE,
                ingestState,
                org.apache.iotdb.consensus.raft.core.log.MembershipCodec.encode(m),
                LogEntry.NO_REQUEST,
                null);
    log.put(e, false);
    applyConfig(index, m);
    pendingConfigIndex = index;
    pendingConfigDone = c.done;
    List<LogEntry> one = Collections.singletonList(e);
    persist(one, -1, new LeaderPersist(one, false, false));
  }

  private void onTransfer(Event.TransferLeadershipReq t) {
    if (t.target < 0) { // cancel (facade timeout)
      transferTarget = -1;
      if (transferDone != null) {
        transferDone.completeExceptionally(
            new IllegalStateException("leadership transfer timed out"));
        transferDone = null;
      }
      return;
    }
    if (!st.isLeader() || replicator == null) {
      fail(t.done, new IllegalStateException("not leader"));
      return;
    }
    if (st.recovering) {
      fail(t.done, new IllegalStateException("leader recovery in progress"));
      return;
    }
    if (!st.membership.isVoter(t.target)) {
      fail(t.done, new IllegalStateException("N" + t.target + " is not a voter"));
      return;
    }
    if (t.target == self) {
      t.done.complete(null);
      return;
    }
    transferTarget = t.target;
    transferDone = t.done;
    checkTransfer();
  }

  /** Once the transfer target holds the whole log, tell it to campaign immediately. */
  private void checkTransfer() {
    if (transferTarget < 0 || !st.isLeader() || replicator == null) {
      return;
    }
    org.apache.iotdb.consensus.raft.core.replication.FollowerProgress fp =
        replicator.progress(transferTarget);
    if (fp != null && fp.matchIndex >= log.lastIndex()) {
      emit(new Action.Send(transferTarget, new TimeoutNowReq(st.currentTerm, self)));
      record(ListenerEvent.of("transfer", self).with("target", transferTarget).build());
      if (transferDone != null) {
        transferDone.complete(null);
      }
      transferDone = null;
      transferTarget = -1;
    }
  }

  private static void fail(java.util.concurrent.CompletableFuture<Void> f, Exception e) {
    if (f != null) {
      f.completeExceptionally(e);
    }
  }

  /**
   * Lease-based step-down: a leader that heard from no majority for an election timeout resigns.
   */
  private void onLeaseCheck(Event.LeaseCheck c) {
    if (!st.isLeader()) {
      return;
    }
    if (c.quorumContactNanos >= 0
        && c.nowNanos - c.quorumContactNanos > cfg.electionTimeoutMaxMs * 1_000_000L) {
      leaveLeadership();
      st.role = Role.FOLLOWER;
      st.leaderId = -1;
      record(
          ListenerEvent.of("stepdown", self)
              .with("term", st.currentTerm)
              .with("reason", "lease_expired")
              .build());
      emit(new Action.RoleChanged(Role.FOLLOWER, -1, st.currentTerm));
      armElectionTimer();
      return;
    }
    if (cfg.leaderLeaseCheckMs > 0) {
      emit(new Action.ArmTimer(Action.TimerKind.LEASE, cfg.leaderLeaseCheckMs));
    }
  }

  private void onAdmin(Event ev) {
    if (ev instanceof Event.ConfigChangeReq) {
      onConfigChange((Event.ConfigChangeReq) ev);
    } else if (ev instanceof Event.TransferLeadershipReq) {
      onTransfer((Event.TransferLeadershipReq) ev);
    }
  }

  // ============================================================ SchedulerContext

  @Override
  public int self() {
    return self;
  }

  @Override
  public long currentTerm() {
    return st.currentTerm;
  }

  @Override
  public long commitIndex() {
    return st.commitIndex;
  }

  @Override
  public long commitTerm() {
    return termAtOr0(st.commitIndex);
  }

  private long termAtOr0(long index) {
    if (index < log.firstIndex()) {
      return index == log.firstIndex() - 1 ? log.snapshotTerm() : 0;
    }
    return log.has(index) ? log.termAt(index) : 0;
  }

  @Override
  public LogView log() {
    return log;
  }

  @Override
  public Membership membership() {
    return st.membership;
  }

  @Override
  public Set<Integer> peers() {
    return st.membership.peersOf(self);
  }

  @Override
  public ProtocolPolicy policy() {
    return policy;
  }

  @Override
  public Mutations mutations() {
    return mut;
  }

  @Override
  public ProtocolListener listener() {
    return listener;
  }

  @Override
  public FaultInjector faults() {
    return faults;
  }

  @Override
  public LeaderIngestState ingestState() {
    return ingestState;
  }

  @Override
  public long nowNanos() {
    return clock.getAsLong();
  }

  @Override
  public long nextMsgId() {
    return ++msgSeq;
  }

  @Override
  public long nextSchedRound() {
    return ++schedRound;
  }

  @Override
  public void onQuorumReady(long index) {
    record(ListenerEvent.of("quorum_ready", self).with("index", index).build());
    if (mut.has("client_ack_on_quorum_ready")) {
      // BUG (E-4): the client is acked at quorum-ready instead of at commit
      ackClient(index, "quorum_ready");
    }
  }

  @Override
  public AppendEntriesReq template(long prevLogIndex, long prevLogTerm, boolean fence, long round) {
    AppendEntriesReq req =
        new AppendEntriesReq(
            st.currentTerm,
            self,
            prevLogIndex,
            prevLogTerm,
            Collections.emptyList(),
            st.commitIndex,
            commitTerm(),
            false,
            fence,
            ++msgSeq,
            round);
    if (caps.outOfOrderCommit) {
      // the commit frontier plus a bitmap of the committed indexes beyond it
      req =
          req.withExt(
              AppendExt.commit(
                  st.commitIndex,
                  AppendExt.bitsOf(st.commitIndex, st.committed, caps.commitBitmapBits)));
    }
    return req;
  }

  // ------------------------------------------------------- leader recovery (SchedulerContext)

  @Override
  public boolean recovering() {
    return st.recovering;
  }

  @Override
  public void persistRecovered(List<LogEntry> entries, long truncateFrom, Runnable onDurable) {
    long cut = -1;
    if (truncateFrom >= 0 && truncateFrom <= log.lastIndex()) {
      Invariants.check(
          this,
          !appliedAtOrAfter(truncateFrom),
          "INV-S1",
          "recovery truncation from " + truncateFrom + " cuts an applied entry");
      List<Long> dropped = log.removeFrom(truncateFrom);
      onLogTruncated(truncateFrom);
      gate.refresh();
      cut = truncateFrom;
      record(
          ListenerEvent.of("recovered_truncate", self)
              .with("from", truncateFrom)
              .with("dropped", dropped)
              .build());
    }
    List<LogEntry> stored = new ArrayList<>(entries);
    for (LogEntry e : stored) {
      replaceSlot(e);
    }
    persist(stored, cut, new RecoveredPersist(stored, scoped(onDurable)));
  }

  private void onRecoveredDurable(RecoveredPersist rp) {
    for (LogEntry e : rp.entries) {
      if (holds(e)) { // a later replacement of the slot is marked by its own Persist
        log.markDurable(e.index);
      }
    }
    if (rp.onDurable != null) {
      rp.onDurable.run();
    }
    if (applyPolicy != null) {
      applyPolicy.onLogChanged(this);
    }
  }

  /** True if the slot of {@code e} still holds that very copy (same term, same metadata). */
  boolean holds(LogEntry e) {
    return log.has(e.index) && log.termAt(e.index) == e.term && log.metaAt(e.index) == e.meta;
  }

  @Override
  public void persistProfileMeta(Runnable onDurable) {
    persistMeta(new MetaCtx(MetaKind.PROFILE, st.currentTerm, -1, null, null, scoped(onDurable)));
  }

  @Override
  public void loadEntries(long[] indexes, Consumer<List<LogEntry>> callback) {
    long token = ++tokenSeq;
    CallbackScope scope = new CallbackScope();
    pendingLoads.put(
        token,
        entries -> {
          if (scope.current()) {
            callback.accept(entries);
          }
        });
    emit(new Action.LoadEntries(indexes.clone(), token));
  }

  @Override
  public void recoveryComplete() {
    if (!st.isLeader() || !st.recovering) {
      return;
    }
    st.recovering = false;
    ingestState = profile.entryFactory().rebuild(log, snapshotExt, this);
    appendElectionEntry();
    advanceCommit();
  }

  @Override
  public void stepDown(String reason) {
    if (!st.isLeader()) {
      return;
    }
    leaveLeadership();
    st.role = Role.FOLLOWER;
    st.leaderId = -1;
    record(
        ListenerEvent.of("stepdown", self)
            .with("term", st.currentTerm)
            .with("reason", reason)
            .build());
    emit(new Action.RoleChanged(Role.FOLLOWER, -1, st.currentTerm));
    armElectionTimer();
  }

  /** {@code r} restricted to the current term / leadership (null stays null). */
  private Runnable scoped(Runnable r) {
    if (r == null) {
      return null;
    }
    CallbackScope scope = new CallbackScope();
    return () -> {
      if (scope.current()) {
        r.run();
      }
    };
  }

  /** True if an entry at or beyond {@code index} was applied or its Apply already issued. */
  boolean appliedAtOrAfter(long index) {
    if (applyPolicy != null) {
      return applyPolicy.appliedAtOrAfter(index);
    }
    return Math.max(st.lastApplied, st.applyIssuedUpTo) >= index;
  }

  // ------------------------------------------------------------------ ApplyContext

  @Override
  public void emitApply(long fromInclusive, long toInclusive) {
    emit(new Action.Apply(fromInclusive, toInclusive));
  }

  // --------------------------------------------------------------- ProtocolContext

  @Override
  public void reply(ReplyHandle h, Message res) {
    emit(new Action.Reply(h, res));
  }

  // ============================================================ read-only views

  public Role role() {
    return st.role;
  }

  public long currentTermView() {
    return st.currentTerm;
  }

  public int leaderId() {
    return st.leaderId;
  }

  public int votedFor() {
    return st.votedFor;
  }

  public long commitIndexView() {
    return st.commitIndex;
  }

  public long lastApplied() {
    return st.lastApplied;
  }

  public long knownLeaderCommit() {
    return st.knownLeaderCommit;
  }

  /**
   * Follower: min(knownLeaderCommit, contiguousEnd); leader: commitIndex (PY commit_index_view).
   * Out-of-order commit: the same as of the end of the last event, readable from any thread (a
   * follower with an apply policy reports the policy's commit frontier).
   */
  public long commitIndexViewForRole() {
    if (caps.outOfOrderCommit) {
      return publishedCommitView;
    }
    return st.isLeader() ? st.commitIndex : Math.min(st.knownLeaderCommit, log.contiguousEnd());
  }

  /**
   * The index a linearizable read waits to see applied. Out-of-order commit: the largest committed
   * index (the leader's {@code max(commitIndex, last of committed)}, a follower's {@code
   * maxCommitted()}) as of the end of the last event, readable from any thread; otherwise {@link
   * #commitIndexViewForRole()}.
   */
  public long readIndexView() {
    return caps.outOfOrderCommit ? publishedReadView : commitIndexViewForRole();
  }

  /**
   * Out-of-order commit: compute the commit views on the event thread (at the end of every event
   * and once at construction) for readers on other threads.
   */
  private void publishViews() {
    if (st.isLeader()) {
      publishedCommitView = st.commitIndex;
      publishedReadView =
          st.committed.isEmpty() ? st.commitIndex : Math.max(st.commitIndex, st.committed.last());
    } else if (applyPolicy != null) {
      publishedCommitView = applyPolicy.commitFrontier();
      publishedReadView = applyPolicy.maxCommitted();
    } else {
      publishedCommitView = Math.min(st.knownLeaderCommit, log.contiguousEnd());
      publishedReadView = publishedCommitView;
    }
  }

  /**
   * Leader: {@code index} was committed (out-of-order: within the frontier or beyond it). Event
   * thread only.
   */
  public boolean isCommittedIndex(long index) {
    if (caps.outOfOrderCommit) {
      return index <= st.commitIndex || st.committed.contains(index);
    }
    return st.committed.contains(index);
  }

  public LogView logView() {
    return log;
  }

  /** The profile's apply policy (null: built-in prefix application). Event thread only. */
  public ApplyPolicy applyPolicyView() {
    return applyPolicy;
  }

  public Replicator replicator() {
    return replicator;
  }

  public Membership membershipView() {
    return st.membership;
  }

  public boolean leaderReady() {
    return st.isLeader() && st.leaderReady;
  }

  public java.util.NavigableSet<Long> committedIndexes() {
    return Collections.unmodifiableNavigableSet(st.committed);
  }

  /** Leader: request id -> index of the client writes of this tenure not acknowledged yet. */
  public Map<Long, Long> myRequests() {
    return Collections.unmodifiableMap(st.myReqs);
  }

  /** Leader: the number of index -> request entries kept for client acknowledgements. */
  public int pendingClientAcks() {
    return st.reqByIndex.size();
  }

  public AcceptGate gate() {
    return gate;
  }

  public LeaderIngestState ingestStateView() {
    return ingestState;
  }

  public long fencedTerm() {
    return st.fencedTerm;
  }

  public long snapshotIndex() {
    return snapshotIndex;
  }

  /**
   * Whether this node may install a leader's snapshot ending at {@code lastIncludedIndex} (asked
   * before the state machine loads it). Never one the node's own compaction already passed: a
   * leader's per-follower view lags behind the follower (an install waits for the answers in
   * flight, an answer may be lost), and installing the older snapshot would leave the state machine
   * at it while the log keeps starting after the node's own, newer snapshot, so the entries in
   * between would be in neither. In-order commit (Raft) also refuses a snapshot below the applied
   * state (the state machine would regress). Out-of-order commit (ParallelRaft, R18b) takes it: the
   * leader installs when it must confirm a slot it compacted while the follower may hold and have
   * applied far beyond it, and refusing would make the leader plan the same install again for good.
   * The state machine is not rolled back for it: such an install only moves the compaction point
   * ({@link #snapshotCoveredByApplied}). Event thread only.
   */
  public boolean acceptsSnapshot(long lastIncludedIndex) {
    long floor = log.firstIndex() - 1;
    if (!caps.outOfOrderCommit) {
      floor = Math.max(floor, st.lastApplied);
    }
    return lastIncludedIndex >= floor;
  }

  public long snapshotTerm() {
    return snapshotTerm;
  }

  public boolean snapshotInProgress() {
    return snapshotInProgress;
  }

  @Override
  public SnapshotMetaExt snapshotExt() {
    return snapshotExt;
  }

  public Map<Long, Membership> configEntries() {
    return Collections.unmodifiableMap(configEntries);
  }

  public int transferTarget() {
    return transferTarget;
  }

  public ProtocolProfile profile() {
    return profile;
  }

  public VoteKey voteKey() {
    return myVoteKey();
  }
}
