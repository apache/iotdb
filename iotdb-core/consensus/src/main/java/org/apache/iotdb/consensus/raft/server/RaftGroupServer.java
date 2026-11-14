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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.raft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.core.FaultInjector;
import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.RaftCore;
import org.apache.iotdb.consensus.raft.core.RaftCoreConfig;
import org.apache.iotdb.consensus.raft.core.RandomSource;
import org.apache.iotdb.consensus.raft.core.Role;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.apply.ApplyPolicy;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.log.MembershipCodec;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.policy.ProtocolPolicy;
import org.apache.iotdb.consensus.raft.core.profile.Capabilities;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.replication.FollowerProgress;
import org.apache.iotdb.consensus.raft.core.replication.LeaderStateView;
import org.apache.iotdb.consensus.raft.snapshot.SnapshotManager;
import org.apache.iotdb.consensus.raft.snapshot.SnapshotMeta;
import org.apache.iotdb.consensus.raft.storage.AppliedCheckpoint;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.raft.storage.FileMetaStore;
import org.apache.iotdb.consensus.raft.storage.LogWriter;
import org.apache.iotdb.consensus.raft.storage.MembershipStore;
import org.apache.iotdb.consensus.raft.storage.PersistentMeta;
import org.apache.iotdb.consensus.raft.storage.SegmentedLogStore;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusReq;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaReq;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.raft.transport.ThriftTransport;
import org.apache.iotdb.consensus.raft.transport.Transport;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One consensus group on this node: the kernel plus everything that drives it (serial executor, log
 * writer, transport, timers, applier, snapshots). All kernel access goes through the group
 * executor.
 */
public class RaftGroupServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftGroupServer.class);

  /** Everything a group needs from the node. */
  public static final class NodeContext {
    public final int self;
    public final TEndPoint selfEndPoint;
    public final RaftConfig config;
    public final ProtocolPolicy policy;
    public final IClientManager<TEndPoint, AsyncRaftServiceClient> asyncClients;
    public final IClientManager<TEndPoint, SyncRaftServiceClient> syncClients;
    public final ExecutorService corePool;
    public final ExecutorService applyPool;
    public final ExecutorService snapshotPool;
    public final ScheduledExecutorService timerPool;
    public final ProtocolListener listener;

    public NodeContext(
        int self,
        TEndPoint selfEndPoint,
        RaftConfig config,
        ProtocolPolicy policy,
        IClientManager<TEndPoint, AsyncRaftServiceClient> asyncClients,
        IClientManager<TEndPoint, SyncRaftServiceClient> syncClients,
        ExecutorService corePool,
        ExecutorService applyPool,
        ExecutorService snapshotPool,
        ScheduledExecutorService timerPool,
        ProtocolListener listener) {
      this.self = self;
      this.selfEndPoint = selfEndPoint;
      this.config = config;
      this.policy = policy;
      this.asyncClients = asyncClients;
      this.syncClients = syncClients;
      this.corePool = corePool;
      this.applyPool = applyPool;
      this.snapshotPool = snapshotPool;
      this.timerPool = timerPool;
      this.listener = listener;
    }
  }

  protected final ConsensusGroupId gid;
  private final File dir;
  protected final NodeContext node;
  protected final IStateMachine stateMachine;
  protected final ProtocolProfile profile;
  protected final RequestPreprocessor preprocessor;
  protected final GroupExecutor executor;
  private org.apache.iotdb.consensus.raft.transport.LogFileTransfer logFiles;
  private final org.apache.iotdb.consensus.common.research.ResearchReplicationLink replicationLink;
  protected final GroupExecutor applyExecutor;

  /**
   * Prefix of the research counters this group feeds: the profile's name in lower case ({@code
   * raft}, {@code parallelraft}), so that the protocols never share a counter.
   */
  protected final String metricsPrefix;

  protected final ClientRequestTable requests;
  private final FileMetaStore metaStore;
  private final MembershipStore membershipStore;
  private final AppliedCheckpoint checkpoint;
  protected final SegmentedLogStore log;
  private LogWriter writer;
  private Transport transport;
  private RaftTimer timer;
  protected RaftApplier applier;
  private ActionDispatcher dispatcher;
  private SnapshotManager snapshots;
  protected RaftCore core;
  private RaftGroupMetrics metrics;
  protected final ResearchMetrics.AdmissionTracker admissionMetrics;

  /**
   * Last time each peer answered this node as the leader of its current term (leader lease / read
   * index): answers to AppendEntries, RecoverTerm and UpdateSync. Cleared when a new leadership
   * starts, so that answers to an earlier one never count for it.
   */
  private final java.util.concurrent.ConcurrentHashMap<Integer, Long> contacts =
      new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * When the current leadership began: the lease check counts a voter that has not answered it yet
   * as last heard then (never as a lease for reads), so an isolated new leader steps down.
   */
  private volatile long leadershipStartNanos;

  /** Kernel views published to non-core threads after every event. */
  private volatile long appliedView = -1;

  private volatile long commitView = -1;
  private volatile Membership initialMembership;
  private volatile boolean started;
  private volatile boolean leaderReady;

  /** Read rounds are correlated with the request message id, never with response arrival time. */
  private final Map<Long, ReadRound> readRounds = new TreeMap<>();

  private static final class ReadRound {
    final long term;
    final long index;
    final Membership membership;
    final java.util.Set<Integer> acknowledgers = new java.util.HashSet<>();
    final CompletableFuture<Long> result;

    ReadRound(long term, long index, Membership membership, CompletableFuture<Long> result) {
      this.term = term;
      this.index = index;
      this.membership = membership;
      this.result = result;
    }
  }

  /**
   * The profile commits out of order (ParallelRaft): reads use the largest committed index, the
   * status carries the protocol's fields, and snapshot installs exclude local snapshots.
   */
  protected final boolean outOfOrder;

  /**
   * The install of a received snapshot that passed the kernel's check and is being loaded (null if
   * none), completed when the load is over. Until then local snapshots are refused (they could
   * compact the log past it in between), and so are other transfers, before they move a fragment.
   * Written on the group thread.
   */
  protected volatile CompletableFuture<Void> pendingInstall;

  private final java.util.ArrayDeque<Event> deferredDuringInstall = new java.util.ArrayDeque<>();

  /** The last status computed on the group thread (the answer when it cannot compute a new one). */
  private volatile TGroupStatusRes lastStatus;

  /** Set once by {@link #stop()}: the group thread takes no more tasks. */
  protected volatile boolean closed;

  /**
   * Why the group stopped on its own (null if it did not): the applier could not read a committed
   * entry, so the state machine cannot go on without a hole.
   */
  protected volatile Throwable failure;

  public RaftGroupServer(
      ConsensusGroupId gid,
      File dir,
      NodeContext node,
      IStateMachine stateMachine,
      ProtocolProfile profile,
      RequestPreprocessor preprocessor,
      Membership initialMembership)
      throws IOException {
    this.gid = gid;
    this.dir = dir;
    this.node = node;
    this.stateMachine = stateMachine;
    this.profile = profile;
    this.preprocessor = preprocessor;
    this.initialMembership = initialMembership;
    Capabilities caps = profile.capabilities();
    this.outOfOrder = caps != null && caps.outOfOrderCommit;
    this.metricsPrefix = profile.name().toLowerCase(Locale.ROOT);
    this.admissionMetrics = new ResearchMetrics.AdmissionTracker(metricsPrefix);
    this.requests = new ClientRequestTable(metricsPrefix);
    replicationLink =
        new org.apache.iotdb.consensus.common.research.ResearchReplicationLink(
            node.config.rpc.replicationBytesPerSecond);

    AtomicFiles.createDirectories(dir);
    this.executor = new GroupExecutor(gid + "-core", node.corePool);
    this.applyExecutor = new GroupExecutor(gid + "-apply", node.applyPool);
    this.metaStore = new FileMetaStore(dir);
    this.membershipStore = new MembershipStore(dir);
    this.checkpoint = new AppliedCheckpoint(dir);
    this.log =
        new SegmentedLogStore(
            new File(dir, "log"),
            profile.metaCodec(),
            node.config.log.segmentBytes,
            node.config.log.cacheBytes);
  }

  // ------------------------------------------------------------------ lifecycle

  /** Recovery: meta, snapshot manifest, membership, log replay, checkpoint, kernel, drivers. */
  public synchronized void recover() throws IOException {
    PersistentMeta meta = metaStore.load();
    metrics = new RaftGroupMetrics(profile.name(), gid.toString(), this::status, outOfOrder);
    snapshots =
        new SnapshotManager(
            gid.convertToTConsensusGroupId(),
            dir,
            stateMachine,
            profile,
            node.config.snapshot.retentionNum,
            node.snapshotPool,
            this::onEvent,
            node.syncClients,
            id -> core == null ? null : core.membershipView().endpointOf(id),
            node.self,
            () -> core == null ? 0 : core.currentTermView());
    SnapshotMeta snap = snapshots.latest();
    MembershipStore.Stored stored = membershipStore.load();
    Membership base;
    if (snap != null) {
      base = snap.membership;
    } else if (stored != null) {
      base = stored.membership;
    } else {
      base = initialMembership == null ? Membership.empty() : initialMembership;
      membershipStore.save(base, true);
    }
    LogIndex index = log.recover();
    if (snapshots.recoveredInstallIndex() >= 0) {
      log.deleteFrom(snapshots.recoveredInstallIndex() + 1);
      log.sync();
      index = log.recover();
    }
    prepareStateMachineReplay(snap == null, index, meta.term);
    if (snap != null && index.firstIndex() <= snap.index) {
      // the snapshot is newer than the compacted log prefix: the log is logically cut there
      log.meta().save(snap.index + 1, snap.term, snap.ext);
      log.purgeBefore(snap.index + 1);
      index = log.recover();
    }
    // The DataRegion is rebuilt from the consensus snapshot and log on every restart. An
    // applied checkpoint is not a state-machine snapshot and must never skip replay.
    if (snap == null && index.firstIndex() > 0) {
      throw new IOException("compacted Raft log has no state-machine snapshot: " + dir);
    }
    if (snap != null) {
      try {
        stateMachine.loadSnapshot(snap.dir);
      } catch (RuntimeException e) {
        throw new IOException("cannot restore Raft state-machine snapshot " + snap.dir, e);
      }
    }
    long lastApplied = snap == null ? -1 : snap.index;
    snapshots.completeInstallRecovery();
    boolean restarted = index.lastIndex() >= 0 || meta.term > 0;
    profile.restoreProfileMeta(meta.profileMeta); // the kernel's strategies see it from the start
    core =
        new RaftCore(
            node.self,
            base,
            index,
            meta.term,
            meta.votedFor,
            lastApplied,
            restarted,
            profile,
            node.policy,
            new RaftCoreConfig(
                node.config.election.electionTimeoutMinMs,
                node.config.election.electionTimeoutMaxMs,
                node.config.election.heartbeatIntervalMs,
                node.config.read.leaseCheckIntervalMs,
                node.config.snapshot.triggerEntries,
                node.config.snapshot.preserveEntriesAfterSnapshot),
            ev -> {
              node.listener.onEvent(ev);
              metrics.onEvent(ev);
            },
            FaultInjector.NONE,
            RandomSource.seeded(new Random().nextLong()),
            Mutations.NONE,
            System::nanoTime);
    appliedView = lastApplied;
    commitView = lastApplied;
    Map<Long, Membership> configs = new TreeMap<>();
    for (long i : index.slots()) {
      if (index.typeAt(i) == EntryType.CONFIG) {
        LogEntry e = log.read(i);
        if (e != null) {
          configs.put(i, MembershipCodec.decode(e.payload()).withConfigIndex(i));
        }
      }
    }
    if (!configs.isEmpty()) {
      core.restoreConfigEntries(configs);
    }
    byte[] summary = log.meta().profileSummary();
    if (snap != null && index.firstIndex() == snap.index + 1 && snap.ext != null) {
      summary = snap.ext;
    }
    if (summary != null && summary.length > 0) {
      core.restoreSnapshotExt(profile.decodeSnapshotMeta(summary));
    }
    if (stored != null && configs.isEmpty() && snap == null) {
      // nothing in the log: the stored configuration is the effective one
    }
    writer =
        new LogWriter(
            gid + "-log-writer",
            log,
            metaStore,
            this::onEvent,
            node.config.log.fsyncMaxBatchEntries,
            node.config.log.fsyncMaxDelayMs);
    logFiles = createLogFileTransfer();
    transport =
        new ThriftTransport(
            gid.convertToTConsensusGroupId(),
            node.asyncClients,
            id -> core.membershipView().endpointOf(id),
            this::onEvent,
            profile.metaCodec());
    ((ThriftTransport) transport).setLogFiles(logFiles);
    timer = new RaftTimer(node.timerPool, this::onEvent);
    applier = createApplier();
    applier.setBatch(node.config.applyBatch);
    applier.installedSnapshot(lastApplied);
    startWriteExtension();
    dispatcher =
        new ActionDispatcher(
            gid.toString(),
            writer,
            log,
            transport,
            timer,
            applier,
            requests,
            membershipStore,
            checkpoint,
            this::onEvent,
            new ActionDispatcher.Hooks() {
              @Override
              public void onRoleChanged(Action.RoleChanged a) {
                leaderReady = false;
                try {
                  if (a.role == Role.LEADER) {
                    stateMachine.event().notifyLeaderChanged(gid, node.self);
                  } else {
                    stateMachine.event().notifyNotLeader();
                    if (a.leaderId >= 0) {
                      stateMachine.event().notifyLeaderChanged(gid, a.leaderId);
                    }
                  }
                } catch (Exception e) {
                  LOGGER.warn("{}: state machine event failed", gid, e);
                }
              }

              @Override
              public void onLeaderReady() {
                leaderReady = true;
                try {
                  stateMachine.event().notifyLeaderReady();
                } catch (Exception e) {
                  LOGGER.warn("{}: notifyLeaderReady failed", gid, e);
                }
              }

              @Override
              public void onTakeSnapshot(Action.TakeSnapshot a) {
                if (pendingInstall != null) {
                  // a received snapshot is being loaded: report this one as failed, the kernel
                  // triggers the next one later
                  LOGGER.info(
                      "{}: local snapshot at {} skipped during a snapshot install",
                      gid,
                      a.lastApplied);
                  onEvent(new Event.SnapshotDone(-1, 0, null));
                  return;
                }
                // Serialize the state-machine image with application. The originally requested
                // boundary may lag already queued apply work, so capture it again after draining.
                applyExecutor.execute(
                    () -> {
                      CompletableFuture<Action.TakeSnapshot> boundary = new CompletableFuture<>();
                      executor.execute(() -> boundary.complete(core.snapshotAtApplied(a.force)));
                      try {
                        snapshots.takeSynchronous(
                            boundary.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS));
                      } catch (Exception e) {
                        LOGGER.error("{}: cannot establish snapshot boundary", gid, e);
                        onEvent(new Event.SnapshotDone(-1, 0, null));
                      }
                    });
              }

              @Override
              public void onInstallSnapshotTo(Action.InstallSnapshotTo a) {
                snapshots.installTo(a.follower);
              }
            });
    LOGGER.info(
        "{}: recovered term={} vote={} log[{}..{}] contiguousEnd={} lastApplied={} snapshot={} membership={}",
        gid,
        meta.term,
        meta.votedFor,
        index.firstIndex(),
        index.lastIndex(),
        index.contiguousEnd(),
        lastApplied,
        snap,
        core.membershipView());
    lastStatus = computeStatus(); // no event has run yet: safe on this thread
  }

  public synchronized void start() {
    if (started) {
      return;
    }
    started = true;
    writer.start();
    stateMachine.start();
    try {
      MetricService.getInstance().addMetricSet(metrics);
    } catch (RuntimeException e) {
      LOGGER.warn("{}: metrics not registered: {}", gid, e.toString());
    }
    if (core.membershipView().isVoter(node.self)) {
      long min = node.config.election.firstElectionTimeoutMinMs;
      long span = Math.max(1, node.config.election.firstElectionTimeoutMaxMs - min);
      timer.arm(
          Action.TimerKind.ELECTION,
          min + new Random().nextInt((int) Math.min(Integer.MAX_VALUE, span)));
    }
  }

  public synchronized void stop() {
    if (!started) {
      return;
    }
    started = false;
    closed = true;
    try {
      MetricService.getInstance().removeMetricSet(metrics);
    } catch (RuntimeException e) {
      LOGGER.debug("{}: metrics not removed: {}", gid, e.toString());
    }
    timer.close();
    stopWriteExtension();
    executor.close();
    requests.failAll(new NotLeaderException(-1));
    writer.close();
    applyExecutor.close();
    try {
      checkpoint.save(core.lastApplied());
    } catch (IOException e) {
      LOGGER.warn("{}: cannot write applied checkpoint", gid, e);
    }
    try {
      log.close();
    } catch (IOException e) {
      LOGGER.warn("{}: cannot close log", gid, e);
    }
    stateMachine.stop();
  }

  /**
   * The applier stopped at a committed entry it cannot read, or a snapshot load failed: the group
   * stops (it neither leads nor votes any more; the other members go on), and its writes fail at
   * once. Stopped on another thread: the applier calls this from the apply executor, which stop()
   * closes.
   */
  protected void onApplierFailure(Throwable cause) {
    failure = cause;
    LOGGER.error("{}: committed entries can no longer be applied; stopping the group", gid, cause);
    Thread t = new Thread(this::stop, gid + "-stop-on-failure");
    t.setDaemon(true);
    t.start();
  }

  /** Why the group stopped on its own (an entry it could not apply, a failed load), or null. */
  public Throwable failure() {
    return failure;
  }

  // ------------------------------------------------------------------ driving

  /**
   * Every driver (transport, writer, timer, applier, snapshots) hands events here. An inbound
   * message (a peer's request or a response to our RPC) first passes the {@link InboundFaults} test
   * / benchmark hook: one volatile read; with the default {@link InboundFaults#NONE} nothing else
   * happens. A dropped message is discarded; a delayed one is re-submitted after its delay through
   * the node's timer pool, so that later messages overtake it.
   */
  public void onEvent(Event ev) {
    if (ev instanceof Event.Inbound) {
      InboundFaults faults = InboundFaults.current();
      if (faults != InboundFaults.NONE) {
        Event.Inbound in = (Event.Inbound) ev;
        if (faults.drop(node.self, in.from, in.msg)) {
          return;
        }
        long delay = faults.delayMillis(node.self, in.from, in.msg);
        if (delay > 0) {
          try {
            node.timerPool.schedule(
                () -> executor.execute(() -> handle(in)), delay, TimeUnit.MILLISECONDS);
          } catch (RuntimeException e) {
            LOGGER.debug(
                "{}: delayed inbound message dropped (node stopping): {}", gid, e.toString());
          }
          return;
        }
      }
    }
    executor.execute(() -> handle(ev));
  }

  protected void handle(Event ev) {
    if (profile.usesFileCheckpoints()
        && pendingInstall != null
        && !(ev instanceof Event.Applied)
        && !(ev instanceof Event.Durable)
        && !(ev instanceof Event.MetaDurable)
        && !(ev instanceof Event.SnapshotLoaded)
        && !(ev instanceof Event.SnapshotDone)) {
      deferredDuringInstall.addLast(ev);
      return;
    }
    long now = System.nanoTime();
    if (ev instanceof Event.Inbound) {
      Event.Inbound in = (Event.Inbound) ev;
      if (isAnswer(in.msg) && in.msg.term == core.currentTermView() && core.role() == Role.LEADER) {
        contacts.put(in.from, now); // an answer in my term: the peer still recognises this leader
      } else if (outOfOrder
          && in.msg instanceof AppendEntriesReq
          && ((AppendEntriesReq) in.msg).isPush()) {
        metrics.increment(RaftGroupMetrics.PUSH); // a recovery / catch-up push reached this node
      }
    } else if (ev instanceof Event.LeaseCheck) {
      ev = new Event.LeaseCheck(now, quorumContactNanos(now, leadershipStartNanos));
    }
    boolean wasLeader = core.role() == Role.LEADER;
    long termBefore = core.currentTermView();
    long lastBefore = core.logView().lastIndex();
    List<Action> actions = core.onEvent(ev);
    if (ev instanceof Event.ClientWrite && core.logView().lastIndex() > lastBefore) {
      Event.ClientWrite write = (Event.ClientWrite) ev;
      requests.onAdmitted(write.reqId, now);
      if (!isAggregateRequest(write.liveRequest)) {
        admissionMetrics.accepted(write.meta.barrier, write.meta.ts);
      }
    }
    if (core.role() == Role.LEADER && (!wasLeader || core.currentTermView() != termBefore)) {
      // a new leadership: the answers to an earlier one (possibly long ago) say nothing about it,
      // and taken for its contacts they would make its first lease check step it down
      contacts.clear();
      ResearchMetrics.add(metricsPrefix + ".leader_terms", 1);
      leadershipStartNanos = now;
    }
    dispatcher.execute(actions);
    appliedView = core.lastApplied();
    commitView = core.commitIndexViewForRole();
    finishReadRounds(ev);
    if (ev instanceof Event.RpcFailed && core.replicator() != null) {
      FollowerProgress fp = core.replicator().progress(((Event.RpcFailed) ev).to);
      if (fp != null && fp.backoffUntilNanos > now) {
        timer.after(
            (fp.backoffUntilNanos - now) / 1_000_000L + 1, new Event.RunScheduler()); // retry later
      }
    }
    if ((pendingInstall == null || !profile.usesFileCheckpoints())
        && core.role() == Role.LEADER
        && core.replicator() != null
        && core.replicator().hasWork(core)) {
      executor.execute(() -> handle(new Event.RunScheduler()));
    }
  }

  /** An answer a peer sends the leader of its term (contact for the lease). */
  private static boolean isAnswer(Message m) {
    return m instanceof AppendEntriesRes
        || m instanceof RecoverTermRes
        || m instanceof UpdateSyncRes;
  }

  /**
   * When a majority of voters (this node included) last answered this leader; a voter that has not
   * answered counts as last heard at {@code missing}.
   */
  private long quorumContactNanos(long now, long missing) {
    Membership m = core.membershipView();
    List<Long> times = new ArrayList<>();
    for (int v : m.voters()) {
      times.add(v == node.self ? now : contacts.getOrDefault(v, missing));
    }
    if (times.isEmpty()) {
      return Long.MIN_VALUE;
    }
    times.sort(java.util.Collections.reverseOrder());
    return times.get(Math.min(m.majority(), times.size()) - 1);
  }

  /**
   * True while a majority answered within the lease (electionTimeoutMin x leaseRatio). Without
   * answers from a majority there is no lease ({@code now - Long.MIN_VALUE} would overflow).
   */
  public boolean leaseValid() {
    long now = System.nanoTime();
    long lease =
        (long) (node.config.election.electionTimeoutMinMs * node.config.read.leaseRatio)
            * 1_000_000L;
    long quorum = quorumContactNanos(now, Long.MIN_VALUE);
    return isLeader() && quorum != Long.MIN_VALUE && now - quorum <= lease;
  }

  // ------------------------------------------------------------------ reads

  /**
   * A read at the configured consistency: LOCAL, LEASE (leader lease) or READ_INDEX. A leader
   * serves LEASE and READ_INDEX reads only once it is ready (its election entry applied): before,
   * its commit index may still lag entries the previous leader committed and acknowledged.
   */
  public DataSet read(IConsensusRequest request, RaftConfig.ReadConsistency consistency)
      throws ConsensusException {
    requireHealthyForRead();
    long timeout = node.config.read.readIndexTimeoutMs;
    switch (consistency) {
      case LEASE:
        if (isLeader() && awaitLeaderReady(timeout)) {
          if (!leaseValid()) {
            throw new ConsensusException(
                "leader lease expired: no majority contact within the lease");
          }
          waitApplied(confirmReadIndex(timeout), timeout);
          return readHealthyStateMachine(request);
        }
        return readAtLeaderCommit(request, timeout);
      case READ_INDEX:
        if (isLeader() && awaitLeaderReady(timeout)) {
          long readIndex = confirmReadIndex(timeout);
          waitApplied(readIndex, timeout);
          return readHealthyStateMachine(request);
        }
        return readAtLeaderCommit(request, timeout);
      default:
        return readHealthyStateMachine(request);
    }
  }

  /**
   * Wait (bounded) until this node is a ready leader or no longer the leader; true if it is a ready
   * leader.
   */
  private boolean awaitLeaderReady(long timeoutMs) throws ConsensusException {
    waitUntil(() -> !isLeader() || isLeaderReady(), timeoutMs, "the leader to become ready");
    return isLeader();
  }

  /**
   * The index a leader read waits to see applied: the commit index, or under out-of-order commit
   * the largest committed index (an entry beyond a hole may be committed and acknowledged).
   */
  private long leaderReadIndex() {
    return outOfOrder ? core.readIndexView() : commitView;
  }

  /**
   * Follower read index: ask the leader for its commit index (under out-of-order commit its largest
   * committed index, {@code maxCommitted}), apply up to it, then read locally.
   */
  private DataSet readAtLeaderCommit(IConsensusRequest request, long timeoutMs)
      throws ConsensusException {
    int leader = core.leaderId();
    TEndPoint ep = leader < 0 ? null : core.membershipView().endpointOf(leader);
    if (ep == null) {
      throw new ConsensusException("no known leader for a consistent read");
    }
    long readIndex;
    try (SyncRaftServiceClient c = node.syncClients.borrowClient(ep)) {
      TGroupStatusRes s =
          c.getGroupStatus(
              new TGroupStatusReq(gid.convertToTConsensusGroupId()).setReadBarrier(true));
      if (s.getRole() != Role.LEADER.ordinal()) {
        throw new ConsensusException("N" + leader + " is no longer the leader");
      }
      readIndex = s.isSetMaxCommitted() ? s.getMaxCommitted() : s.getCommitIndex();
    } catch (ConsensusException e) {
      throw e;
    } catch (Exception e) {
      throw new ConsensusException("read index from leader N" + leader + " failed: " + e);
    }
    waitApplied(readIndex, timeoutMs);
    return readHealthyStateMachine(request);
  }

  /** Obtain a quorum confirmation for this read, then wait for the captured prefix to apply. */
  public TGroupStatusRes readIndexStatus() throws ConsensusException {
    requireHealthyForRead();
    if (!awaitLeaderReady(node.config.read.readIndexTimeoutMs)) {
      throw new ConsensusException("not the ready leader");
    }
    long index = confirmReadIndex(node.config.read.readIndexTimeoutMs);
    TGroupStatusRes status = status();
    status.setCommitIndex(index);
    if (outOfOrder) {
      status.setMaxCommitted(index); // the confirmed index is the largest committed one
    } else {
      status.unsetMaxCommitted();
    }
    return status;
  }

  private DataSet readHealthyStateMachine(IConsensusRequest request) throws ConsensusException {
    requireHealthyForRead();
    return stateMachine.read(request);
  }

  private void requireHealthyForRead() throws ConsensusException {
    if (!started || closed || failure != null) {
      throw new ConsensusException("Raft group is unavailable for reads", failure);
    }
  }

  private long confirmReadIndex(long timeoutMs) throws ConsensusException {
    requireHealthyForRead();
    CompletableFuture<Long> result = new CompletableFuture<>();
    executor.execute(
        () -> {
          if (!isLeader() || !core.leaderReady()) {
            result.completeExceptionally(new NotLeaderException(core.leaderId()));
            return;
          }
          // the commit index, or under out-of-order commit the largest committed index: an entry
          // beyond a hole may be committed and acknowledged (the leader holds every committed
          // entry, so its term is known; the applied frontier passes it once the hole is filled)
          long index = outOfOrder ? core.readIndexView() : core.commitIndexViewForRole();
          LogView view = core.logView();
          long term = index < view.firstIndex() ? view.snapshotTerm() : view.termAt(index);
          AppendEntriesReq request = core.template(index, term, false, 0);
          ReadRound round = new ReadRound(request.term, index, core.membershipView(), result);
          if (round.membership.isVoter(node.self)) {
            round.acknowledgers.add(node.self);
          }
          readRounds.put(request.msgId, round);
          if (round.acknowledgers.size() >= round.membership.majority()) {
            readRounds.remove(request.msgId);
            result.complete(index);
          } else {
            for (int voter : round.membership.voters()) {
              if (voter != node.self) {
                transport.send(voter, request, request.msgId);
              }
            }
          }
        });
    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusException("interrupted during read confirmation", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new ConsensusException("cannot confirm read with a majority", e);
    } finally {
      executor.execute(() -> readRounds.values().removeIf(round -> round.result == result));
    }
  }

  /** Runs on the group executor after the kernel has processed any term change. */
  private void finishReadRounds(Event event) {
    Event.Inbound inbound = event instanceof Event.Inbound ? (Event.Inbound) event : null;
    AppendEntriesRes response =
        inbound != null && inbound.msg instanceof AppendEntriesRes
            ? (AppendEntriesRes) inbound.msg
            : null;
    java.util.Iterator<Map.Entry<Long, ReadRound>> iterator = readRounds.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, ReadRound> entry = iterator.next();
      ReadRound round = entry.getValue();
      if (core.role() != Role.LEADER
          || core.currentTermView() != round.term
          || !core.membershipView().equals(round.membership)) {
        round.result.completeExceptionally(new NotLeaderException(core.leaderId()));
        iterator.remove();
      } else if (response != null
          && response.msgId == entry.getKey()
          && response.term == round.term
          && round.membership.isVoter(inbound.from)) {
        // A rejection still confirms the term; it need not certify that this follower has
        // the whole prefix. Read safety comes from the leader's committed prefix.
        round.acknowledgers.add(inbound.from);
        if (round.acknowledgers.size() >= round.membership.majority()) {
          round.result.complete(round.index);
          iterator.remove();
        }
      }
    }
  }

  private void waitApplied(long index, long timeoutMs) throws ConsensusException {
    waitUntil(() -> appliedView >= index, timeoutMs, "apply of e" + index);
  }

  private void waitUntil(java.util.function.BooleanSupplier cond, long timeoutMs, String what)
      throws ConsensusException {
    long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new ConsensusException("timed out waiting for " + what);
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConsensusException("interrupted while waiting for " + what);
      }
    }
  }

  public RaftGroupMetrics metrics() {
    return metrics;
  }

  /** A client write; the future completes after apply or exceptionally with NotLeaderException. */
  public CompletableFuture<TSStatus> write(IConsensusRequest request) {
    CompletableFuture<TSStatus> extension = submitWriteExtension(request);
    if (extension != null) {
      return extension;
    }
    CompletableFuture<TSStatus> f = new CompletableFuture<>();
    if (failure != null) {
      f.completeExceptionally(new IllegalStateException(gid + " stopped: " + failure, failure));
      return f;
    }
    if (core.role() != Role.LEADER) {
      f.completeExceptionally(new NotLeaderException(core.leaderId()));
      return f;
    }
    long reqId = requests.register(f);
    RequestMeta meta = preprocessor.preprocess(request);
    onEvent(new Event.ClientWrite(reqId, request.serializeToByteBuffer(), meta, request));
    return f;
  }

  /** Optional specialized entry metrics; ordinary Raft has no aggregate entries. */
  public boolean routesWritesToL0() {
    return false;
  }

  public long l0ProposedBatches() {
    return 0;
  }

  public long l0ProposedRequests() {
    return 0;
  }

  public long l0ProposedRawBytes() {
    return 0;
  }

  public long l0ProposedPayloadBytes() {
    return 0;
  }

  public long l0AppliedBatches() {
    return applier == null ? 0 : applier.l0AppliedBatches();
  }

  /** An inbound RPC; the future completes with the kernel's Reply. */
  public CompletableFuture<Message> handle(int from, Message msg) {
    CompletableFuture<Message> f = new CompletableFuture<>();
    long msgId = msg instanceof AppendEntriesReq ? ((AppendEntriesReq) msg).msgId : 0;
    onEvent(new Event.Inbound(from, msg, new ReplyHandle(from, msgId, f)));
    return f;
  }

  public CompletableFuture<Void> transferLeadership(int target) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    onEvent(new Event.TransferLeadershipReq(target, f));
    return f;
  }

  /** Cancel a pending transfer (facade timeout). */
  public void cancelTransfer() {
    onEvent(new Event.TransferLeadershipReq(-1, null));
  }

  public CompletableFuture<Void> configChange(
      Event.ConfigOp op, int peer, TEndPoint endpoint, Map<Integer, TEndPoint> peers) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    onEvent(new Event.ConfigChangeReq(op, peer, endpoint, peers, f));
    return f;
  }

  public void takeSnapshot(boolean force) {
    onEvent(new Event.TakeSnapshotReq(force));
  }

  /** Drain prior applies, capture their exact index on the kernel thread, then freeze files. */
  private void takeSnapshotFenced(Action.TakeSnapshot requested) {
    applyExecutor.execute(
        () -> {
          CompletableFuture<Action.TakeSnapshot> boundary = new CompletableFuture<>();
          executor.execute(
              () -> {
                try {
                  boundary.complete(
                      core.snapshotAtAppliedBoundary(applier.appliedIndex(), requested.force));
                } catch (Throwable t) {
                  boundary.completeExceptionally(t);
                }
              });
          try {
            snapshots.takeFenced(boundary.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS));
          } catch (Exception e) {
            LOGGER.warn("{}: cannot capture checkpoint boundary", gid, e);
            onEvent(new Event.SnapshotDone(-1, 0, null));
          }
        });
  }

  public SnapshotManager.TransferStats fileTransferStats() {
    return snapshots.stats();
  }

  // ------------------------------------------------------------ snapshot receive

  public TSStatus onInstallSnapshotMeta(TInstallSnapshotMetaReq req) {
    if (InboundFaults.current().blockSnapshot(node.self)) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "snapshot isolated by test hook");
    }
    CompletableFuture<Void> running = pendingInstall;
    if (running != null) {
      // refused before any fragment moves. The answer waits for the load, up to the RPC timeout:
      // a leader without a follower report to plan from (Raft) retries as soon as it hears this
      try {
        running.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException e) {
        LOGGER.debug(
            "{}: snapshot {} refused during a load: {}", gid, req.getSnapshotId(), e.toString());
      }
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, INSTALL_BUSY);
    }
    return snapshots.onInstallMeta(req);
  }

  public TSStatus onSnapshotFragment(TSendSnapshotFragmentReq req) {
    if (InboundFaults.current().blockSnapshot(node.self)) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "snapshot isolated by test hook");
    }
    return snapshots.onFragment(req);
  }

  public TSStatus onSnapshotLoad(TTriggerSnapshotLoadReq req) {
    if (!profile.usesFileCheckpoints()) {
      return snapshots.onLoad(
          req, this::installReceived, () -> pendingInstall != null, INSTALL_BUSY, !outOfOrder);
    }
    if (InboundFaults.current().blockSnapshot(node.self)) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "snapshot isolated by test hook");
    }
    try {
      TSStatus status =
          snapshots.onLoad(
              req,
              new SnapshotManager.Installer() {
                @Override
                public TSStatus prepare(SnapshotMeta received) {
                  if (!profile.usesFileCheckpoints()) {
                    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
                  }
                  String refusal =
                      beginInstall(received.index, new CompletableFuture<Void>(), req.getTerm());
                  return refusal == null
                      ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                      : RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, refusal);
                }

                @Override
                public TSStatus install(SnapshotMeta received) {
                  return installReceived(received);
                }
              });
      if (profile.usesFileCheckpoints()) {
        if (snapshots.hasPendingInstall()) {
          // The immutable stage is the recovery authority until publication and journal removal.
          // Do not acknowledge a new suffix that restart would then cut behind this marker.
          onApplierFailure(new IOException("checkpoint installation requires recovery"));
        } else {
          CompletableFuture<Void> install = pendingInstall;
          if (install != null) {
            endInstall(install);
          }
        }
      }
      return status;
    } catch (RuntimeException e) {
      if (profile.usesFileCheckpoints() && snapshots.hasPendingInstall()) {
        onApplierFailure(e);
      }
      throw e;
    }
  }

  public TSStatus onSnapshotCleanup(String snapshotId) {
    return snapshots.cleanup(snapshotId);
  }

  /** Runs on the RPC worker: load the state machine, then reset the kernel on the group thread. */
  private TSStatus installReceived(SnapshotMeta m) {
    // Out-of-order commit (ParallelRaft): the kernel refuses a snapshot older than the node's own
    // (RaftCore#acceptsSnapshot). The check and the load must not interleave with a local
    // snapshot, whose compaction could pass the received snapshot in between: wait for a running
    // one, then check and block new ones in one step on the group thread.
    CompletableFuture<Void> install =
        profile.usesFileCheckpoints() ? pendingInstall : new CompletableFuture<Void>();
    String refusal =
        profile.usesFileCheckpoints()
            ? (install == null ? "checkpoint installation was not prepared" : null)
            : beginInstall(m.index, install);
    if (refusal != null) {
      LOGGER.info("{}: refused snapshot {}: {}", gid, m.id(), refusal);
      if (!refusal.equals(INSTALL_BUSY)) {
        discardReceived(m);
      }
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, refusal);
    }
    if (profile.usesFileCheckpoints()) {
      CompletableFuture<TSStatus> loaded = new CompletableFuture<>();
      applyExecutor.execute(
          () -> {
            try {
              // beginInstall drained old persistence and blocked new inbound work. Any queued
              // prefix applies have now completed; no application overlaps the loader.
              log.deleteFrom(m.index + 1);
              log.sync();
              TSStatus status = loadReceived(m);
              if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                applier.resetAppliedIndex(m.index);
              }
              loaded.complete(status);
            } catch (Throwable t) {
              loaded.completeExceptionally(t);
              onApplierFailure(t);
            }
            // onSnapshotLoad releases the fence after the manager publishes the checkpoint and
            // removes its installation journal, rather than immediately after loading the state.
          });
      try {
        return loaded.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // The task retains its fence until it actually finishes, even if this RPC times out.
        // Stop serving until restart can pair the journaled state with the durable log cut.
        onApplierFailure(e);
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
      }
    }
    // The window stays held by the load itself, however long this RPC waits: released with the
    // answer, a load still running could interleave with a local snapshot or a second install.
    CompletableFuture<TSStatus> loaded = new CompletableFuture<>();
    applyExecutor.execute(
        () -> {
          try {
            loaded.complete(loadReceived(m));
          } catch (Throwable t) {
            loaded.completeExceptionally(t);
          } finally {
            endInstall(install);
          }
        });
    try {
      return loaded.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      if (closed) {
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, gid + " stopped");
      }
      // still loading: later transfers get INSTALL_BUSY until it finishes. A ParallelRaft leader
      // plans again from the follower's next report (its first index and contiguous end); a Raft
      // leader has no such report and sends the snapshot again: answered without a second load
      // once the kernel took this one (SnapshotManager#onLoad)
      LOGGER.warn("{}: snapshot {} still loading after the RPC timeout", gid, m.id());
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, INSTALL_BUSY);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    } catch (ExecutionException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getCause().toString());
    }
  }

  private static final String INSTALL_BUSY = "another snapshot install is in progress";

  private static final String LOCAL_SNAPSHOT_RUNNING = "a local snapshot is in progress";

  /**
   * Out-of-order commit: on the group thread, once no local snapshot runs, check the kernel and
   * make {@code install} the pending one (local snapshots are refused until {@link #endInstall}).
   * Null if the install may go on, else why not.
   */
  private String beginInstall(long index, CompletableFuture<Void> install) {
    return beginInstall(index, install, -1);
  }

  private String beginInstall(long index, CompletableFuture<Void> install, long expectedTerm) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(node.config.rpc.rpcTimeoutMs);
    while (true) {
      CompletableFuture<String> verdict = new CompletableFuture<>();
      executor.execute(
          () -> {
            try {
              if (expectedTerm >= 0 && core.currentTermView() != expectedTerm) {
                verdict.complete("checkpoint session term changed");
              } else if (pendingInstall != null) {
                verdict.complete(INSTALL_BUSY);
              } else if (profile.usesFileCheckpoints() && core.hasPendingPersistence()) {
                verdict.complete(LOCAL_SNAPSHOT_RUNNING);
              } else if (core.snapshotInProgress()) {
                verdict.complete(LOCAL_SNAPSHOT_RUNNING);
              } else if (!core.acceptsSnapshot(index)) {
                verdict.complete("snapshot older than own");
              } else {
                pendingInstall = install;
                verdict.complete(null);
              }
            } catch (Throwable t) {
              verdict.completeExceptionally(t);
            }
          });
      String v;
      try {
        v = verdict.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return "interrupted";
      } catch (ExecutionException | TimeoutException e) {
        endInstall(install); // runs after the check, which may still mark this install pending
        return "cannot check the snapshot against the log: " + e;
      }
      if (!LOCAL_SNAPSHOT_RUNNING.equals(v) || System.nanoTime() > deadline) {
        return v;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return "interrupted";
      }
    }
  }

  /** On the group thread: {@code install} is over; local snapshots may run again. */
  private void endInstall(CompletableFuture<Void> install) {
    executor.execute(
        () -> {
          if (pendingInstall == install) {
            pendingInstall = null;
            while (!deferredDuringInstall.isEmpty()) {
              Event deferred = deferredDuringInstall.removeFirst();
              executor.execute(() -> handle(deferred));
            }
          }
          install.complete(null);
        });
  }

  /**
   * A refused snapshot, or one the state machine failed to load, was already moved into the
   * snapshot directory; a restart would take it for this node's latest one (and cut the log behind
   * it) although the state machine never loaded it.
   */
  private void discardReceived(SnapshotMeta m) {
    SnapshotMeta latest = snapshots.latest();
    if (latest == null || !latest.dir.equals(m.dir)) {
      FileUtils.deleteFileOrDirectory(m.dir);
    }
  }

  /** What a received snapshot does to this node, decided once before anything is touched. */
  private enum InstallPlan {
    REFUSE,
    LOAD,
    COMPACT_ONLY
  }

  /**
   * Load {@code m} into the state machine, then into the kernel. If the state machine fails, the
   * received directory is discarded and the group stops (every protocol): a restart rebuilds the
   * state machine from the last snapshot and the log. If the kernel does not confirm in time, the
   * directory stays: the state machine already holds the snapshot's state, which a restart must
   * pair with it (the log cut behind it), and the kernel event may still run. A snapshot the state
   * machine already covers (out-of-order commit) is not loaded: this task holds the apply thread,
   * so every Applied it produced is ahead of the check, and nothing is applied until the kernel
   * moved its compaction point.
   */
  private TSStatus loadReceived(SnapshotMeta m) {
    CompletableFuture<InstallPlan> planned = new CompletableFuture<>();
    executor.execute(
        () -> {
          try {
            if (!core.acceptsSnapshot(m.index)) {
              planned.complete(InstallPlan.REFUSE);
            } else if (core.snapshotCoveredByApplied(m.index, m.term)) {
              planned.complete(InstallPlan.COMPACT_ONLY);
            } else {
              planned.complete(InstallPlan.LOAD);
            }
          } catch (Throwable t) {
            planned.completeExceptionally(t);
          }
        });
    InstallPlan plan;
    try {
      plan = planned.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOGGER.error("{}: cannot check snapshot {} against the log", gid, m.id(), e);
      if (profile.usesFileCheckpoints()) {
        onApplierFailure(e);
      } else {
        discardReceived(m);
      }
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
    if (plan == InstallPlan.REFUSE) {
      discardReceived(m);
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "snapshot older than applied state");
    }
    if (plan == InstallPlan.LOAD) {
      try {
        stateMachine.loadSnapshot(m.dir);
        applier.installedSnapshot(m.index);
      } catch (Throwable t) {
        // a load clears the state machine before it restores: whatever it held may be gone while
        // the kernel still counts it as applied, so reads and later entries would meet a hole
        LOGGER.error("{}: state machine cannot load snapshot {}", gid, m.id(), t);
        discardReceived(m);
        applier.stopAfterFailedLoad(t);
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, t.toString());
      }
    }
    boolean coveredByApplied = plan == InstallPlan.COMPACT_ONLY;
    CompletableFuture<Void> done = new CompletableFuture<>();
    executor.execute(
        () -> {
          try {
            handle(
                new Event.SnapshotLoaded(
                    m.index,
                    m.term,
                    m.membership,
                    profile.decodeSnapshotMeta(m.ext),
                    coveredByApplied));
            snapshots.installed(m);
            done.complete(null);
          } catch (Throwable t) {
            done.completeExceptionally(t);
          }
        });
    try {
      done.get(node.config.rpc.rpcTimeoutMs, TimeUnit.MILLISECONDS);
      snapshots.retainAfterInstall();
      LOGGER.info("{}: installed snapshot {}", gid, m.id());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      LOGGER.error("{}: kernel did not accept snapshot {}", gid, m.id(), e);
      if (profile.usesFileCheckpoints()) {
        onApplierFailure(e);
      }
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
  }

  // -------------------------------------------------------------------- views

  public ConsensusGroupId groupId() {
    return gid;
  }

  public File dir() {
    return dir;
  }

  public boolean isLeader() {
    return started && !closed && failure == null && core.role() == Role.LEADER;
  }

  public boolean isLeaderReady() {
    return isLeader() && (leaderReady || core.leaderReady());
  }

  public int leaderId() {
    return core.leaderId();
  }

  public long term() {
    return core.currentTermView();
  }

  public long commitIndex() {
    return core.commitIndexViewForRole();
  }

  public Membership membership() {
    return core.membershipView();
  }

  public IStateMachine stateMachine() {
    return stateMachine;
  }

  public RaftCore core() {
    return core;
  }

  /** The profile's entry-metadata codec (log records and RPC payloads). */
  /** Decode on the RPC worker; no file I/O or link waiting runs on the Raft core executor. */
  public org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq decodeAppend(
      org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq request) throws IOException {
    replicationLink.receive(
        org.apache.iotdb.consensus.raft.transport.LogFileTransfer.payloadBytes(request));
    if (request.isSetLogFile()) {
      if (logFiles == null) {
        throw new IOException("File append is not enabled for this group");
      }
      return logFiles.decode(request);
    }
    return org.apache.iotdb.consensus.raft.transport.ThriftCodec.decode(
        request, profile.metaCodec());
  }

  public long receivedLogFiles() {
    return logFiles == null ? 0 : logFiles.receivedFiles();
  }

  public EntryMetaCodec metaCodec() {
    return profile.metaCodec();
  }

  public SnapshotManager snapshots() {
    return snapshots;
  }

  /** Leader-side match index of a follower (-1 if unknown); a racy read for progress polling. */
  public long followerMatchIndex(int follower) {
    if (core.replicator() == null) {
      return -1;
    }
    FollowerProgress fp = core.replicator().progress(follower);
    return fp == null ? -1 : fp.matchIndex;
  }

  /**
   * The group status for status RPCs and metrics. Every value is computed on the group thread (the
   * kernel's strategies, the replicator and the apply policy are not thread-safe); if that thread
   * does not answer within a second (busy, or the group stopped), the last status it computed.
   * Never call it from the group thread.
   */
  public TGroupStatusRes status() {
    if (!closed) {
      CompletableFuture<TGroupStatusRes> f = new CompletableFuture<>();
      executor.execute(
          () -> {
            try {
              TGroupStatusRes s = computeStatus();
              lastStatus = s;
              f.complete(s);
            } catch (Throwable t) {
              f.completeExceptionally(t);
            }
          });
      try {
        return new TGroupStatusRes(
            f.get(
                Math.max(1, Math.min(1000, node.config.rpc.rpcTimeoutMs)), TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException e) {
        LOGGER.debug("{}: status not computed in time: {}", gid, e.toString());
      }
    }
    TGroupStatusRes last = lastStatus;
    return last == null ? null : new TGroupStatusRes(last);
  }

  /**
   * The status as of the end of the last event; group thread only (or before the first event). The
   * pending size is {@link LeaderStateView#pendingCount()}; under out-of-order commit the status
   * adds the sync number, the largest committed index and the committed entries not yet applied.
   */
  private TGroupStatusRes computeStatus() {
    LogView lv = core.logView();
    TGroupStatusRes s =
        new TGroupStatusRes(
            core.currentTermView(),
            core.leaderId(),
            core.role().ordinal(),
            core.commitIndexViewForRole(),
            core.lastApplied(),
            lv.firstIndex(),
            lv.lastIndex(),
            lv.contiguousEnd());
    long wp = core.gate().workingPartition();
    if (wp >= 0) {
      s.setWorkingPartition(wp);
    }
    long rhoMax = core.ingestStateView().partitionMax();
    if (rhoMax >= 0) {
      s.setRhoMax(rhoMax);
    }
    if (core.replicator() instanceof LeaderStateView) {
      s.setPendingSize(((LeaderStateView) core.replicator()).pendingCount());
    }
    if (outOfOrder) {
      long sync = core.gate().syncNumber();
      if (sync != AppendEntriesRes.UNSET) {
        s.setSyncNumber(sync);
      }
      s.setMaxCommitted(core.readIndexView());
      ApplyPolicy ap = core.applyPolicyView();
      long pending = ap == null ? -1 : ap.pendingApply();
      if (pending >= 0) {
        s.setPendingApply(pending);
      }
    }
    return s;
  }

  protected void prepareStateMachineReplay(boolean noSnapshot, LogIndex index, long term)
      throws IOException {}

  protected org.apache.iotdb.consensus.raft.transport.LogFileTransfer createLogFileTransfer() {
    return null;
  }

  protected void startWriteExtension() {}

  protected void stopWriteExtension() {}

  protected CompletableFuture<TSStatus> submitWriteExtension(IConsensusRequest request) {
    return null;
  }

  protected boolean isAggregateRequest(Object request) {
    return false;
  }

  protected RaftApplier createApplier() {
    return new RaftApplier(
        gid + "-apply",
        log,
        stateMachine,
        requests,
        applyExecutor,
        this::onEvent,
        this::onApplierFailure,
        outOfOrder);
  }
}
