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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ConsensusWriteNormalizer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.config.RaftConfigLoader;
import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.raft.storage.MembershipStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.runtime.TRaftGroup;
import org.apache.iotdb.consensus.traft.paper.runtime.TRaftRpc;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperBatch;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperControl;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperWrite;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Current Overleaf protocol: fixed-membership, durable proposals and recovery before service. */
public class TRaftConsensus implements IConsensus, TRaftRpc.Handler {
  public static final String CLASS_NAME = "org.apache.iotdb.consensus.traft.TRaftConsensus";
  private final int self;
  private final TEndPoint endpoint;
  private final File storage;
  private final IStateMachine.Registry registry;
  private final RaftConfig config;
  private final int theta;
  private final long partitionMillis;
  private final boolean compaction;
  private final boolean seriesOrder;
  private final long journalAsyncForceMillis;
  private final boolean journalPipeline;
  private final boolean replicateBeforeForce;
  private final long journalFlushMillis;
  private final long journalFlushEntries;
  private final boolean pipelineRows;
  private final boolean applyBatch;
  private final boolean applyAsync;
  private final int admissionRound;
  private final long fetchGraceMs;
  private final Map<ConsensusGroupId, TRaftGroup> groups = new ConcurrentHashMap<>();
  private final ExecutorService cores;
  private final ExecutorService network;
  private final ExecutorService journal;
  private final ExecutorService appliers;
  private final ScheduledExecutorService timers;
  private final TRaftRpc rpc;
  private Map<ConsensusGroupId, List<Peer>> expectedPeers = Collections.emptyMap();
  // Request identities: a random prefix per node process plus a sequence. They only need to be
  // unique, so writes no longer draw a SecureRandom UUID each, which contends across client
  // threads.
  private final String tokenPrefix;
  private final AtomicLong tokenSequence = new AtomicLong();
  private volatile boolean started;

  public TRaftConsensus(ConsensusConfig cc, IStateMachine.Registry registry) {
    self = cc.getThisNodeId();
    endpoint = cc.getThisNodeEndPoint();
    storage = new File(cc.getStorageDir());
    this.registry = registry;
    config = RaftConfigLoader.load(cc);
    Properties properties = RaftConfigLoader.collect();
    // Zero: no size limit, a partition closes only on an order conflict, a barrier, the timer or a
    // leadership change.
    long requestedTheta =
        RaftConfigLoader.getLong(properties, "traft_partition_max_entries", 10000);
    partitionMillis = RaftConfigLoader.getLong(properties, "traft_partition_max_ms", 1000);
    // Drop the in-memory state of partitions every member has closed; false keeps all history.
    compaction = RaftConfigLoader.getBool(properties, "traft_compaction", true);
    // SERIES: an entry opens a partition only on a conflict within its own series (device);
    // GLOBAL: on any timestamp not newer than the group's maximum, the paper's original rule.
    String orderScope = properties.getProperty("traft_order_scope", "SERIES").trim();
    if (!orderScope.equals("SERIES") && !orderScope.equals("GLOBAL")) {
      throw new IllegalArgumentException("traft_order_scope must be SERIES or GLOBAL");
    }
    seriesOrder = orderScope.equals("SERIES");
    // SYNC: every journal append is forced before it returns, so every ACK is durable. ASYNC:
    // IoTDB's wal_mode=ASYNC for the journal, forced every traft_journal_async_fsync_ms; an ACK
    // then survives a process crash but not power loss (TRaftFileStore.setAsyncForceInterval).
    String journalMode = properties.getProperty("traft_journal_mode", "SYNC").trim();
    if (!journalMode.equals("SYNC") && !journalMode.equals("ASYNC")) {
      throw new IllegalArgumentException("traft_journal_mode must be SYNC or ASYNC");
    }
    journalAsyncForceMillis =
        journalMode.equals("ASYNC")
            ? RaftConfigLoader.getLong(properties, "traft_journal_async_fsync_ms", 1000)
            : -1;
    if (journalMode.equals("ASYNC") && journalAsyncForceMillis < 0) {
      throw new IllegalArgumentException("traft_journal_async_fsync_ms must not be negative");
    }
    // SYNC only: protocol rounds write without waiting for the disk, a journal thread forces, and
    // frames and replies leave once forced (TRaftGroup.pipelineJournal); false forces inline.
    journalPipeline =
        journalAsyncForceMillis < 0
            && RaftConfigLoader.getBool(properties, "traft_journal_pipeline", true);
    // Pipelined SYNC journal only: data leaves before the local force, while ACKs still wait for
    // theirs and a leader counts remote ACKs only after its own force (TRaftGroup).
    replicateBeforeForce =
        RaftConfigLoader.getBool(properties, "traft_replicate_before_force", true);
    // Pipelined SYNC journal only: a force waits until the unforced journal holds this many entries
    // or its oldest unforced write is this old; 0 ms forces at once. Every ACK still follows its
    // force, so writes wait up to the interval (TRaftFileStore.setForcePolicy).
    journalFlushMillis =
        RaftConfigLoader.getLong(properties, "traft_journal_flush_interval_ms", 1000);
    journalFlushEntries = RaftConfigLoader.getLong(properties, "traft_journal_flush_entries", 1000);
    if (journalFlushMillis < 0 || journalFlushEntries < 1) {
      throw new IllegalArgumentException(
          "traft_journal_flush_interval_ms >= 0 and traft_journal_flush_entries >= 1 required");
    }
    // Submit every row of a request before waiting for any, instead of waiting for each row's
    // commit before submitting the next (ConsensusWriteNormalizer.writePipelined).
    pipelineRows = RaftConfigLoader.getBool(properties, "traft_pipeline_rows", true);
    // Hand each protocol task's committed run to the state machine in one call (TRaftGroup).
    applyBatch = RaftConfigLoader.getBool(properties, "traft_apply_batch", true);
    // Write those runs to the state machine on a per-group applier thread instead of the group's
    // protocol executor, as Raft's apply pool does (TRaftGroup.setApplyExecutor).
    applyAsync = RaftConfigLoader.getBool(properties, "traft_apply_async", false);
    // Commands one drive admits before yielding the group's executor; 0 = the transport batch.
    admissionRound = (int) RaftConfigLoader.getLong(properties, "traft_admission_round", 0);
    // Batches to one follower travel in parallel, so a proposal often lands a moment before the
    // entries it names; a follower waits this long before fetching them. 0 fetches at once.
    fetchGraceMs = RaftConfigLoader.getLong(properties, "traft_fetch_grace_ms", 5);
    if (requestedTheta != 0 && (requestedTheta < 2 || requestedTheta > 1_000_000)
        || partitionMillis < 0) {
      throw new IllegalArgumentException(
          "TRaft requires theta = 0 (unlimited) or 2 <= theta <= 1000000, and timeout >= 0");
    }
    theta = requestedTheta == 0 ? Integer.MAX_VALUE : (int) requestedTheta;
    // A proposal has at most theta identities (17 encoded bytes each), plus
    // fixed metadata. Recovery reports use bounded pages independently of history size. An
    // unlimited partition is meant for ordered data: if it ever closes, its manifest must still
    // fit.
    if (requestedTheta != 0
        && config.replication.maxInflightBytes < Math.max(65536L, 4096L + 17L * theta)) {
      throw new IllegalArgumentException(
          "TRaft in-flight byte budget cannot hold a complete partition manifest");
    }
    // A group's protocol rounds force the journal on its executor thread (Raft hands its forces to
    // a per-group LogWriter thread instead). With a fixed pool smaller than the number of groups,
    // groups queue for a thread while others wait on their disk, so every commit step pays that
    // queue as well. Give each active group its own thread, as for deliveries below.
    cores = Executors.newCachedThreadPool(named("TRaft-core"));
    // Deliveries are synchronous Thrift calls that block a thread until the peer has persisted
    // the batch. Demand is bounded by groups x peers x maxInflightBatches, so size the pool to
    // that demand instead of letting a tiny fixed pool serialize peers behind one another.
    network = Executors.newCachedThreadPool(named("TRaft-network"));
    // One force loop per group at most, each mostly waiting for its disk.
    journal = Executors.newCachedThreadPool(named("TRaft-journal"));
    // One applier at a time per group, each mostly inside the storage engine.
    appliers = Executors.newCachedThreadPool(named("TRaft-apply"));
    timers = Executors.newScheduledThreadPool(1, named("TRaft-timer"));
    rpc = new TRaftRpc(endpoint, config.rpc, this);
    tokenPrefix = self + "-" + Long.toUnsignedString(new SecureRandom().nextLong(), 36) + "-";
  }

  private String newToken() {
    return tokenPrefix + Long.toString(tokenSequence.incrementAndGet(), 36);
  }

  private static java.util.concurrent.ThreadFactory named(String prefix) {
    java.util.concurrent.atomic.AtomicInteger sequence =
        new java.util.concurrent.atomic.AtomicInteger();
    return task -> {
      Thread t = new Thread(task, prefix + "-" + sequence.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  @Override
  public synchronized void start() throws IOException {
    if (started) {
      return;
    }
    AtomicFiles.createDirectories(storage);
    rpc.start();
    try {
      File[] dirs = storage.listFiles(File::isDirectory);
      if (dirs != null)
        for (File dir : dirs) {
          if (!dir.getName().matches("[0-9]+_[0-9]+")) {
            continue;
          }
          String[] parts = dir.getName().split("_");
          ConsensusGroupId id =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
          byte[] format = AtomicFiles.read(new File(dir, "paper-format"));
          if (format == null || format.length != 8 || ByteBuffer.wrap(format).getInt() != 3) {
            throw new IOException("TRaft group has an unsupported legacy format: " + dir);
          }
          int previousTheta = ByteBuffer.wrap(format).getInt(4);
          if (previousTheta < 2 || previousTheta > theta) {
            throw new IOException(
                "Cannot reduce TRaft partition capacity for an existing journal: " + dir);
          }
          MembershipStore.Stored stored = new MembershipStore(dir).load();
          if (stored == null) {
            throw new IOException("TRaft membership missing: " + dir);
          }
          List<Peer> peers = new ArrayList<>();
          for (int node : stored.membership.voters()) {
            peers.add(new Peer(id, node, stored.membership.endpointOf(node)));
          }
          if (expectedPeers.containsKey(id)
              && !new HashSet<>(expectedPeers.get(id)).equals(new HashSet<>(peers))) {
            throw new IOException("Fixed TRaft membership differs from ConfigNode for " + id);
          }
          if (previousTheta != theta) {
            AtomicFiles.write(new File(dir, "paper-format"), formatMarker());
          }
          groups.put(id, newGroup(id, peers));
        }
      started = true;
    } catch (Exception e) {
      for (TRaftGroup g : groups.values()) {
        try {
          g.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
      }
      groups.clear();
      rpc.close();
      throw new IOException("TRaft startup failed", e);
    }
  }

  private File dir(ConsensusGroupId id) {
    return new File(storage, id.getType().getValue() + "_" + id.getId());
  }

  private byte[] formatMarker() {
    return ByteBuffer.allocate(8).putInt(3).putInt(theta).array();
  }

  private TRaftGroup newGroup(ConsensusGroupId id, List<Peer> peers) throws IOException {
    TConsensusGroupId wire = id.convertToTConsensusGroupId();
    TRaftGroup group =
        new TRaftGroup(
            self,
            id,
            dir(id),
            peers,
            registry.apply(id),
            config,
            theta,
            partitionMillis,
            compaction,
            rpc,
            replies -> {
              try {
                accept(new TPaperBatch(wire, replies), false);
              } catch (ConsensusException e) {
                // The group was removed while a reply was in flight; the protocol retries later.
              }
            },
            cores,
            network,
            timers);
    group.setSeriesOrder(seriesOrder);
    group.setJournalAsyncForceInterval(journalAsyncForceMillis);
    if (journalPipeline) {
      group.pipelineJournal(journal);
      group.setReplicateBeforeForce(replicateBeforeForce);
      group.setForcePolicy(journalFlushMillis, journalFlushEntries);
    }
    group.setApplyBatch(applyBatch);
    group.setAdmissionRound(admissionRound);
    group.setFetchGrace(fetchGraceMs);
    if (applyAsync) {
      group.setApplyExecutor(appliers);
    }
    return group;
  }

  protected TRaftGroup group(ConsensusGroupId id) throws ConsensusGroupNotExistException {
    TRaftGroup g = groups.get(id);
    if (g == null) throw new ConsensusGroupNotExistException(id);
    return g;
  }

  private <T> T await(CompletableFuture<T> future) throws ConsensusException {
    try {
      return future.get(config.replication.writeTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusException(e);
    } catch (Exception e) {
      throw new ConsensusException("TRaft operation failed", e);
    }
  }

  private TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private TSStatus awaitWrite(CompletableFuture<TSStatus> future) throws ConsensusException {
    try {
      return await(future);
    } catch (ConsensusException failure) {
      // Discard only this caller's wait and live-request optimization. An admitted command may
      // still commit and must be applied from its durable bytes after timeout or interruption.
      future.cancel(false);
      throw failure;
    }
  }

  @Override
  public synchronized void stop() throws IOException {
    IOException failure = null;
    for (TRaftGroup g : groups.values()) {
      try {
        g.close();
      } catch (IOException e) {
        failure = e;
      }
    }
    groups.clear();
    rpc.close();
    timers.shutdownNow();
    network.shutdownNow();
    journal.shutdown();
    cores.shutdown();
    appliers.shutdown();
    started = false;
    if (failure != null) throw failure;
  }

  @Override
  public TSStatus write(ConsensusGroupId id, IConsensusRequest request) throws ConsensusException {
    if (pipelineRows) {
      TRaftGroup g = group(id);
      if (g.ready()) {
        // No row waits for an earlier row's commit; the leader forwards none of them.
        return ConsensusWriteNormalizer.writePipelined(
            request, child -> submitLocal(g, child), this::awaitWrite);
      }
    }
    return ConsensusWriteNormalizer.write(request, child -> writeSingle(id, child));
  }

  /** Admits one normalized child at this node, which leads the group, without waiting. */
  private CompletableFuture<TSStatus> submitLocal(TRaftGroup g, IConsensusRequest request) {
    RequestSemantics semantics =
        request instanceof RequestSemanticsProvider
            ? ((RequestSemanticsProvider) request).getRequestSemantics()
            : RequestSemantics.barrier();
    ByteBuffer bytes = request.serializeToByteBuffer().duplicate();
    byte[] payload = new byte[bytes.remaining()];
    bytes.get(payload);
    boolean barrier = semantics.getKind() == RequestSemantics.Kind.BARRIER;
    return g.write(
        null,
        barrier ? Kind.BARRIER : Kind.DATA,
        barrier ? 0 : semantics.getTimestamp(),
        !barrier && semantics.hasSeriesKey(),
        !barrier && semantics.hasSeriesKey() ? semantics.getSeriesKey() : 0,
        payload,
        request);
  }

  private TSStatus writeSingle(ConsensusGroupId id, IConsensusRequest request)
      throws ConsensusException {
    RequestSemantics semantics =
        request instanceof RequestSemanticsProvider
            ? ((RequestSemanticsProvider) request).getRequestSemantics()
            : RequestSemantics.barrier();
    ByteBuffer bytes = request.serializeToByteBuffer().duplicate();
    byte[] payload = new byte[bytes.remaining()];
    bytes.get(payload);
    boolean barrier = semantics.getKind() == RequestSemantics.Kind.BARRIER;
    long timestamp = barrier ? 0 : semantics.getTimestamp();
    boolean hasSeries = !barrier && semantics.hasSeriesKey();
    long seriesKey = hasSeries ? semantics.getSeriesKey() : 0;
    TRaftGroup g = group(id);
    if (g.ready()) {
      return awaitWrite(
          g.write(
              null,
              barrier ? Kind.BARRIER : Kind.DATA,
              timestamp,
              hasSeries,
              seriesKey,
              payload,
              request));
    }
    // A forwarded write carries no series key; the leader admits it as keyless (conservative).
    TPaperWrite forwarded =
        new TPaperWrite(
            id.convertToTConsensusGroupId(),
            newToken(),
            barrier,
            timestamp,
            ByteBuffer.wrap(payload));
    Peer leader = g.leaderPeer();
    if (leader == null || leader.getNodeId() == self)
      throw new ConsensusException("TRaft leader is not ready");
    try {
      return rpc.write(leader.getEndpoint(), forwarded);
    } catch (Exception e) {
      throw new ConsensusException("TRaft write forwarding failed", e);
    }
  }

  @Override
  public TSStatus write(TPaperWrite request) throws ConsensusException {
    TRaftGroup g =
        group(ConsensusGroupId.Factory.createFromTConsensusGroupId(request.getGroupId()));
    return awaitWrite(
        g.write(
            request.getRequestId(),
            request.isBarrier() ? Kind.BARRIER : Kind.DATA,
            request.getTimestamp(),
            request.getPayload()));
  }

  @Override
  public DataSet read(ConsensusGroupId id, IConsensusRequest request) throws ConsensusException {
    TRaftGroup g = group(id);
    if (config.read.consistency != RaftConfig.ReadConsistency.LOCAL) {
      if (g.ready()) {
        await(g.write(null, Kind.BARRIER, 0, new byte[0]));
      } else {
        Peer leader = g.leaderPeer();
        if (leader == null || leader.getNodeId() == self)
          throw new ConsensusException("No ready TRaft leader for read");
        TSStatus fence;
        try {
          fence =
              rpc.control(
                  leader.getEndpoint(),
                  new TPaperControl(id.convertToTConsensusGroupId(), "FENCE"));
        } catch (Exception e) {
          throw new ConsensusException("TRaft read fence failed", e);
        }
        if (fence.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
          throw new ConsensusException("TRaft read fence rejected");
        EntryId fenceId;
        try {
          String[] identity = fence.getMessage().split(":");
          if (identity.length != 2) {
            throw new IllegalArgumentException("Invalid fence identity");
          }
          fenceId = new EntryId(Long.parseLong(identity[0]), Long.parseLong(identity[1]));
        } catch (RuntimeException e) {
          throw new ConsensusException("Invalid TRaft read fence identity", e);
        }
        long deadline =
            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.read.readIndexTimeoutMs);
        while (!await(g.hasApplied(fenceId))) {
          if (System.nanoTime() > deadline)
            throw new ConsensusException("TRaft follower did not apply the read fence");
          try {
            Thread.sleep(2);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsensusException(e);
          }
        }
      }
    }
    return await(g.read(request));
  }

  @Override
  public List<ByteBuffer> deliver(TPaperBatch request) throws ConsensusException {
    return await(accept(request, true));
  }

  /**
   * Every inbound protocol frame passes here: RPC requests (which may answer with replies) and the
   * replies carried back in our own RPC responses. Tests override it to inject faults.
   */
  protected CompletableFuture<List<ByteBuffer>> accept(TPaperBatch batch, boolean replyAllowed)
      throws ConsensusException {
    return group(ConsensusGroupId.Factory.createFromTConsensusGroupId(batch.getGroupId()))
        .receive(batch.getMessages(), replyAllowed);
  }

  /**
   * Read-only snapshot of a local group ({@link TRaftGroup#status()}): observation for tests and
   * benchmarks, never a protocol input.
   */
  public TRaftGroup.Status groupStatus(ConsensusGroupId id) throws ConsensusException {
    return await(group(id).status());
  }

  @Override
  public TSStatus control(TPaperControl request) throws ConsensusException {
    TRaftGroup g =
        group(ConsensusGroupId.Factory.createFromTConsensusGroupId(request.getGroupId()));
    switch (request.getOperation()) {
      case "FENCE":
        return await(g.write(null, Kind.BARRIER, 0, new byte[0]));
      case "CAMPAIGN":
        await(g.campaign());
        return success();
      case "READY":
        return success().setMessage(Boolean.toString(g.ready()));
      default:
        throw new ConsensusException("Unknown TRaft control operation");
    }
  }

  @Override
  public synchronized void createLocalPeer(ConsensusGroupId id, List<Peer> peers)
      throws ConsensusException {
    if (groups.containsKey(id)) throw new ConsensusGroupAlreadyExistException(id);
    if (peers.isEmpty()) throw new IllegalPeerNumException(0);
    Map<Integer, TEndPoint> endpoints = new HashMap<>();
    for (Peer p : peers) {
      if (!id.equals(p.getGroupId()) || endpoints.put(p.getNodeId(), p.getEndpoint()) != null)
        throw new ConsensusException("Invalid TRaft membership");
    }
    if (!endpoint.equals(endpoints.get(self)))
      throw new IllegalPeerEndpointException(endpoint, peers);
    try {
      File d = dir(id);
      if (d.exists() && d.list() != null && d.list().length > 0)
        throw new IOException("Refusing to overwrite existing TRaft group " + d);
      AtomicFiles.createDirectories(d);
      new MembershipStore(d)
          .save(
              new Membership(
                  new ArrayList<>(endpoints.keySet()), Collections.emptyList(), endpoints, -1),
              true);
      AtomicFiles.write(new File(d, "paper-format"), formatMarker());
      groups.put(id, newGroup(id, peers));
    } catch (Exception e) {
      throw new ConsensusException("Cannot create TRaft group", e);
    }
  }

  @Override
  public synchronized void deleteLocalPeer(ConsensusGroupId id) throws ConsensusException {
    TRaftGroup g = group(id);
    try {
      g.close();
    } catch (IOException e) {
      throw new ConsensusException(e);
    }
    groups.remove(id);
    FileUtils.deleteFileOrDirectory(dir(id));
  }

  private ConsensusException fixed() {
    return new ConsensusException(
        "Current-paper TRaft has fixed membership; online membership changes are unsupported");
  }

  @Override
  public void addRemotePeer(ConsensusGroupId id, Peer peer) throws ConsensusException {
    throw fixed();
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId id, Peer peer) throws ConsensusException {
    throw fixed();
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(Map<ConsensusGroupId, List<Peer>> peers) {
    expectedPeers = new HashMap<>(peers);
  }

  @Override
  public void resetPeerList(ConsensusGroupId id, List<Peer> peers) throws ConsensusException {
    if (!new HashSet<>(group(id).peers()).equals(new HashSet<>(peers))) throw fixed();
  }

  @Override
  public void transferLeader(ConsensusGroupId id, Peer target) throws ConsensusException {
    TRaftGroup g = group(id);
    if (target.getNodeId() == self && g.ready()) return;
    if (!g.ready() || !g.peers().contains(target))
      throw new ConsensusException("TRaft transfer requires current leader and an existing voter");
    try {
      rpc.control(
          target.getEndpoint(), new TPaperControl(id.convertToTConsensusGroupId(), "CAMPAIGN"));
      long deadline =
          System.nanoTime()
              + TimeUnit.MILLISECONDS.toNanos(config.election.leaderTransferTimeoutMs);
      while (System.nanoTime() < deadline) {
        if (g.leaderId() == target.getNodeId()) {
          TSStatus s =
              rpc.control(
                  target.getEndpoint(),
                  new TPaperControl(id.convertToTConsensusGroupId(), "READY"));
          if ("true".equals(s.getMessage())) return;
        }
        Thread.sleep(10);
      }
      throw new ConsensusException("TRaft leader transfer timed out");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusException(e);
    } catch (Exception e) {
      throw new ConsensusException("TRaft transfer failed", e);
    }
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId id, boolean force) throws ConsensusException {
    await(group(id).takeSnapshot());
  }

  @Override
  public boolean isLeader(ConsensusGroupId id) {
    TRaftGroup g = groups.get(id);
    return g != null && g.ready();
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId id) {
    return isLeader(id);
  }

  @Override
  public long getLogicalClock(ConsensusGroupId id) {
    TRaftGroup g = groups.get(id);
    return g == null ? 0 : g.term();
  }

  @Override
  public Peer getLeader(ConsensusGroupId id) {
    TRaftGroup g = groups.get(id);
    return g == null ? null : g.leaderPeer();
  }

  @Override
  public int getReplicationNum(ConsensusGroupId id) {
    TRaftGroup g = groups.get(id);
    return g == null ? 0 : g.peers().size();
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(groups.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId id) {
    return dir(id).getAbsolutePath();
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig cc) {
    /* Immutable protocol parameters until restart. */
  }
}
