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

package org.apache.iotdb.consensus.raft;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.ConsensusWriteNormalizer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.raft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.client.RaftClientPool;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.config.RaftConfigLoader;
import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.PartitionedMeta;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.policy.FidelityProfile;
import org.apache.iotdb.consensus.raft.core.policy.ProtocolPolicy;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.RaftProfile;
import org.apache.iotdb.consensus.raft.server.NotLeaderException;
import org.apache.iotdb.consensus.raft.server.RaftGroupServer;
import org.apache.iotdb.consensus.raft.server.RequestPreprocessor;
import org.apache.iotdb.consensus.raft.server.WriteForwarder;
import org.apache.iotdb.consensus.raft.service.RaftRPCService;
import org.apache.iotdb.consensus.raft.service.RaftRPCServiceProcessor;
import org.apache.iotdb.consensus.raft.service.RaftRpcDispatcher;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.raft.thrift.TAdminOp;
import org.apache.iotdb.consensus.raft.thrift.TAdminReq;
import org.apache.iotdb.consensus.raft.thrift.TAdminRes;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteReq;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteRes;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaReq;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaRes;
import org.apache.iotdb.consensus.raft.thrift.TPeerInfo;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The self-implemented Raft consensus protocol as an IoTDB {@link IConsensus}. Loaded by {@code
 * ConsensusFactory} through reflection ({@code data_region_consensus_protocol_class =
 * org.apache.iotdb.consensus.raft.RaftConsensus}). The revised TRaft implementation has its own
 * kernel and runtime; it does not share this Raft commitment or recovery logic.
 */
public class RaftConsensus implements IConsensus, RaftRpcDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftConsensus.class);
  public static final String CLASS_NAME = "org.apache.iotdb.consensus.raft.RaftConsensus";

  protected final ConsensusConfig consensusConfig;
  protected final RaftConfig config;
  protected final IStateMachine.Registry registry;
  protected final int thisNodeId;
  protected final TEndPoint thisNode;
  protected final File storageDir;
  private final Map<ConsensusGroupId, RaftGroupServer> groups = new ConcurrentHashMap<>();
  private final IClientManager<TEndPoint, AsyncRaftServiceClient> asyncClients;
  private final IClientManager<TEndPoint, SyncRaftServiceClient> syncClients;
  private final WriteForwarder forwarder;
  private final RaftRPCService service;
  private final ExecutorService corePool;
  private final ExecutorService applyPool;
  private final ExecutorService snapshotPool;
  private final ScheduledExecutorService timerPool;
  private volatile RaftGroupServer.NodeContext nodeContext;
  private volatile RequestPreprocessor preprocessor;
  private Map<ConsensusGroupId, List<Peer>> correctPeerListBeforeStart;
  private volatile boolean started;

  public RaftConsensus(ConsensusConfig config, IStateMachine.Registry registry) {
    this.consensusConfig = config;
    this.config = loadConfig(config);
    this.registry = registry;
    this.thisNodeId = config.getThisNodeId();
    this.thisNode = config.getThisNodeEndPoint();
    this.storageDir = new File(config.getStorageDir());
    this.asyncClients = RaftClientPool.newAsyncManager(this.config.rpc);
    this.syncClients = RaftClientPool.newSyncManager(this.config.rpc);
    this.forwarder = new WriteForwarder(syncClients);
    this.service =
        new RaftRPCService(
            thisNode,
            this.config.rpc,
            new RaftRPCServiceProcessor(this, this.config.rpc.rpcTimeoutMs));
    this.corePool = groupPool(this.config.threads.coreThreads, protocolName() + "-core");
    this.applyPool = groupPool(this.config.threads.applyThreads, protocolName() + "-apply");
    this.snapshotPool = IoTDBThreadPoolFactory.newFixedThreadPool(2, protocolName() + "-snapshot");
    this.timerPool =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(protocolName() + "-timer");
  }

  /** Executors of the groups' GroupExecutors: see {@link RaftConfig.Threads}. */
  private static ExecutorService groupPool(int threads, String name) {
    return threads > 0
        ? IoTDBThreadPoolFactory.newFixedThreadPool(threads, name)
        : IoTDBThreadPoolFactory.newCachedThreadPool(name);
  }

  /** Built on first use so that subclass constructors have finished before the hooks run. */
  private RaftGroupServer.NodeContext nodeContext() {
    RaftGroupServer.NodeContext ctx = nodeContext;
    if (ctx == null) {
      synchronized (this) {
        ctx = nodeContext;
        if (ctx == null) {
          ctx =
              new RaftGroupServer.NodeContext(
                  thisNodeId,
                  thisNode,
                  config,
                  createPolicy(),
                  asyncClients,
                  syncClients,
                  corePool,
                  applyPool,
                  snapshotPool,
                  timerPool,
                  createListener());
          nodeContext = ctx;
        }
      }
    }
    return ctx;
  }

  private RequestPreprocessor preprocessor() {
    RequestPreprocessor p = preprocessor;
    if (p == null) {
      p = preprocessorFor(createPreprocessor());
      preprocessor = p;
    }
    return p;
  }

  // ---------------------------------------------------------------- hooks

  protected RaftConfig loadConfig(ConsensusConfig config) {
    return RaftConfigLoader.load(config);
  }

  protected String protocolName() {
    return "Raft";
  }

  protected ProtocolProfile createProfile(ConsensusGroupId gid) {
    return new RaftProfile();
  }

  protected RequestPreprocessor createPreprocessor() {
    return RequestPreprocessor.REQUEST_SEMANTICS;
  }

  protected ProtocolPolicy createPolicy() {
    ProtocolPolicy p = FidelityProfile.PRODUCTION.policy();
    p.preVote = config.election.enablePreVote;
    p.maxBatchEntries = config.replication.maxEntriesPerAppend;
    p.maxInflightRpcs = config.replication.maxInflightBatches;
    p.basicRetryWaitMs = config.replication.basicRetryWaitMs;
    p.maxRetryWaitMs = config.replication.maxRetryWaitMs;
    p.strictAsserts = config.strictAsserts;
    return p;
  }

  protected ProtocolListener createListener() {
    return ProtocolListener.NOOP;
  }

  // ------------------------------------------------------------- lifecycle

  @Override
  public synchronized void start() throws IOException {
    if (started) {
      return;
    }
    initAndRecover();
    try {
      service.start();
    } catch (StartupException e) {
      throw new IOException(e);
    }
    for (RaftGroupServer g : groups.values()) {
      g.start();
    }
    started = true;
    LOGGER.info("{} started on {} with {} groups", protocolName(), thisNode, groups.size());
  }

  private void initAndRecover() throws IOException {
    AtomicFiles.createDirectories(storageDir);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
      for (Path path : stream) {
        if (!Files.isDirectory(path)) {
          continue;
        }
        String[] items = path.getFileName().toString().split("_");
        if (items.length != 2) {
          continue;
        }
        ConsensusGroupId gid;
        try {
          gid =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
        } catch (RuntimeException e) {
          LOGGER.warn("skipping unrecognized directory {}", path);
          continue;
        }
        RaftGroupServer g = newGroup(gid, null);
        g.recover();
        groups.put(gid, g);
      }
    }
    if (correctPeerListBeforeStart != null) {
      for (ConsensusGroupId gid : new ArrayList<>(groups.keySet())) {
        List<Peer> correct = correctPeerListBeforeStart.get(gid);
        if (correct == null) {
          LOGGER.warn("{} is not in the correct peer list; removing it", gid);
          removeGroup(gid);
        }
      }
    }
  }

  private RaftGroupServer newGroup(ConsensusGroupId gid, Membership initial) throws IOException {
    return createGroup(
        gid,
        new File(buildPeerDir(storageDir, gid)),
        nodeContext(),
        registry.apply(gid),
        createProfile(gid),
        preprocessor(),
        initial);
  }

  @Override
  public synchronized void stop() throws IOException {
    if (!started) {
      return;
    }
    started = false;
    for (RaftGroupServer g : groups.values()) {
      g.stop();
    }
    service.stop();
    asyncClients.close();
    syncClients.close();
    timerPool.shutdownNow();
    corePool.shutdown();
    applyPool.shutdown();
    snapshotPool.shutdown();
    LOGGER.info("{} stopped", protocolName());
  }

  public static String buildPeerDir(File storageDir, ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }

  private RaftGroupServer group(ConsensusGroupId gid) throws ConsensusGroupNotExistException {
    RaftGroupServer g = groups.get(gid);
    if (g == null) {
      throw new ConsensusGroupNotExistException(gid);
    }
    return g;
  }

  // ------------------------------------------------------------------ write / read

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    if (config.pipelineRows) {
      RaftGroupServer g = group(groupId);
      if (!g.stateMachine().isReadOnly() && g.isLeader()) {
        waitUntilLeaderReady(g);
        if (g.isLeader() && g.isLeaderReady()) {
          // No row waits for an earlier row's commit; the leader forwards none of them.
          long deadline =
              System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.replication.writeTimeoutMs);
          return ConsensusWriteNormalizer.writePipelined(
              request, g::write, waiting -> awaitRow(waiting, deadline));
        }
      }
    }
    return ConsensusWriteNormalizer.write(request, child -> writeSingle(groupId, child));
  }

  /**
   * One row of a pipelined request; the rows share the request's deadline. A row this node can no
   * longer commit as leader is rejected (the client retries); any subset of the rows may apply.
   */
  private TSStatus awaitRow(CompletableFuture<TSStatus> waiting, long deadline) {
    try {
      return waiting.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof NotLeaderException) {
        return RpcUtils.getStatus(
            TSStatusCode.WRITE_PROCESS_REJECT, "leadership changed during the write");
      }
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, String.valueOf(e.getCause()));
    } catch (TimeoutException e) {
      // Abandon only the client's wait. An admitted entry can still commit and must be applied.
      waiting.cancel(false);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "consensus write timed out");
    } catch (InterruptedException e) {
      waiting.cancel(false);
      Thread.currentThread().interrupt();
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, "interrupted");
    }
  }

  private TSStatus writeSingle(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    RaftGroupServer g = group(groupId);
    if (g.stateMachine().isReadOnly()) {
      return RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    }
    if (g.isLeader()) {
      waitUntilLeaderReady(g);
      TSStatus local = writeLocally(g, request);
      if (local != null) {
        return local;
      }
    }
    int leader = g.leaderId();
    TEndPoint ep = leader >= 0 ? g.membership().endpointOf(leader) : null;
    if (ep == null || leader == thisNodeId) {
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, groupId + ": no known leader");
    }
    return forwarder.forward(
        groupId, ep, request.serializeToByteBuffer(), preprocessor().preprocess(request));
  }

  /** Null when this node turned out not to be the leader. */
  private TSStatus writeLocally(RaftGroupServer g, IConsensusRequest request) {
    CompletableFuture<TSStatus> waiting = g.write(request);
    try {
      return waiting.get(config.replication.writeTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof NotLeaderException) {
        return null;
      }
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, String.valueOf(e.getCause()));
    } catch (TimeoutException e) {
      // Abandon only the client's wait. An admitted entry can still commit and must be applied.
      waiting.cancel(false);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "consensus write timed out");
    } catch (InterruptedException e) {
      waiting.cancel(false);
      Thread.currentThread().interrupt();
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, "interrupted");
    }
  }

  private void waitUntilLeaderReady(RaftGroupServer g) {
    long deadline =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.replication.leaderReadyWaitMs);
    while (g.isLeader() && !g.isLeaderReady() && System.nanoTime() < deadline) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    return group(groupId).read(request, config.read.consistency);
  }

  // ---------------------------------------------------------------- groups

  @Override
  public synchronized void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    if (groups.containsKey(groupId)) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }
    Membership membership;
    if (peers == null || peers.isEmpty()) {
      membership = Membership.empty(); // learner until a CONFIG entry / snapshot tells it the group
    } else {
      Map<Integer, TEndPoint> eps = new HashMap<>();
      boolean self = false;
      for (Peer p : peers) {
        eps.put(p.getNodeId(), p.getEndpoint());
        self |= p.getNodeId() == thisNodeId;
      }
      if (!self) {
        throw new IllegalPeerEndpointException(thisNode, peers);
      }
      membership = Membership.ofVoters(eps);
    }
    try {
      RaftGroupServer g = newGroup(groupId, membership);
      g.recover();
      groups.put(groupId, g);
      if (started) {
        g.start();
      }
    } catch (IOException e) {
      throw new ConsensusException(e);
    }
  }

  @Override
  public synchronized void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    if (!groups.containsKey(groupId)) {
      throw new ConsensusGroupNotExistException(groupId);
    }
    removeGroup(groupId);
  }

  private void removeGroup(ConsensusGroupId gid) {
    RaftGroupServer g = groups.remove(gid);
    if (g != null) {
      g.stop();
      FileUtils.deleteFileOrDirectory(g.dir());
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    RaftGroupServer g = group(groupId);
    if (!g.isLeader()) {
      forwardAdmin(g, TAdminOp.ADD_PEER, peer);
      return;
    }
    addPeerAsLeader(g, peer.getNodeId(), peer.getEndpoint());
  }

  /** Raft §4.1 single-server change in three steps: learner, catch-up, promotion. */
  private void addPeerAsLeader(RaftGroupServer g, int nodeId, TEndPoint ep)
      throws ConsensusException {
    awaitConfig(
        g.configChange(Event.ConfigOp.ADD_LEARNER, nodeId, ep, null), "add learner N" + nodeId);
    long deadline =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.replication.catchUpTimeoutMs);
    while (g.isLeader()
        && g.followerMatchIndex(nodeId) < g.commitIndex() - config.replication.catchUpLagEntries) {
      if (System.nanoTime() > deadline) {
        throw new ConsensusException(
            "N"
                + nodeId
                + " did not catch up within "
                + config.replication.catchUpTimeoutMs
                + " ms");
      }
      sleep(50);
    }
    awaitConfig(g.configChange(Event.ConfigOp.PROMOTE, nodeId, ep, null), "promote N" + nodeId);
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    RaftGroupServer g = group(groupId);
    if (!g.isLeader()) {
      forwardAdmin(g, TAdminOp.REMOVE_PEER, peer);
      return;
    }
    if (peer.getNodeId() == thisNodeId) {
      // hand leadership to the most advanced voter first, then ask the new leader
      int best = -1;
      long bestMatch = -1;
      for (int v : g.membership().voters()) {
        if (v != thisNodeId && g.followerMatchIndex(v) > bestMatch) {
          best = v;
          bestMatch = g.followerMatchIndex(v);
        }
      }
      if (best < 0) {
        throw new ConsensusException("no other voter to hand leadership to");
      }
      transferLeader(groupId, new Peer(groupId, best, g.membership().endpointOf(best)));
      long deadline =
          System.nanoTime()
              + TimeUnit.MILLISECONDS.toNanos(config.election.leaderTransferTimeoutMs);
      while (g.leaderId() < 0 || g.leaderId() == thisNodeId) {
        if (System.nanoTime() > deadline) {
          throw new ConsensusException("no new leader after transferring leadership");
        }
        sleep(50);
      }
      forwardAdmin(g, TAdminOp.REMOVE_PEER, peer);
      return;
    }
    awaitConfig(
        g.configChange(Event.ConfigOp.REMOVE, peer.getNodeId(), peer.getEndpoint(), null),
        "remove N" + peer.getNodeId());
  }

  private void awaitConfig(CompletableFuture<Void> f, String what) throws ConsensusException {
    try {
      f.get(config.replication.configChangeTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw new ConsensusException(what + " failed: " + e.getCause(), e.getCause());
    } catch (TimeoutException e) {
      throw new ConsensusException(
          what + " was not committed within " + config.replication.configChangeTimeoutMs + " ms");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusException(what + " interrupted");
    }
  }

  private static void sleep(long ms) throws ConsensusException {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusException("interrupted");
    }
  }

  /** Ask the leader to run the change; follows one leader hint. */
  private void forwardAdmin(RaftGroupServer g, TAdminOp op, Peer peer) throws ConsensusException {
    forwardAdmin(g, op, peer == null ? Collections.emptyList() : Collections.singletonList(peer));
  }

  private void forwardAdmin(RaftGroupServer g, TAdminOp op, List<Peer> peers)
      throws ConsensusException {
    int leader = g.leaderId();
    TEndPoint ep = leader >= 0 ? g.membership().endpointOf(leader) : null;
    if (ep == null) {
      throw new ConsensusException(g.groupId() + ": no known leader to run " + op);
    }
    List<TPeerInfo> infos = new ArrayList<>();
    for (Peer p : peers) {
      infos.add(new TPeerInfo(p.getNodeId(), p.getEndpoint()));
    }
    TAdminReq req = new TAdminReq(g.groupId().convertToTConsensusGroupId(), op, infos);
    for (int attempt = 0; attempt < 2 && ep != null; attempt++) {
      TAdminRes res;
      try (SyncRaftServiceClient c = syncClients.borrowClient(ep)) {
        res = c.admin(req);
      } catch (Exception e) {
        throw new ConsensusException(op + " forwarded to " + ep + " failed", e);
      }
      if (res.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
      if (res.isSetLeaderEndPoint() && !res.getLeaderEndPoint().equals(ep)) {
        ep = res.getLeaderEndPoint();
        continue;
      }
      throw new ConsensusException(op + " rejected by " + ep + ": " + res.getStatus());
    }
    throw new ConsensusException(op + " could not reach the leader");
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(
      Map<ConsensusGroupId, List<Peer>> correctPeerList) {
    this.correctPeerListBeforeStart = correctPeerList;
  }

  @Override
  public synchronized void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    RaftGroupServer g = group(groupId);
    boolean self = false;
    for (Peer p : correctPeers) {
      self |= p.getNodeId() == thisNodeId;
    }
    if (!self) {
      LOGGER.info(
          "{}: this node is not in the correct peer list {}; removing the group",
          groupId,
          correctPeers);
      removeGroup(groupId);
      return;
    }
    Map<Integer, TEndPoint> eps = new HashMap<>();
    for (Peer p : correctPeers) {
      eps.put(p.getNodeId(), p.getEndpoint());
    }
    Membership target = Membership.ofVoters(eps);
    if (g.membership().voters().equals(target.voters()) && g.membership().learners().isEmpty()) {
      return; // already correct
    }
    LOGGER.info("{}: resetPeerList to {} (current {})", groupId, correctPeers, g.membership());
    if (g.isLeader()) {
      awaitConfig(g.configChange(Event.ConfigOp.RESET, -1, null, eps), "reset peers");
    } else if (g.leaderId() >= 0 && g.membership().endpointOf(g.leaderId()) != null) {
      forwardAdmin(g, TAdminOp.RESET_PEERS, correctPeers);
    } else {
      // no leader reachable (e.g. the whole group restarting): override locally
      awaitConfig(g.configChange(Event.ConfigOp.RESET, -1, null, eps), "reset peers locally");
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    RaftGroupServer g = group(groupId);
    if (!g.isLeader()) {
      throw new ConsensusException("not the leader of " + groupId);
    }
    try {
      g.transferLeadership(newLeader.getNodeId())
          .get(config.election.leaderTransferTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      g.cancelTransfer();
      throw new ConsensusException("leadership transfer to " + newLeader + " timed out");
    } catch (Exception e) {
      throw new ConsensusException(e);
    }
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    group(groupId).takeSnapshot(force);
  }

  // ----------------------------------------------------------------- views

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftGroupServer g = groups.get(groupId);
    return g != null && g.isLeader();
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    RaftGroupServer g = groups.get(groupId);
    return g == null ? 0 : g.term();
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    RaftGroupServer g = groups.get(groupId);
    return g != null && g.isLeaderReady();
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    RaftGroupServer g = groups.get(groupId);
    if (g == null || g.leaderId() < 0) {
      return null;
    }
    TEndPoint ep = g.membership().endpointOf(g.leaderId());
    return ep == null ? null : new Peer(groupId, g.leaderId(), ep);
  }

  @Override
  public int getReplicationNum(ConsensusGroupId groupId) {
    RaftGroupServer g = groups.get(groupId);
    return g == null ? 0 : g.membership().voters().size();
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(groups.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return buildPeerDir(storageDir, groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    RaftConfigLoader.load(consensusConfig, config);
  }

  public RaftGroupServer getGroup(ConsensusGroupId gid) {
    return groups.get(gid);
  }

  // --------------------------------------------------------- RaftRpcDispatcher

  @Override
  public CompletableFuture<Message> handle(ConsensusGroupId gid, int from, Message msg) {
    RaftGroupServer g = groups.get(gid);
    return g == null ? null : g.handle(from, msg);
  }

  @Override
  public TGroupStatusRes groupStatus(ConsensusGroupId gid) {
    RaftGroupServer g = groups.get(gid);
    return g == null ? null : g.status();
  }

  @Override
  public TGroupStatusRes readIndexStatus(ConsensusGroupId gid) throws ConsensusException {
    RaftGroupServer g = groups.get(gid);
    if (g == null) {
      throw new ConsensusGroupNotExistException(gid);
    }
    return g.readIndexStatus();
  }

  @Override
  public org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq decodeAppend(
      ConsensusGroupId gid, org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq request)
      throws IOException {
    RaftGroupServer group = groups.get(gid);
    if (group == null) {
      throw new IOException("Unknown group " + gid);
    }
    return group.decodeAppend(request);
  }

  @Override
  public EntryMetaCodec metaCodec(ConsensusGroupId gid) {
    RaftGroupServer g = groups.get(gid);
    return g == null ? PartitionedMeta.CODEC : g.metaCodec();
  }

  @Override
  public TForwardWriteRes forwardWrite(TForwardWriteReq req) {
    ConsensusGroupId gid = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId());
    RaftGroupServer g = groups.get(gid);
    TForwardWriteRes res = new TForwardWriteRes();
    if (g == null) {
      res.setStatus(RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST));
      return res;
    }
    if (!g.isLeader()) {
      res.setStatus(RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "not leader"));
      if (g.leaderId() >= 0) {
        res.setLeaderId(g.leaderId());
        TEndPoint ep = g.membership().endpointOf(g.leaderId());
        if (ep != null) {
          res.setLeaderEndPoint(ep);
        }
      }
      return res;
    }
    RequestMeta meta = forwardedMeta(req);
    waitUntilLeaderReady(g);
    TSStatus status = writeLocally(g, new ForwardedRequest(req.bufferForPayload().slice(), meta));
    res.setStatus(
        status == null
            ? RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "leadership lost")
            : status);
    return res;
  }

  @Override
  public TAdminRes admin(TAdminReq req) {
    ConsensusGroupId gid = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId());
    RaftGroupServer g = groups.get(gid);
    TAdminRes res = new TAdminRes();
    if (g == null) {
      res.setStatus(RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST));
      return res;
    }
    if (!g.isLeader()) {
      res.setStatus(RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "not leader"));
      if (g.leaderId() >= 0) {
        res.setLeaderId(g.leaderId());
        TEndPoint ep = g.membership().endpointOf(g.leaderId());
        if (ep != null) {
          res.setLeaderEndPoint(ep);
        }
      }
      return res;
    }
    try {
      List<TPeerInfo> peers = req.getPeers() == null ? Collections.emptyList() : req.getPeers();
      switch (req.getOp()) {
        case ADD_PEER:
          addPeerAsLeader(g, peers.get(0).getNodeId(), peers.get(0).getEndPoint());
          break;
        case REMOVE_PEER:
          awaitConfig(
              g.configChange(
                  Event.ConfigOp.REMOVE,
                  peers.get(0).getNodeId(),
                  peers.get(0).getEndPoint(),
                  null),
              "remove N" + peers.get(0).getNodeId());
          break;
        case TRANSFER_LEADER:
          transferLeader(gid, new Peer(gid, peers.get(0).getNodeId(), peers.get(0).getEndPoint()));
          break;
        case RESET_PEERS:
          Map<Integer, TEndPoint> eps = new HashMap<>();
          for (TPeerInfo p : peers) {
            eps.put(p.getNodeId(), p.getEndPoint());
          }
          awaitConfig(g.configChange(Event.ConfigOp.RESET, -1, null, eps), "reset peers");
          break;
        default:
          res.setStatus(RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
          return res;
      }
      res.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      res.setStatus(RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString()));
    }
    return res;
  }

  @Override
  public TInstallSnapshotMetaRes installSnapshotMeta(TInstallSnapshotMetaReq req) {
    RaftGroupServer g =
        groups.get(ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId()));
    if (g == null) {
      return new TInstallSnapshotMetaRes(
          0, RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST));
    }
    return new TInstallSnapshotMetaRes(g.term(), g.onInstallSnapshotMeta(req));
  }

  @Override
  public TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req) {
    RaftGroupServer g =
        groups.get(ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId()));
    if (g == null) {
      return new TSendSnapshotFragmentRes(
          RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST), 0);
    }
    return new TSendSnapshotFragmentRes(g.onSnapshotFragment(req), g.term());
  }

  @Override
  public TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req) {
    RaftGroupServer g =
        groups.get(ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId()));
    if (g == null) {
      return new TTriggerSnapshotLoadRes(
          RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST));
    }
    return new TTriggerSnapshotLoadRes(g.onSnapshotLoad(req));
  }

  @Override
  public TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(
      TCleanupTransferredSnapshotReq req) {
    RaftGroupServer g =
        groups.get(ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId()));
    if (g == null) {
      return new TCleanupTransferredSnapshotRes(
          RpcUtils.getStatus(TSStatusCode.CONSENSUS_GROUP_NOT_EXIST));
    }
    return new TCleanupTransferredSnapshotRes(g.onSnapshotCleanup(req.getSnapshotId()));
  }

  /** The metadata the forwarding node extracted ({@code WriteForwarder}), extension included. */
  static RequestMeta forwardedMeta(TForwardWriteReq req) {
    RequestMeta meta =
        req.isSetBarrier() && req.isBarrier()
            ? RequestMeta.OPAQUE
            : req.isSetTimestamp()
                ? RequestMeta.timed(
                    req.getTimestamp(),
                    req.isSetTimestampMax() ? req.getTimestampMax() : req.getTimestamp())
                : RequestMeta.OPAQUE;
    return req.isSetRequestExt() ? meta.withExt(req.getRequestExt()) : meta;
  }

  /** A forwarded write: payload plus the metadata the forwarding node already extracted. */
  private static final class ForwardedRequest extends ByteBufferConsensusRequest {
    final RequestMeta meta;

    ForwardedRequest(java.nio.ByteBuffer payload, RequestMeta meta) {
      super(payload);
      this.meta = meta;
    }
  }

  /** Preprocessor wrapper that honours forwarded metadata. */
  protected final RequestPreprocessor preprocessorFor(RequestPreprocessor inner) {
    return r -> r instanceof ForwardedRequest ? ((ForwardedRequest) r).meta : inner.preprocess(r);
  }

  protected static List<Peer> noPeers() {
    return Collections.emptyList();
  }

  protected RaftGroupServer createGroup(
      ConsensusGroupId gid,
      File dir,
      RaftGroupServer.NodeContext context,
      IStateMachine stateMachine,
      ProtocolProfile profile,
      RequestPreprocessor preprocessor,
      Membership initial)
      throws IOException {
    return new RaftGroupServer(gid, dir, context, stateMachine, profile, preprocessor, initial);
  }
}
