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

package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.exception.RatisRequestFailedException;
import org.apache.iotdb.consensus.exception.RatisUnderRecoveryException;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricSet;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricsManager;
import org.apache.iotdb.consensus.ratis.utils.RatisLogMonitor;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** A multi-raft consensus implementation based on Apache Ratis. */
class RatisConsensus implements IConsensus {

  private static final Logger logger = LoggerFactory.getLogger(RatisConsensus.class);

  /** the unique net communication endpoint */
  private final RaftPeer myself;

  private final RaftServer server;

  private final RaftProperties properties = new RaftProperties();
  private final RaftClientRpc clientRpc;

  private final IClientManager<RaftGroup, RatisClient> clientManager;

  private final Map<RaftGroupId, RaftGroup> lastSeen = new ConcurrentHashMap<>();

  private final ClientId localFakeId = ClientId.randomId();
  private final AtomicLong localFakeCallId = new AtomicLong(0);

  private static final int DEFAULT_PRIORITY = 0;
  private static final int LEADER_PRIORITY = 1;

  private static final int DEFAULT_WAIT_LEADER_READY_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(20);

  private final ScheduledExecutorService diskGuardian;
  private final long triggerSnapshotThreshold;

  private final RatisConfig config;

  private final RatisLogMonitor monitor = new RatisLogMonitor();

  private final RatisMetricSet ratisMetricSet;
  private final TConsensusGroupType consensusGroupType;

  private final ConcurrentHashMap<ConsensusGroupId, AtomicBoolean> canServeStaleRead =
      new ConcurrentHashMap<>();

  public RatisConsensus(ConsensusConfig config, IStateMachine.Registry registry)
      throws IOException {
    myself =
        Utils.fromNodeInfoAndPriorityToRaftPeer(
            config.getThisNodeId(), config.getThisNodeEndPoint(), DEFAULT_PRIORITY);

    RaftServerConfigKeys.setStorageDir(
        properties, Collections.singletonList(new File(config.getStorageDir())));
    GrpcConfigKeys.Server.setPort(properties, config.getThisNodeEndPoint().getPort());

    Utils.initRatisConfig(properties, config.getRatisConfig());
    this.config = config.getRatisConfig();
    this.consensusGroupType = config.getConsensusGroupType();
    this.ratisMetricSet = new RatisMetricSet();

    this.triggerSnapshotThreshold = this.config.getImpl().getTriggerSnapshotFileSize();
    diskGuardian =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.RATIS_BG_DISK_GUARDIAN.getName());

    clientManager =
        new IClientManager.Factory<RaftGroup, RatisClient>()
            .createClientManager(new RatisClientPoolFactory());

    clientRpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);

    server =
        RaftServer.newBuilder()
            .setServerId(myself.getId())
            .setProperties(properties)
            .setStateMachineRegistry(
                raftGroupId ->
                    new ApplicationStateMachineProxy(
                        registry.apply(Utils.fromRaftGroupIdToConsensusGroupId(raftGroupId)),
                        raftGroupId,
                        canServeStaleRead))
            .build();
  }

  @Override
  public synchronized void start() throws IOException {
    MetricService.getInstance().addMetricSet(this.ratisMetricSet);
    server.start();
    startSnapshotGuardian();
  }

  @Override
  public synchronized void stop() throws IOException {
    diskGuardian.shutdown();
    try {
      diskGuardian.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("{}: interrupted when shutting down add Executor with exception {}", this, e);
      Thread.currentThread().interrupt();
    } finally {
      clientManager.close();
      server.close();
      MetricService.getInstance().removeMetricSet(this.ratisMetricSet);
    }
  }

  private boolean shouldRetry(RaftClientReply reply) {
    // currently, we only retry when ResourceUnavailableException is caught
    return !reply.isSuccess() && (reply.getException() instanceof ResourceUnavailableException);
  }

  /** launch a consensus write with retry mechanism */
  private RaftClientReply writeWithRetry(CheckedSupplier<RaftClientReply, IOException> caller)
      throws IOException {

    final int maxRetryTimes = config.getImpl().getRetryTimesMax();
    final long waitMillis = config.getImpl().getRetryWaitMillis();

    int retry = 0;
    RaftClientReply reply = null;
    while (retry < maxRetryTimes) {
      retry++;

      reply = caller.get();
      if (!shouldRetry(reply)) {
        return reply;
      }
      logger.debug("{} sending write request with retry = {} and reply = {}", this, retry, reply);

      try {
        Thread.sleep(waitMillis);
      } catch (InterruptedException e) {
        logger.warn("{} retry write sleep is interrupted: {}", this, e);
        Thread.currentThread().interrupt();
      }
    }
    if (reply == null) {
      return RaftClientReply.newBuilder()
          .setSuccess(false)
          .setException(
              new RaftException("null reply received in writeWithRetry for request " + caller))
          .build();
    }
    return reply;
  }

  private RaftClientReply writeLocallyWithRetry(RaftClientRequest request) throws IOException {
    return writeWithRetry(() -> server.submitClientRequest(request));
  }

  private RaftClientReply writeRemotelyWithRetry(RatisClient client, Message message)
      throws IOException {
    return writeWithRetry(() -> client.getRaftClient().io().send(message));
  }

  /**
   * write will first send request to local server using local method call. If local server is not
   * leader, it will use RaftClient to send RPC to read leader
   */
  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    // pre-condition: group exists and myself server serves this group
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    // current Peer is group leader and in ReadOnly State
    if (isLeader(groupId) && Utils.rejectWrite()) {
      try {
        forceStepDownLeader(raftGroup);
      } catch (Exception e) {
        logger.warn("leader {} read only, force step down failed due to {}", myself, e);
      }
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    }

    // serialize request into Message
    Message message = new RequestMessage(request);

    // 1. first try the local server
    RaftClientRequest clientRequest =
        buildRawRequest(raftGroupId, message, RaftClientRequest.writeRequestType());

    RaftPeer suggestedLeader = null;
    if (isLeader(groupId) && waitUntilLeaderReady(raftGroupId)) {
      try (AutoCloseable ignored =
          RatisMetricsManager.getInstance().startWriteLocallyTimer(consensusGroupType)) {
        RaftClientReply localServerReply = writeLocallyWithRetry(clientRequest);
        if (localServerReply.isSuccess()) {
          ResponseMessage responseMessage = (ResponseMessage) localServerReply.getMessage();
          return (TSStatus) responseMessage.getContentHolder();
        }
        NotLeaderException ex = localServerReply.getNotLeaderException();
        if (ex != null) {
          suggestedLeader = ex.getSuggestedLeader();
        }
      } catch (Exception e) {
        throw new RatisRequestFailedException(e);
      }
    }

    // 2. try raft client
    TSStatus writeResult;
    try (AutoCloseable ignored =
            RatisMetricsManager.getInstance().startWriteRemotelyTimer(consensusGroupType);
        RatisClient client = getRaftClient(raftGroup)) {
      RaftClientReply reply = writeRemotelyWithRetry(client, message);
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
      writeResult = Utils.deserializeFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    }

    if (suggestedLeader != null) {
      TEndPoint leaderEndPoint = Utils.fromRaftPeerAddressToTEndPoint(suggestedLeader.getAddress());
      writeResult.setRedirectNode(new TEndPoint(leaderEndPoint.getIp(), leaderEndPoint.getPort()));
    }
    return writeResult;
  }

  /**
   * Read directly from LOCAL COPY notice, although we do some optimizations to try to ensure
   * linearizable (such as enforcing linearizable reads when the leader transfers), linearizable can
   * be violated in some extreme cases.
   */
  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    if (group == null || !group.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    final boolean isLinearizableRead =
        !canServeStaleRead.computeIfAbsent(groupId, id -> new AtomicBoolean(false)).get();

    RaftClientReply reply;
    try {
      reply = doRead(raftGroupId, request, isLinearizableRead);
      // allow stale read if current linearizable read returns successfully
      if (isLinearizableRead) {
        canServeStaleRead.get(groupId).set(true);
      }
    } catch (ReadException | ReadIndexException e) {
      if (isLinearizableRead) {
        // linearizable read failed. the RaftServer is recovering from Raft Log and cannot serve
        // read requests.
        throw new RatisUnderRecoveryException(e);
      } else {
        throw new RatisRequestFailedException(e);
      }
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    }
    Message ret = reply.getMessage();
    ResponseMessage readResponseMessage = (ResponseMessage) ret;
    return (DataSet) readResponseMessage.getContentHolder();
  }

  /** return a success raft client reply or throw an Exception */
  private RaftClientReply doRead(
      RaftGroupId gid, IConsensusRequest readRequest, boolean linearizable) throws Exception {
    final RaftClientRequest.Type readType =
        linearizable
            ? RaftClientRequest.readRequestType()
            : RaftClientRequest.staleReadRequestType(-1);
    final RequestMessage requestMessage = new RequestMessage(readRequest);
    final RaftClientRequest request = buildRawRequest(gid, requestMessage, readType);

    RaftClientReply reply;
    try (AutoCloseable ignored =
        RatisMetricsManager.getInstance().startReadTimer(consensusGroupType)) {
      reply = server.submitClientRequest(request);
    }

    // rethrow the exception if the reply is not successful
    if (!reply.isSuccess()) {
      throw reply.getException();
    }

    return reply;
  }

  /**
   * Add this IConsensus Peer into ConsensusGroup(groupId, peers) Caller's responsibility to call
   * addConsensusGroup to every peer of this group and ensure the group is all up
   *
   * <p>underlying Ratis will 1. initialize a RaftServer instance 2. call GroupManagementApi to
   * register self to the RaftGroup
   */
  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    RaftGroup group = buildRaftGroup(groupId, peers);
    RaftGroup clientGroup =
        group.getPeers().isEmpty() ? RaftGroup.valueOf(group.getGroupId(), myself) : group;
    try (RatisClient client = getRaftClient(clientGroup)) {
      RaftClientReply reply =
          client.getRaftClient().getGroupManagementApi(myself.getId()).add(group);
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (AlreadyExistsException e) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    }
  }

  /**
   * Remove this IConsensus Peer out of ConsensusGroup(groupId, peers) Caller's responsibility to
   * call removeConsensusGroup to every peer of this group and ensure the group is fully removed
   *
   * <p>underlying Ratis will 1. call GroupManagementApi to unregister self off the RaftGroup 2.
   * clean up
   */
  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);

    // send remove group to myself
    RaftClientReply reply;
    try {
      reply =
          server.groupManagement(
              GroupManagementRequest.newRemove(
                  localFakeId,
                  myself.getId(),
                  localFakeCallId.incrementAndGet(),
                  raftGroupId,
                  true,
                  false));
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (GroupMismatchException e) {
      throw new ConsensusGroupNotExistException(groupId);
    } catch (IOException e) {
      throw new RatisRequestFailedException(e);
    }
  }

  /**
   * Add a new IConsensus Peer into ConsensusGroup with groupId
   *
   * <p>underlying Ratis will 1. call the AdminApi to notify group leader of this configuration
   * change
   */
  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);

    RaftGroup group = getGroupInfo(raftGroupId);
    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    RaftPeer peerToAdd = Utils.fromPeerAndPriorityToRaftPeer(peer, DEFAULT_PRIORITY);
    // pre-condition: peer not in this group
    if (group.getPeers().contains(peerToAdd)) {
      throw new PeerAlreadyInConsensusGroupException(groupId, peer);
    }

    List<RaftPeer> newConfig = new ArrayList<>(group.getPeers());
    newConfig.add(peerToAdd);

    sendReconfiguration(RaftGroup.valueOf(raftGroupId, newConfig));
  }

  /**
   * Remove IConsensus Peer from ConsensusGroup with groupId
   *
   * <p>underlying Ratis will 1. call the AdminApi to notify group leader of this configuration
   * change
   */
  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    RaftPeer peerToRemove = Utils.fromPeerAndPriorityToRaftPeer(peer, DEFAULT_PRIORITY);

    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }
    // pre-condition: peer is a member of groupId
    if (!group.getPeers().contains(peerToRemove)) {
      throw new PeerNotInConsensusGroupException(groupId, myself.getAddress());
    }

    // update group peer information
    List<RaftPeer> newConfig =
        group.getPeers().stream()
            .filter(raftPeer -> !raftPeer.equals(peerToRemove))
            .collect(Collectors.toList());

    sendReconfiguration(RaftGroup.valueOf(raftGroupId, newConfig));
  }

  /**
   * NOTICE: transferLeader *does not guarantee* the leader be transferred to newLeader.
   * transferLeader is implemented by 1. modify peer priority 2. ask current leader to step down
   *
   * <p>1. call setConfiguration to upgrade newLeader's priority to 1 and degrade all follower peers
   * to 0. By default, Ratis gives every Raft Peer same priority 0. Ratis does not allow a peer with
   * priority <= currentLeader.priority to becomes the leader, so we have to upgrade leader's
   * priority to 1
   *
   * <p>2. call transferLeadership to force current leader to step down and raise a new round of
   * election. In this election, the newLeader peer with priority 1 is guaranteed to be elected.
   */
  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {

    // first fetch the newest information
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup raftGroup =
        Optional.ofNullable(getGroupInfo(raftGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    RaftPeer newRaftLeader = Utils.fromPeerAndPriorityToRaftPeer(newLeader, LEADER_PRIORITY);

    ArrayList<RaftPeer> newConfiguration = new ArrayList<>();
    for (RaftPeer raftPeer : raftGroup.getPeers()) {
      if (raftPeer.getId().equals(newRaftLeader.getId())) {
        newConfiguration.add(newRaftLeader);
      } else {
        // degrade every other peer to default priority
        newConfiguration.add(
            Utils.fromNodeInfoAndPriorityToRaftPeer(
                Utils.fromRaftPeerIdToNodeId(raftPeer.getId()),
                Utils.fromRaftPeerAddressToTEndPoint(raftPeer.getAddress()),
                DEFAULT_PRIORITY));
      }
    }

    RaftClientReply reply;
    try (RatisClient client = getRaftClient(raftGroup)) {
      RaftClientReply configChangeReply =
          client.getRaftClient().admin().setConfiguration(newConfiguration);
      if (!configChangeReply.isSuccess()) {
        throw new RatisRequestFailedException(configChangeReply.getException());
      }

      reply = transferLeader(raftGroup, newRaftLeader);
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    }
  }

  private void forceStepDownLeader(RaftGroup group) throws Exception {
    // when newLeaderPeerId == null, ratis forces current leader to step down and raise new
    // election
    transferLeader(group, null);
  }

  private RaftClientReply transferLeader(RaftGroup group, RaftPeer newLeader) throws Exception {
    try (RatisClient client = getRaftClient(group)) {
      return client
          .getRaftClient()
          .admin()
          .transferLeadership(newLeader != null ? newLeader.getId() : null, 10000);
    }
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    try {
      return server.getDivision(raftGroupId).getInfo().isLeader();
    } catch (IOException exception) {
      // if the read fails, simply return not leader
      logger.info("isLeader request failed with exception: ", exception);
      return false;
    }
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    try {
      return server.getDivision(raftGroupId).getInfo().isLeaderReady();
    } catch (IOException exception) {
      // if the read fails, simply return not ready
      logger.info("isLeaderReady request failed with exception: ", exception);
      return false;
    }
  }

  private boolean waitUntilLeaderReady(RaftGroupId groupId) {
    DivisionInfo divisionInfo;
    try {
      divisionInfo = server.getDivision(groupId).getInfo();
    } catch (IOException e) {
      // if the read fails, simply return not leader
      logger.info("isLeaderReady checking failed with exception: ", e);
      return false;
    }
    long startTime = System.currentTimeMillis();
    try {
      while (divisionInfo.isLeader() && !divisionInfo.isLeaderReady()) {
        Thread.sleep(10);
        long consumedTime = System.currentTimeMillis() - startTime;
        if (consumedTime >= DEFAULT_WAIT_LEADER_READY_TIMEOUT) {
          logger.warn("{}: leader is still not ready after {}ms", groupId, consumedTime);
          return false;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Unexpected interruption when waitUntilLeaderReady", e);
      return false;
    }
    return divisionInfo.isLeader();
  }

  /**
   * returns the known leader to the given group. NOTICE: if the local peer isn't a member of given
   * group, getLeader will return null.
   *
   * @return null if local peer isn't in group, otherwise group leader.
   */
  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftPeerId leaderId;

    try {
      leaderId = server.getDivision(raftGroupId).getInfo().getLeaderId();
    } catch (IOException e) {
      logger.warn("fetch division info for group " + groupId + " failed due to: ", e);
      return null;
    }
    if (leaderId == null) {
      return null;
    }
    int nodeId = Utils.fromRaftPeerIdToNodeId(leaderId);
    return new Peer(groupId, nodeId, null);
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    List<ConsensusGroupId> ids = new ArrayList<>();
    server
        .getGroupIds()
        .forEach(groupId -> ids.add(Utils.fromRaftGroupIdToConsensusGroupId(groupId)));
    return ids;
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId) throws ConsensusException {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup groupInfo = getGroupInfo(raftGroupId);
    if (groupInfo == null || !groupInfo.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    // TODO tuning snapshot create timeout
    SnapshotManagementRequest request =
        SnapshotManagementRequest.newCreate(
            localFakeId, myself.getId(), raftGroupId, localFakeCallId.incrementAndGet(), 30000);

    RaftClientReply reply;
    try {
      reply = server.snapshotManagement(request);
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (IOException ioException) {
      throw new RatisRequestFailedException(ioException);
    }
  }

  private void triggerSnapshotByCustomize() {

    for (RaftGroupId raftGroupId : server.getGroupIds()) {
      File currentDir;

      try {
        currentDir =
            server.getDivision(raftGroupId).getRaftStorage().getStorageDir().getCurrentDir();
      } catch (IOException e) {
        logger.warn("{}: get division {} failed: ", this, raftGroupId, e);
        continue;
      }

      final long currentDirLength = monitor.updateAndGetDirectorySize(currentDir);

      if (currentDirLength >= triggerSnapshotThreshold) {
        final int filesCount = monitor.getFilesUnder(currentDir).size();
        logger.info(
            "{}: take snapshot for region {}, current dir size {}, {} files to be purged",
            this,
            raftGroupId,
            currentDirLength,
            filesCount);

        try {
          triggerSnapshot(Utils.fromRaftGroupIdToConsensusGroupId(raftGroupId));
          logger.info("Raft group {} took snapshot successfully", raftGroupId);
        } catch (ConsensusException e) {
          logger.warn("Raft group {} failed to take snapshot due to", raftGroupId, e);
        }
      }
    }
  }

  private void startSnapshotGuardian() {
    final long delay = config.getImpl().getTriggerSnapshotTime();
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        diskGuardian, this::triggerSnapshotByCustomize, 0, delay, TimeUnit.SECONDS);
  }

  private RaftClientRequest buildRawRequest(
      RaftGroupId groupId, Message message, RaftClientRequest.Type type) {
    return RaftClientRequest.newBuilder()
        .setServerId(server.getId())
        .setClientId(localFakeId)
        .setCallId(localFakeCallId.incrementAndGet())
        .setGroupId(groupId)
        .setType(type)
        .setMessage(message)
        .build();
  }

  private RaftGroup getGroupInfo(RaftGroupId raftGroupId) {
    RaftGroup raftGroup = null;
    try {
      raftGroup = server.getDivision(raftGroupId).getGroup();
      RaftGroup lastSeenGroup = lastSeen.getOrDefault(raftGroupId, null);
      if (lastSeenGroup != null && !lastSeenGroup.equals(raftGroup)) {
        // delete the pooled raft-client of the out-dated group and cache the latest
        clientManager.clear(lastSeenGroup);
        lastSeen.put(raftGroupId, raftGroup);
      }
    } catch (IOException e) {
      logger.debug("get group {} failed ", raftGroupId, e);
    }
    return raftGroup;
  }

  private RaftGroup buildRaftGroup(ConsensusGroupId groupId, List<Peer> peers) {
    return RaftGroup.valueOf(
        Utils.fromConsensusGroupIdToRaftGroupId(groupId),
        Utils.fromPeersAndPriorityToRaftPeers(peers, DEFAULT_PRIORITY));
  }

  private RatisClient getRaftClient(RaftGroup group) throws ClientManagerException {
    try {
      return clientManager.borrowClient(group);
    } catch (ClientManagerException e) {
      logger.error(String.format("Borrow client from pool for group %s failed.", group), e);
      // rethrow the exception
      throw e;
    }
  }

  private RaftClientReply sendReconfiguration(RaftGroup newGroupConf)
      throws RatisRequestFailedException {
    // notify the group leader of configuration change
    RaftClientReply reply;
    try (RatisClient client = getRaftClient(newGroupConf)) {
      reply =
          client.getRaftClient().admin().setConfiguration(new ArrayList<>(newGroupConf.getPeers()));
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    }
    return reply;
  }

  @TestOnly
  public RaftServer getServer() {
    return server;
  }

  @TestOnly
  public void allowStaleRead(ConsensusGroupId consensusGroupId) {
    canServeStaleRead.computeIfAbsent(consensusGroupId, id -> new AtomicBoolean(false)).set(true);
  }

  private class RatisClientPoolFactory implements IClientPoolFactory<RaftGroup, RatisClient> {

    @Override
    public KeyedObjectPool<RaftGroup, RatisClient> createClientPool(
        ClientManager<RaftGroup, RatisClient> manager) {
      GenericKeyedObjectPool<RaftGroup, RatisClient> clientPool =
          new GenericKeyedObjectPool<>(
              new RatisClient.Factory(manager, properties, clientRpc, config.getClient()),
              new ClientPoolProperty.Builder<RatisClient>()
                  .setCoreClientNumForEachNode(config.getClient().getCoreClientNumForEachNode())
                  .setMaxClientNumForEachNode(config.getClient().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
