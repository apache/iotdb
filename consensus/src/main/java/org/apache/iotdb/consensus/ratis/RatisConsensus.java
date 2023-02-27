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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.Utils.MemorizedFileSizeCalc;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.NodeReadOnlyException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.exception.RatisRequestFailedException;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricSet;

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
import org.apache.ratis.protocol.exceptions.NotLeaderException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** A multi-raft consensus implementation based on Apache Ratis. */
class RatisConsensus implements IConsensus {

  private final Logger logger = LoggerFactory.getLogger(RatisConsensus.class);

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

  /** TODO make it configurable */
  private static final int DEFAULT_WAIT_LEADER_READY_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(20);

  private final ExecutorService addExecutor;
  private final ScheduledExecutorService diskGuardian;

  private final RatisConfig config;

  private final ConcurrentHashMap<File, MemorizedFileSizeCalc> calcMap = new ConcurrentHashMap<>();

  private final RatisMetricSet ratisMetricSet;

  public RatisConsensus(ConsensusConfig config, IStateMachine.Registry registry)
      throws IOException {
    myself =
        Utils.fromNodeInfoAndPriorityToRaftPeer(
            config.getThisNodeId(), config.getThisNodeEndPoint(), DEFAULT_PRIORITY);

    RaftServerConfigKeys.setStorageDir(
        properties, Collections.singletonList(new File(config.getStorageDir())));
    GrpcConfigKeys.Server.setPort(properties, config.getThisNodeEndPoint().getPort());

    addExecutor = IoTDBThreadPoolFactory.newCachedThreadPool("ratis-add");
    diskGuardian =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("ratis-bg-disk-guardian");

    Utils.initRatisConfig(properties, config.getRatisConfig());
    this.config = config.getRatisConfig();

    this.ratisMetricSet = new RatisMetricSet(config.getConsensusGroupType());

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
                        raftGroupId))
            .build();
  }

  @Override
  public void start() throws IOException {
    MetricService.getInstance().addMetricSet(this.ratisMetricSet);
    server.start();
    startSnapshotGuardian();
  }

  @Override
  public void stop() throws IOException {
    addExecutor.shutdown();
    diskGuardian.shutdown();
    try {
      addExecutor.awaitTermination(5, TimeUnit.SECONDS);
      diskGuardian.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("{}: interrupted when shutting down add Executor with exception {}", this, e);
      Thread.currentThread().interrupt();
    } finally {
      clientManager.close();
      server.close();
    }
    MetricService.getInstance().removeMetricSet(this.ratisMetricSet);
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
   * write will first send request to local server use method call if local server is not leader, it
   * will use RaftClient to send RPC to read leader
   */
  @Override
  public ConsensusWriteResponse write(
      ConsensusGroupId consensusGroupId, IConsensusRequest IConsensusRequest) {

    // pre-condition: group exists and myself server serves this group
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(consensusGroupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      return failedWrite(new ConsensusGroupNotExistException(consensusGroupId));
    }

    // current Peer is group leader and in ReadOnly State
    if (isLeader(consensusGroupId) && CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      try {
        forceStepDownLeader(raftGroup);
      } catch (Exception e) {
        logger.warn("leader {} read only, force step down failed due to {}", myself, e);
      }
      return failedWrite(new NodeReadOnlyException(myself));
    }

    // serialize request into Message
    Message message = new RequestMessage(IConsensusRequest);

    // 1. first try the local server
    RaftClientRequest clientRequest =
        buildRawRequest(raftGroupId, message, RaftClientRequest.writeRequestType());
    RaftClientReply localServerReply;
    RaftPeer suggestedLeader = null;
    if (isLeader(consensusGroupId) && waitUntilLeaderReady(raftGroupId)) {
      try {
        localServerReply = writeLocallyWithRetry(clientRequest);
        if (localServerReply.isSuccess()) {
          ResponseMessage responseMessage = (ResponseMessage) localServerReply.getMessage();
          TSStatus writeStatus = (TSStatus) responseMessage.getContentHolder();
          return ConsensusWriteResponse.newBuilder().setStatus(writeStatus).build();
        }
        NotLeaderException ex = localServerReply.getNotLeaderException();
        if (ex != null) { // local server is not leader
          suggestedLeader = ex.getSuggestedLeader();
        }
      } catch (IOException e) {
        return failedWrite(new RatisRequestFailedException(e));
      }
    }

    // 2. try raft client
    TSStatus writeResult;
    RatisClient client = null;
    try {
      client = getRaftClient(raftGroup);
      RaftClientReply reply = writeRemotelyWithRetry(client, message);
      if (!reply.isSuccess()) {
        return failedWrite(new RatisRequestFailedException(reply.getException()));
      }
      writeResult = Utils.deserializeFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    } catch (Exception e) {
      return failedWrite(new RatisRequestFailedException(e));
    } finally {
      if (client != null) {
        client.returnSelf();
      }
    }

    if (suggestedLeader != null) {
      TEndPoint leaderEndPoint = Utils.fromRaftPeerAddressToTEndPoint(suggestedLeader.getAddress());
      writeResult.setRedirectNode(new TEndPoint(leaderEndPoint.getIp(), leaderEndPoint.getPort()));
    }

    return ConsensusWriteResponse.newBuilder().setStatus(writeResult).build();
  }

  /** Read directly from LOCAL COPY notice: May read stale data (not linearizable) */
  @Override
  public ConsensusReadResponse read(
      ConsensusGroupId consensusGroupId, IConsensusRequest IConsensusRequest) {
    RaftGroupId groupId = Utils.fromConsensusGroupIdToRaftGroupId(consensusGroupId);
    RaftGroup group = getGroupInfo(groupId);
    if (group == null || !group.getPeers().contains(myself)) {
      return failedRead(new ConsensusGroupNotExistException(consensusGroupId));
    }

    RaftClientReply reply;
    try {
      RequestMessage message = new RequestMessage(IConsensusRequest);
      RaftClientRequest clientRequest =
          buildRawRequest(groupId, message, RaftClientRequest.staleReadRequestType(-1));
      reply = server.submitClientRequest(clientRequest);
      if (!reply.isSuccess()) {
        return failedRead(new RatisRequestFailedException(reply.getException()));
      }
    } catch (IOException e) {
      return failedRead(new RatisRequestFailedException(e));
    }

    Message ret = reply.getMessage();
    ResponseMessage readResponseMessage = (ResponseMessage) ret;
    DataSet dataSet = (DataSet) readResponseMessage.getContentHolder();

    return ConsensusReadResponse.newBuilder().setDataSet(dataSet).build();
  }

  /**
   * Add this IConsensus Peer into ConsensusGroup(groupId, peers) Caller's responsibility to call
   * addConsensusGroup to every peer of this group and ensure the group is all up
   *
   * <p>underlying Ratis will 1. initialize a RaftServer instance 2. call GroupManagementApi to
   * register self to the RaftGroup
   */
  @Override
  public ConsensusGenericResponse createPeer(ConsensusGroupId groupId, List<Peer> peers) {
    RaftGroup group = buildRaftGroup(groupId, peers);
    // add RaftPeer myself to this RaftGroup
    return addNewGroupToServer(group, myself);
  }

  private ConsensusGenericResponse addNewGroupToServer(RaftGroup group, RaftPeer server) {
    RaftClientReply reply;
    RatisClient client = null;
    try {
      if (group.getPeers().isEmpty()) {
        client = getRaftClient(RaftGroup.valueOf(group.getGroupId(), server));
      } else {
        client = getRaftClient(group);
      }
      reply = client.getRaftClient().getGroupManagementApi(server.getId()).add(group);
      if (!reply.isSuccess()) {
        return failed(new RatisRequestFailedException(reply.getException()));
      }
    } catch (Exception e) {
      return failed(new RatisRequestFailedException(e));
    } finally {
      if (client != null) {
        client.returnSelf();
      }
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  /**
   * Remove this IConsensus Peer out of ConsensusGroup(groupId, peers) Caller's responsibility to
   * call removeConsensusGroup to every peer of this group and ensure the group is fully removed
   *
   * <p>underlying Ratis will 1. call GroupManagementApi to unregister self off the RaftGroup 2.
   * clean up
   */
  @Override
  public ConsensusGenericResponse deletePeer(ConsensusGroupId groupId) {
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
        return failed(new RatisRequestFailedException(reply.getException()));
      }
    } catch (IOException e) {
      return failed(new RatisRequestFailedException(e));
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  /**
   * Add a new IConsensus Peer into ConsensusGroup with groupId
   *
   * <p>underlying Ratis will 1. call the AdminApi to notify group leader of this configuration
   * change
   */
  @Override
  public ConsensusGenericResponse addPeer(ConsensusGroupId groupId, Peer peer) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    RaftPeer peerToAdd = Utils.fromPeerAndPriorityToRaftPeer(peer, DEFAULT_PRIORITY);

    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

    // pre-condition: peer not in this group
    if (group.getPeers().contains(peerToAdd)) {
      return failed(new PeerAlreadyInConsensusGroupException(groupId, peer));
    }

    List<RaftPeer> newConfig = new ArrayList<>(group.getPeers());
    newConfig.add(peerToAdd);

    RaftClientReply reply;
    try {
      reply = sendReconfiguration(RaftGroup.valueOf(raftGroupId, newConfig));
    } catch (RatisRequestFailedException e) {
      return failed(e);
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  /**
   * Remove IConsensus Peer from ConsensusGroup with groupId
   *
   * <p>underlying Ratis will 1. call the AdminApi to notify group leader of this configuration
   * change
   */
  @Override
  public ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    RaftPeer peerToRemove = Utils.fromPeerAndPriorityToRaftPeer(peer, DEFAULT_PRIORITY);

    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }
    // pre-condition: peer is a member of groupId
    if (!group.getPeers().contains(peerToRemove)) {
      return failed(new PeerNotInConsensusGroupException(groupId, myself));
    }

    // update group peer information
    List<RaftPeer> newConfig =
        group.getPeers().stream()
            .filter(raftPeer -> !raftPeer.equals(peerToRemove))
            .collect(Collectors.toList());

    RaftClientReply reply;
    try {
      reply = sendReconfiguration(RaftGroup.valueOf(raftGroupId, newConfig));
    } catch (RatisRequestFailedException e) {
      return failed(e);
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  @Override
  public ConsensusGenericResponse updatePeer(ConsensusGroupId groupId, Peer oldPeer, Peer newPeer) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers) {
    RaftGroup raftGroup = buildRaftGroup(groupId, newPeers);

    // pre-conditions: myself in this group
    if (!raftGroup.getPeers().contains(myself)) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

    // add RaftPeer myself to this RaftGroup
    RaftClientReply reply;
    try {
      reply = sendReconfiguration(raftGroup);
    } catch (RatisRequestFailedException e) {
      return failed(e);
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
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
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {

    // first fetch the newest information

    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);

    if (raftGroup == null) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

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
    RatisClient client = null;
    try {
      client = getRaftClient(raftGroup);
      RaftClientReply configChangeReply =
          client.getRaftClient().admin().setConfiguration(newConfiguration);
      if (!configChangeReply.isSuccess()) {
        return failed(new RatisRequestFailedException(configChangeReply.getException()));
      }

      reply = transferLeader(raftGroup, newRaftLeader);
      if (!reply.isSuccess()) {
        return failed(new RatisRequestFailedException(reply.getException()));
      }
    } catch (Exception e) {
      return failed(new RatisRequestFailedException(e));
    } finally {
      if (client != null) {
        client.returnSelf();
      }
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  private void forceStepDownLeader(RaftGroup group) throws ClientManagerException, IOException {
    // when newLeaderPeerId == null, ratis forces current leader to step down and raise new
    // election
    transferLeader(group, null);
  }

  private RaftClientReply transferLeader(RaftGroup group, RaftPeer newLeader)
      throws ClientManagerException, IOException {
    RatisClient client = null;
    try {
      client = getRaftClient(group);
      // TODO tuning for timeoutMs
      return client
          .getRaftClient()
          .admin()
          .transferLeadership(newLeader != null ? newLeader.getId() : null, 10000);
    } finally {
      if (client != null) {
        client.returnSelf();
      }
    }
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);

    boolean isLeader;
    try {
      isLeader = server.getDivision(raftGroupId).getInfo().isLeader();
    } catch (IOException exception) {
      // if the query fails, simply return not leader
      logger.info("isLeader request failed with exception: ", exception);
      isLeader = false;
    }
    return isLeader;
  }

  private boolean waitUntilLeaderReady(RaftGroupId groupId) {
    DivisionInfo divisionInfo;
    try {
      divisionInfo = server.getDivision(groupId).getInfo();
    } catch (IOException e) {
      // if the query fails, simply return not leader
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
      logger.warn("Unexpected interruption", e);
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
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    RaftGroup groupInfo = getGroupInfo(raftGroupId);

    if (groupInfo == null || !groupInfo.getPeers().contains(myself)) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

    // TODO tuning snapshot create timeout
    SnapshotManagementRequest request =
        SnapshotManagementRequest.newCreate(
            localFakeId, myself.getId(), raftGroupId, localFakeCallId.incrementAndGet(), 30000);

    RaftClientReply reply;
    try {
      reply = server.snapshotManagement(request);
      if (!reply.isSuccess()) {
        return failed(new RatisRequestFailedException(reply.getException()));
      }
    } catch (IOException ioException) {
      return failed(new RatisRequestFailedException(ioException));
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
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

      final long currentDirLength =
          calcMap.computeIfAbsent(currentDir, MemorizedFileSizeCalc::new).getTotalFolderSize();
      final long triggerSnapshotFileSize = config.getImpl().getTriggerSnapshotFileSize();

      if (currentDirLength >= triggerSnapshotFileSize) {
        ConsensusGenericResponse consensusGenericResponse =
            triggerSnapshot(Utils.fromRaftGroupIdToConsensusGroupId(raftGroupId));
        if (consensusGenericResponse.isSuccess()) {
          logger.info("Raft group {} took snapshot successfully", raftGroupId);
        } else {
          logger.warn(
              "Raft group {} failed to take snapshot due to {}",
              raftGroupId,
              consensusGenericResponse.getException());
        }
      }
    }
  }

  private void startSnapshotGuardian() {
    final long delay = config.getImpl().getTriggerSnapshotTime();
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        diskGuardian, this::triggerSnapshotByCustomize, 0, delay, TimeUnit.SECONDS);
  }

  private ConsensusGenericResponse failed(ConsensusException e) {
    logger.debug("{} request failed with exception {}", this, e);
    return ConsensusGenericResponse.newBuilder().setSuccess(false).setException(e).build();
  }

  private ConsensusWriteResponse failedWrite(ConsensusException e) {
    logger.debug("{} write request failed with exception {}", this, e);
    return ConsensusWriteResponse.newBuilder().setException(e).build();
  }

  private ConsensusReadResponse failedRead(ConsensusException e) {
    logger.debug("{} read request failed with exception {}", this, e);
    return ConsensusReadResponse.newBuilder().setException(e).build();
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
    RatisClient client = null;
    try {
      client = getRaftClient(newGroupConf);
      reply =
          client.getRaftClient().admin().setConfiguration(new ArrayList<>(newGroupConf.getPeers()));
      if (!reply.isSuccess()) {
        throw new RatisRequestFailedException(reply.getException());
      }
    } catch (Exception e) {
      throw new RatisRequestFailedException(e);
    } finally {
      if (client != null) {
        client.returnSelf();
      }
    }
    return reply;
  }

  @TestOnly
  public RaftServer getServer() {
    return server;
  }

  private class RatisClientPoolFactory implements IClientPoolFactory<RaftGroup, RatisClient> {

    @Override
    public KeyedObjectPool<RaftGroup, RatisClient> createClientPool(
        ClientManager<RaftGroup, RatisClient> manager) {
      return new GenericKeyedObjectPool<>(
          new RatisClient.Factory(manager, properties, clientRpc, config.getClient()),
          new ClientPoolProperty.Builder<RatisClient>()
              .setCoreClientNumForEachNode(config.getClient().getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(config.getClient().getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }
}
