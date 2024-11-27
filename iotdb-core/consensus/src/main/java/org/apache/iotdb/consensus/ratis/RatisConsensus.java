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
import org.apache.iotdb.consensus.exception.RatisReadUnavailableException;
import org.apache.iotdb.consensus.exception.RatisRequestFailedException;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricSet;
import org.apache.iotdb.consensus.ratis.metrics.RatisMetricsManager;
import org.apache.iotdb.consensus.ratis.utils.Retriable;
import org.apache.iotdb.consensus.ratis.utils.RetryPolicy;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.rpc.TSStatusCode;

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
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/** A multi-raft consensus implementation based on Apache Ratis. */
class RatisConsensus implements IConsensus {

  private static final Logger logger = LoggerFactory.getLogger(RatisConsensus.class);

  /** The unique net communication endpoint */
  private final RaftPeer myself;

  private final File storageDir;
  private final MemoizedCheckedSupplier<RaftServer, IOException> server;

  private final RaftProperties properties = new RaftProperties();
  private final RaftClientRpc clientRpc;

  private final IClientManager<RaftGroup, RatisClient> clientManager;
  private final IClientManager<RaftGroup, RatisClient> reconfigurationClientManager;

  private final DiskGuardian diskGuardian;

  private final Map<RaftGroupId, RaftGroup> lastSeen = new ConcurrentHashMap<>();

  private final ClientId localFakeId = ClientId.randomId();
  private final AtomicLong localFakeCallId = new AtomicLong(0);

  private static final int DEFAULT_PRIORITY = 0;

  private static final int DEFAULT_WAIT_LEADER_READY_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(20);

  private final RatisConfig config;
  private final RatisConfig.Read.Option readOption;
  private final RetryPolicy<RaftClientReply> readRetryPolicy;
  private final RetryPolicy<RaftClientReply> writeRetryPolicy;

  private final RatisMetricSet ratisMetricSet;
  private final TConsensusGroupType consensusGroupType;

  private final ConcurrentHashMap<ConsensusGroupId, AtomicBoolean> canServeStaleRead;

  public RatisConsensus(ConsensusConfig config, IStateMachine.Registry registry) {
    myself =
        Utils.fromNodeInfoAndPriorityToRaftPeer(
            config.getThisNodeId(), config.getThisNodeEndPoint(), DEFAULT_PRIORITY);
    this.storageDir = new File(config.getStorageDir());

    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    GrpcConfigKeys.Server.setPort(properties, config.getThisNodeEndPoint().getPort());

    Utils.initRatisConfig(properties, config.getRatisConfig());
    this.config = config.getRatisConfig();
    this.readOption = this.config.getRead().getReadOption();
    this.canServeStaleRead =
        this.readOption == RatisConfig.Read.Option.DEFAULT ? new ConcurrentHashMap<>() : null;
    this.consensusGroupType = config.getConsensusGroupType();
    this.ratisMetricSet = new RatisMetricSet();
    this.readRetryPolicy =
        RetryPolicy.<RaftClientReply>newBuilder()
            .setRetryHandler(
                c ->
                    !c.isSuccess()
                        && (c.getException() instanceof ReadIndexException
                            || c.getException() instanceof ReadException
                            || c.getException() instanceof NotLeaderException))
            .setMaxAttempts(this.config.getImpl().getRetryTimesMax())
            .setWaitTime(
                TimeDuration.valueOf(
                    this.config.getImpl().getRetryWaitMillis(), TimeUnit.MILLISECONDS))
            .setMaxWaitTime(
                TimeDuration.valueOf(
                    this.config.getImpl().getRetryMaxWaitMillis(), TimeUnit.MILLISECONDS))
            .setExponentialBackoff(true)
            .build();
    this.writeRetryPolicy =
        RetryPolicy.<RaftClientReply>newBuilder()
            // currently, we only retry when ResourceUnavailableException is caught
            .setRetryHandler(
                reply ->
                    !reply.isSuccess()
                        && ((reply.getException() instanceof ResourceUnavailableException)
                            || reply.getException() instanceof LeaderNotReadyException
                            || reply.getException() instanceof LeaderSteppingDownException
                            || reply.getException() instanceof StateMachineException))
            .setMaxAttempts(this.config.getImpl().getRetryTimesMax())
            .setWaitTime(
                TimeDuration.valueOf(
                    this.config.getImpl().getRetryWaitMillis(), TimeUnit.MILLISECONDS))
            .setMaxWaitTime(
                TimeDuration.valueOf(
                    this.config.getImpl().getRetryMaxWaitMillis(), TimeUnit.MILLISECONDS))
            .setExponentialBackoff(true)
            .build();

    this.diskGuardian = new DiskGuardian(() -> this, this.config);

    clientManager =
        new IClientManager.Factory<RaftGroup, RatisClient>()
            .createClientManager(new RatisClientPoolFactory(false));
    reconfigurationClientManager =
        new IClientManager.Factory<RaftGroup, RatisClient>()
            .createClientManager(new RatisClientPoolFactory(true));

    clientRpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);

    // do not build server in constructor in case stateMachine is not ready
    server =
        MemoizedCheckedSupplier.valueOf(
            () ->
                RaftServer.newBuilder()
                    .setServerId(myself.getId())
                    .setProperties(properties)
                    .setOption(RaftStorage.StartupOption.RECOVER)
                    .setStateMachineRegistry(
                        raftGroupId ->
                            new ApplicationStateMachineProxy(
                                registry.apply(
                                    Utils.fromRaftGroupIdToConsensusGroupId(raftGroupId)),
                                raftGroupId,
                                this::onLeaderChanged))
                    .build());
  }

  @Override
  public synchronized void start() throws IOException {
    MetricService.getInstance().addMetricSet(this.ratisMetricSet);
    server.get().start();
    registerAndStartDiskGuardian();
  }

  @Override
  public synchronized void stop() throws IOException {
    try {
      diskGuardian.stop();
    } catch (InterruptedException e) {
      logger.warn("{}: interrupted when shutting down add Executor with exception {}", this, e);
      Thread.currentThread().interrupt();
    } finally {
      clientManager.close();
      reconfigurationClientManager.close();
      server.get().close();
      MetricService.getInstance().removeMetricSet(this.ratisMetricSet);
    }
  }

  /** Launch a consensus write with retry mechanism */
  private RaftClientReply writeWithRetry(CheckedSupplier<RaftClientReply, IOException> caller)
      throws IOException {
    RaftClientReply reply = null;
    try {
      reply = Retriable.attempt(caller, writeRetryPolicy, () -> caller, logger);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.debug("{}: interrupted when retrying for write request {}", this, caller);
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
    return writeWithRetry(() -> server.get().submitClientRequest(request));
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
    if (isLeader(groupId) && Utils.rejectWrite(consensusGroupType)) {
      try {
        forceStepDownLeader(raftGroup);
      } catch (Exception e) {
        logger.warn("leader {} read only, force step down failed due to {}", myself, e);
      }
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    }

    // serialize request into Message
    RequestMessage message = new RequestMessage(request);

    // 1. first try the local server
    RaftClientRequest clientRequest;
    try {
      clientRequest = buildRawRequest(raftGroupId, message, RaftClientRequest.writeRequestType());
    } catch (IOException e) {
      throw new RatisRequestFailedException(e);
    }

    RaftPeer suggestedLeader = null;
    if ((isLeader(groupId) || raftGroup.getPeers().size() == 1)
        && waitUntilLeaderReady(raftGroupId)) {
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
      } catch (GroupMismatchException e) {
        throw new ConsensusGroupNotExistException(groupId);
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
    } catch (GroupMismatchException e) {
      throw new ConsensusGroupNotExistException(groupId);
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

    // perform linearizable read under following two conditions:
    // 1. Read.Option is linearizable
    // 2. First probing read when Read.Option is default
    final boolean isLinearizableRead =
        readOption == RatisConfig.Read.Option.LINEARIZABLE
            || !canServeStaleRead.computeIfAbsent(groupId, id -> new AtomicBoolean(false)).get();

    RaftClientReply reply;
    try {
      reply = doRead(raftGroupId, request, isLinearizableRead);
      // allow stale read if current linearizable read returns successfully
      if (canServeStaleRead != null && isLinearizableRead) {
        canServeStaleRead.get(groupId).set(true);
      }
    } catch (ReadException | ReadIndexException | NotLeaderException e) {
      if (isLinearizableRead) {
        // linearizable read failed. the RaftServer is recovering from Raft Log and cannot serve
        // read requests.
        throw new RatisReadUnavailableException(e);
      } else {
        throw new RatisRequestFailedException(e);
      }
    } catch (GroupMismatchException e) {
      throw new ConsensusGroupNotExistException(groupId);
    } catch (IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("ServerNotReadyException")) {
        ServerNotReadyException serverNotReadyException =
            new ServerNotReadyException(e.getMessage());
        throw new RatisReadUnavailableException(serverNotReadyException);
      } else {
        throw new RatisRequestFailedException(e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RatisReadUnavailableException(e);
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
      reply =
          Retriable.attempt(
              () -> {
                try {
                  return server.get().submitClientRequest(request);
                } catch (
                    IOException
                        ioe) { // IOE indicates some unexpected errors, say StatusRuntimeException
                  if (ioe.getCause() instanceof StatusRuntimeException) {
                    // StatusRuntimeException will be thrown if the leader is offline (gRPC cannot
                    // connect to the peer)
                    // We can still retry in case it's a temporary network partition.
                    return RaftClientReply.newBuilder()
                        .setClientId(localFakeId)
                        .setServerId(server.get().getId())
                        .setGroupId(request.getRaftGroupId())
                        .setException(
                            new ReadIndexException(
                                "internal GRPC connection error:", ioe.getCause()))
                        .setSuccess(false)
                        .build();
                  } else {
                    throw ioe;
                  }
                }
              },
              readRetryPolicy,
              () -> readRequest,
              logger);
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
    try {
      RaftClientReply reply =
          server
              .get()
              .groupManagement(
                  GroupManagementRequest.newAdd(
                      localFakeId, myself.getId(), localFakeCallId.incrementAndGet(), group, true));
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
          server
              .get()
              .groupManagement(
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

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    logger.info("[RESET PEER LIST] Start to reset peer list to {}", correctPeers);
    final RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    final RaftGroup group = getGroupInfo(raftGroupId);

    if (group == null) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    boolean myselfInCorrectPeers =
        correctPeers.stream()
            .map(
                peer ->
                    Utils.fromNodeInfoAndPriorityToRaftPeer(
                        peer.getNodeId(), peer.getEndpoint(), DEFAULT_PRIORITY))
            .anyMatch(
                raftPeer ->
                    myself.getId() == raftPeer.getId()
                        && myself.getAddress().equals(raftPeer.getAddress()));
    if (!myselfInCorrectPeers) {
      logger.info(
          "[RESET PEER LIST] Local peer is not in the correct peer list, delete local peer {}",
          groupId);
      deleteLocalPeer(groupId);
      return;
    }

    final List<RaftPeer> newGroupPeers =
        Utils.fromPeersAndPriorityToRaftPeers(correctPeers, DEFAULT_PRIORITY);
    final RaftGroup newGroup = RaftGroup.valueOf(raftGroupId, newGroupPeers);

    RaftClientReply reply = sendReconfiguration(newGroup);
    if (reply.isSuccess()) {
      logger.info("[RESET PEER LIST] Peer list has been reset to {}", newGroupPeers);
    } else {
      logger.warn(
          "[RESET PEER LIST] Peer list failed to reset to {}, reply is {}", newGroup, reply);
    }
  }

  /** NOTICE: transferLeader *does not guarantee* the leader be transferred to newLeader. */
  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    // first fetch the newest information
    final RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    final RaftGroup raftGroup =
        Optional.ofNullable(getGroupInfo(raftGroupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    final RaftPeer newRaftLeader = Utils.fromPeerAndPriorityToRaftPeer(newLeader, DEFAULT_PRIORITY);
    final RaftClientReply reply;
    try {
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
      return server.get().getDivision(raftGroupId).getInfo().isLeader();
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
      return server.get().getDivision(raftGroupId).getInfo().isLeaderReady();
    } catch (IOException exception) {
      // if the read fails, simply return not ready
      logger.info("isLeaderReady request failed with exception: ", exception);
      return false;
    }
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    try {
      return server.get().getDivision(raftGroupId).getInfo().getCurrentTerm();
    } catch (IOException exception) {
      // if the read fails, simply return 0
      logger.info("getLogicalClock request failed with exception: ", exception);
      return 0;
    }
  }

  private boolean waitUntilLeaderReady(RaftGroupId groupId) {
    DivisionInfo divisionInfo;
    try {
      divisionInfo = server.get().getDivision(groupId).getInfo();
    } catch (IOException e) {
      // if the read fails, simply return not leader
      logger.info("isLeaderReady checking failed with exception: ", e);
      return false;
    }

    final long startTime = System.currentTimeMillis();
    final BooleanSupplier noRetryAtAnyOfFollowingCondition =
        () ->
            Utils.anyOf(
                // this peer is not a leader
                () -> getGroupInfo(groupId).getPeers().size() > 1 && !divisionInfo.isLeader(),
                // this peer is a ready leader
                () -> divisionInfo.isLeader() && divisionInfo.isLeaderReady(),
                // reaches max retry timeout
                () -> System.currentTimeMillis() - startTime >= DEFAULT_WAIT_LEADER_READY_TIMEOUT);

    try {
      Retriable.attemptUntilTrue(
          noRetryAtAnyOfFollowingCondition,
          TimeDuration.valueOf(10, TimeUnit.MILLISECONDS),
          "waitLeaderReady",
          logger);
      if (divisionInfo.isLeader() && !divisionInfo.isLeaderReady()) {
        logger.warn(
            "{}: leader is still not ready after {}ms", groupId, DEFAULT_WAIT_LEADER_READY_TIMEOUT);
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
      leaderId = server.get().getDivision(raftGroupId).getInfo().getLeaderId();
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
  public int getReplicationNum(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    try {
      return server.get().getDivision(raftGroupId).getGroup().getPeers().size();
    } catch (IOException e) {
      return 0;
    }
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    List<ConsensusGroupId> ids = new ArrayList<>();
    try {
      server
          .get()
          .getGroupIds()
          .forEach(groupId -> ids.add(Utils.fromRaftGroupIdToConsensusGroupId(groupId)));
      return ids;
    } catch (IOException e) {
      return Collections.emptyList();
    }
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIdsWithoutStarting() {
    if (!storageDir.exists()) {
      return Collections.emptyList();
    }
    List<ConsensusGroupId> consensusGroupIds = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
      for (Path path : stream) {
        try {
          RaftGroupId raftGroupId =
              RaftGroupId.valueOf(UUID.fromString(path.getFileName().toString()));
          consensusGroupIds.add(Utils.fromRaftGroupIdToConsensusGroupId(raftGroupId));
        } catch (Exception e) {
          logger.info(
              "The directory {} is not a group directory;" + " ignoring it. ",
              path.getFileName().toString());
        }
      }
    } catch (IOException e) {
      logger.error("Failed to get all consensus group ids from disk", e);
    }
    return consensusGroupIds;
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId consensusGroupId) {
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(consensusGroupId);
    return storageDir + File.separator + raftGroupId.getUuid().toString();
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    // do not support reload consensus config for now
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    final RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(groupId);
    final RaftGroup groupInfo = getGroupInfo(raftGroupId);
    if (groupInfo == null || !groupInfo.getPeers().contains(myself)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    final SnapshotManagementRequest request =
        SnapshotManagementRequest.newCreate(
            localFakeId,
            myself.getId(),
            raftGroupId,
            localFakeCallId.incrementAndGet(),
            300000,
            force ? 1 : 0);

    synchronized (raftGroupId) {
      final RaftClientReply reply;
      try {
        reply = server.get().snapshotManagement(request);
        if (!reply.isSuccess()) {
          throw new RatisRequestFailedException(reply.getException());
        }
        logger.info(
            "{} group {}: successfully taken snapshot at index {} with force = {}",
            this,
            raftGroupId,
            reply.getLogIndex(),
            force);
      } catch (IOException ioException) {
        throw new RatisRequestFailedException(ioException);
      }
    }
  }

  private void registerAndStartDiskGuardian() {
    final RatisConfig.Impl implConfig = config.getImpl();
    // by default, we control disk space
    diskGuardian.registerChecker(
        summary -> summary.getTotalSize() > implConfig.getRaftLogSizeMaxThreshold(),
        TimeDuration.valueOf(implConfig.getCheckAndTakeSnapshotInterval(), TimeUnit.SECONDS));

    // if we force periodic snapshot
    final long forceSnapshotInterval = implConfig.getForceSnapshotInterval();
    if (forceSnapshotInterval > 0) {
      diskGuardian.registerChecker(
          summary -> true, // just take it!
          TimeDuration.valueOf(forceSnapshotInterval, TimeUnit.SECONDS));
    }

    diskGuardian.start();
  }

  private RaftClientRequest buildRawRequest(
      RaftGroupId groupId, Message message, RaftClientRequest.Type type) throws IOException {
    return RaftClientRequest.newBuilder()
        .setServerId(server.get().getId())
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
      raftGroup = server.get().getDivision(raftGroupId).getGroup();
      RaftGroup lastSeenGroup = lastSeen.getOrDefault(raftGroupId, null);
      if (lastSeenGroup != null && !lastSeenGroup.equals(raftGroup)) {
        // delete the pooled raft-client of the out-dated group and cache the latest
        clientManager.clear(lastSeenGroup);
        reconfigurationClientManager.clear(lastSeenGroup);
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
      logger.error("Borrow client from pool for group {} failed.", group, e);
      // rethrow the exception
      throw e;
    }
  }

  private RatisClient getConfigurationRaftClient(RaftGroup group) throws ClientManagerException {
    try {
      return reconfigurationClientManager.borrowClient(group);
    } catch (ClientManagerException e) {
      logger.error("Borrow client from pool for group {} failed.", group, e);
      // rethrow the exception
      throw e;
    }
  }

  private RaftClientReply sendReconfiguration(RaftGroup newGroupConf)
      throws RatisRequestFailedException {
    // notify the group leader of configuration change
    RaftClientReply reply;
    try (RatisClient client = getConfigurationRaftClient(newGroupConf)) {
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

  private void onLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId leaderId) {
    Optional.ofNullable(canServeStaleRead)
        .ifPresent(
            m -> {
              final ConsensusGroupId gid =
                  Utils.fromRaftGroupIdToConsensusGroupId(groupMemberId.getGroupId());
              canServeStaleRead.computeIfAbsent(gid, id -> new AtomicBoolean()).set(false);
            });
  }

  public RaftServer getServer() throws IOException {
    return server.get();
  }

  @TestOnly
  public void allowStaleRead(ConsensusGroupId consensusGroupId) {
    canServeStaleRead.computeIfAbsent(consensusGroupId, id -> new AtomicBoolean(false)).set(true);
  }

  private class RatisClientPoolFactory implements IClientPoolFactory<RaftGroup, RatisClient> {

    private final boolean isReconfiguration;

    RatisClientPoolFactory(boolean isReconfiguration) {
      this.isReconfiguration = isReconfiguration;
    }

    @Override
    public GenericKeyedObjectPool<RaftGroup, RatisClient> createClientPool(
        ClientManager<RaftGroup, RatisClient> manager) {
      GenericKeyedObjectPool<RaftGroup, RatisClient> clientPool =
          new GenericKeyedObjectPool<>(
              isReconfiguration
                  ? new RatisClient.EndlessRetryFactory(
                      manager, properties, clientRpc, config.getClient())
                  : new RatisClient.Factory(manager, properties, clientRpc, config.getClient()),
              new ClientPoolProperty.Builder<RatisClient>()
                  .setMaxClientNumForEachNode(config.getClient().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
