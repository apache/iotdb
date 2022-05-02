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
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.exception.RatisRequestFailedException;
import org.apache.iotdb.consensus.statemachine.IStateMachine;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A multi-raft consensus implementation based on Ratis, currently still under development.
 *
 * <p>See jira [IOTDB-2674](https://issues.apache.org/jira/browse/IOTDB-2674) for more details.
 */
class RatisConsensus implements IConsensus {

  private final Logger logger = LoggerFactory.getLogger(RatisConsensus.class);

  // the unique net communication endpoint
  private final RaftPeer myself;
  private final RaftServer server;

  private final RaftProperties properties = new RaftProperties();
  private final RaftClientRpc clientRpc;

  private final IClientManager<RaftGroup, RatisClient> clientManager =
      new IClientManager.Factory<RaftGroup, RatisClient>()
          .createClientManager(new RatisClientPoolFactory());

  private Map<RaftGroupId, RaftGroup> lastSeen;

  private final ClientId localFakeId = ClientId.randomId();
  private final AtomicLong localFakeCallId = new AtomicLong(0);

  private static final int DEFAULT_PRIORITY = 0;
  private static final int LEADER_PRIORITY = 1;

  /**
   * @param ratisStorageDir different groups of RatisConsensus Peer all share ratisStorageDir as
   *     root dir
   */
  public RatisConsensus(TEndPoint endpoint, File ratisStorageDir, IStateMachine.Registry registry)
      throws IOException {
    lastSeen = new ConcurrentHashMap<>();

    // create a RaftPeer as endpoint of comm
    String address = Utils.IPAddress(endpoint);
    myself = Utils.toRaftPeer(endpoint, DEFAULT_PRIORITY);

    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(ratisStorageDir));
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);

    // set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(address).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);
    clientRpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);

    server =
        RaftServer.newBuilder()
            .setServerId(myself.getId())
            .setProperties(properties)
            .setStateMachineRegistry(
                raftGroupId ->
                    new ApplicationStateMachineProxy(
                        registry.apply(Utils.toConsensusGroupId(raftGroupId))))
            .build();
  }

  @Override
  public void start() throws IOException {
    server.start();
  }

  @Override
  public void stop() throws IOException {
    clientManager.close();
    server.close();
  }

  /**
   * write will first send request to local server use method call if local server is not leader, it
   * will use RaftClient to send RPC to read leader
   */
  @Override
  public ConsensusWriteResponse write(
      ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {

    // pre-condition: group exists and myself server serves this group
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      return failedWrite(new ConsensusGroupNotExistException(groupId));
    }

    // serialize request into Message
    Message message = new RequestMessage(IConsensusRequest);

    // 1. first try the local server
    RaftClientRequest clientRequest =
        buildRawRequest(groupId, message, RaftClientRequest.writeRequestType());
    RaftClientReply localServerReply;
    RaftPeer suggestedLeader = null;
    try {
      localServerReply = server.submitClientRequest(clientRequest);
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

    // 2. try raft client
    TSStatus writeResult;
    try {
      RatisClient client = getRaftClient(raftGroup);
      RaftClientReply reply = client.getRaftClient().io().send(message);
      writeResult = Utils.deserializeFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
      client.returnSelf();
    } catch (IOException | TException e) {
      return failedWrite(new RatisRequestFailedException(e));
    }

    if (suggestedLeader != null) {
      TEndPoint leaderEndPoint = Utils.getEndpoint(suggestedLeader);
      writeResult.setRedirectNode(new TEndPoint(leaderEndPoint.getIp(), leaderEndPoint.getPort()));
    }

    return ConsensusWriteResponse.newBuilder().setStatus(writeResult).build();
  }

  /** Read directly from LOCAL COPY notice: May read stale data (not linearizable) */
  @Override
  public ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {

    RaftGroup group = getGroupInfo(Utils.toRatisGroupId(groupId));
    if (group == null || !group.getPeers().contains(myself)) {
      return failedRead(new ConsensusGroupNotExistException(groupId));
    }

    RaftClientReply reply;
    try {
      RequestMessage message = new RequestMessage(IConsensusRequest);

      RaftClientRequest clientRequest =
          buildRawRequest(groupId, message, RaftClientRequest.staleReadRequestType(0));

      reply = server.submitClientRequest(clientRequest);
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
  public ConsensusGenericResponse addConsensusGroup(ConsensusGroupId groupId, List<Peer> peers) {
    RaftGroup group = buildRaftGroup(groupId, peers);
    // pre-conditions: myself in this new group
    if (!group.getPeers().contains(myself)) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

    // add RaftPeer myself to this RaftGroup
    RaftClientReply reply;
    try {
      RatisClient client = getRaftClient(group);
      reply = client.getRaftClient().getGroupManagementApi(myself.getId()).add(group);
      client.returnSelf();
    } catch (IOException e) {
      return failed(new RatisRequestFailedException(e));
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
  public ConsensusGenericResponse removeConsensusGroup(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);

    // pre-conditions: group exists and myself in this group
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      return failed(new PeerNotInConsensusGroupException(groupId, myself));
    }

    // send remove group to myself
    RaftClientReply reply;
    try {
      RatisClient client = getRaftClient(raftGroup);
      reply =
          client
              .getRaftClient()
              .getGroupManagementApi(myself.getId())
              .remove(raftGroupId, false, false);
      client.returnSelf();
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
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    RaftPeer peerToAdd = Utils.toRaftPeer(peer, DEFAULT_PRIORITY);

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
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup group = getGroupInfo(raftGroupId);
    RaftPeer peerToRemove = Utils.toRaftPeer(peer, DEFAULT_PRIORITY);

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
      return failed(new RatisRequestFailedException(e));
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  /**
   * transferLeader in Ratis implementation is not guaranteed to transfer leadership to the
   * designated peer Thus, it may produce undetermined results. Caller should not count on this API.
   */
  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    // By default, Ratis gives every Raft Peer same priority 0
    // Ratis does not allow a peer.priority <= currentLeader.priority to becomes the leader
    // So we have to enhance to leader's priority

    // first fetch the newest information

    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup raftGroup = getGroupInfo(raftGroupId);

    if (raftGroup == null) {
      return failed(new ConsensusGroupNotExistException(groupId));
    }

    RaftPeer newRaftLeader = Utils.toRaftPeer(newLeader, LEADER_PRIORITY);

    ArrayList<RaftPeer> newConfiguration = new ArrayList<>();
    for (RaftPeer raftPeer : raftGroup.getPeers()) {
      if (raftPeer.getId().equals(newRaftLeader.getId())) {
        newConfiguration.add(newRaftLeader);
      } else {
        // degrade every other peer to default priority
        newConfiguration.add(Utils.toRaftPeer(Utils.getEndpoint(raftPeer), DEFAULT_PRIORITY));
      }
    }

    RaftClientReply reply = null;
    try {
      RatisClient client = getRaftClient(raftGroup);
      RaftClientReply configChangeReply =
          client.getRaftClient().admin().setConfiguration(newConfiguration);
      if (!configChangeReply.isSuccess()) {
        return failed(new RatisRequestFailedException(configChangeReply.getException()));
      }
      // TODO tuning for timeoutMs
      reply = client.getRaftClient().admin().transferLeadership(newRaftLeader.getId(), 2000);
      client.returnSelf();
    } catch (IOException e) {
      return failed(new RatisRequestFailedException(e));
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);

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

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    if (isLeader(groupId)) {
      return new Peer(groupId, Utils.parseFromRatisId(myself.getId().toString()));
    }

    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftClient client = null;
    try {
      client = server.getDivision(raftGroupId).getRaftClient();
    } catch (IOException e) {
      logger.warn("cannot find raft client for group " + groupId);
      return null;
    }
    TEndPoint leaderEndpoint = Utils.parseFromRatisId(client.getLeaderId().toString());
    return new Peer(groupId, leaderEndpoint);
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  private ConsensusGenericResponse failed(ConsensusException e) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).setException(e).build();
  }

  private ConsensusWriteResponse failedWrite(ConsensusException e) {
    return ConsensusWriteResponse.newBuilder().setException(e).build();
  }

  private ConsensusReadResponse failedRead(ConsensusException e) {
    return ConsensusReadResponse.newBuilder().setException(e).build();
  }

  private RaftClientRequest buildRawRequest(
      ConsensusGroupId groupId, Message message, RaftClientRequest.Type type) {
    return RaftClientRequest.newBuilder()
        .setServerId(server.getId())
        .setClientId(localFakeId)
        .setCallId(localFakeCallId.incrementAndGet())
        .setGroupId(Utils.toRatisGroupId(groupId))
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
      logger.debug("get group failed ", e);
    }
    return raftGroup;
  }

  private RaftGroup buildRaftGroup(ConsensusGroupId groupId, List<Peer> peers) {
    List<RaftPeer> raftPeers =
        peers.stream()
            .map(peer -> Utils.toRaftPeer(peer, DEFAULT_PRIORITY))
            .collect(Collectors.toList());
    return RaftGroup.valueOf(Utils.toRatisGroupId(groupId), raftPeers);
  }

  private RatisClient getRaftClient(RaftGroup group) throws IOException {
    try {
      return clientManager.borrowClient(group);
    } catch (IOException e) {
      logger.error(String.format("Borrow client from pool for group %s failed.", group), e);
      // rethrow the exception
      throw e;
    }
  }

  private RaftClientReply sendReconfiguration(RaftGroup newGroupConf)
      throws RatisRequestFailedException {
    // notify the group leader of configuration change
    RaftClientReply reply;
    try {
      RatisClient client = getRaftClient(newGroupConf);
      reply =
          client.getRaftClient().admin().setConfiguration(new ArrayList<>(newGroupConf.getPeers()));
      client.returnSelf();
    } catch (IOException e) {
      throw new RatisRequestFailedException(e);
    }
    return reply;
  }

  private class RatisClientPoolFactory implements IClientPoolFactory<RaftGroup, RatisClient> {
    @Override
    public KeyedObjectPool<RaftGroup, RatisClient> createClientPool(
        ClientManager<RaftGroup, RatisClient> manager) {
      return new GenericKeyedObjectPool<>(
          new RatisClient.Factory(
              manager, new ClientFactoryProperty.Builder().build(), properties, clientRpc),
          new ClientPoolProperty.Builder<RatisClient>().build().getConfig());
    }
  }
}
