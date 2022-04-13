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

import org.apache.iotdb.commons.cluster.Endpoint;
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
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
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
  // the unique net communication endpoint
  private final RaftPeer myself;

  private final RaftServer server;

  private final Map<RaftGroupId, RaftClient> clientMap;
  private final Map<RaftGroupId, RaftGroup> raftGroupMap;

  private ClientId localFakeId;
  private AtomicLong localFakeCallId;

  private static final int DEFAULT_PRIORITY = 0;
  private static final int LEADER_PRIORITY = 1;

  private Logger logger = LoggerFactory.getLogger(RatisConsensus.class);

  public RatisConsensus(Endpoint endpoint, File ratisStorageDir, IStateMachine.Registry registry)
      throws IOException {

    this.clientMap = new ConcurrentHashMap<>();
    this.raftGroupMap = new ConcurrentHashMap<>();
    this.localFakeId = ClientId.randomId();
    this.localFakeCallId = new AtomicLong(0);

    // create a RaftPeer as endpoint of comm
    String address = Utils.IPAddress(endpoint);
    myself = Utils.toRaftPeer(endpoint, DEFAULT_PRIORITY);

    RaftProperties properties = new RaftProperties();

    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(ratisStorageDir));

    // set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(address).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

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
    RaftGroup raftGroup = raftGroupMap.get(Utils.toRatisGroupId(groupId));
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      return failedWrite(new ConsensusGroupNotExistException(groupId));
    }

    // serialize request into Message
    Message message = new RequestMessage(IConsensusRequest);

    // 1. first try the local server
    RaftClientRequest clientRequest =
        buildRawRequest(groupId, message, RaftClientRequest.writeRequestType());
    RaftClientReply localServerReply = null;
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
    RaftClient client = clientMap.get(Utils.toRatisGroupId(groupId));
    TSStatus writeResult = null;
    try {
      RaftClientReply reply = client.io().send(message);
      writeResult = Utils.deserializeFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    } catch (IOException | TException e) {
      return failedWrite(new RatisRequestFailedException(e));
    }

    if (suggestedLeader != null) {
      Endpoint leaderEndPoint = Utils.getEndpoint(suggestedLeader);
      writeResult.setRedirectNode(new EndPoint(leaderEndPoint.getIp(), leaderEndPoint.getPort()));
    }

    return ConsensusWriteResponse.newBuilder().setStatus(writeResult).build();
  }

  /** Read directly from LOCAL COPY notice: May read stale data (not linearizable) */
  @Override
  public ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {

    RaftGroup group = raftGroupMap.get(Utils.toRatisGroupId(groupId));
    if (group == null || !group.getPeers().contains(myself)) {
      return failedRead(new ConsensusGroupNotExistException(groupId));
    }

    RaftClientReply reply = null;
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
    raftGroupMap.put(group.getGroupId(), group);

    // build and store the corresponding client
    RaftClient client = buildClientAndCache(group);

    // add RaftPeer myself to this RaftGroup
    RaftClientReply reply = null;
    try {
      reply = client.getGroupManagementApi(myself.getId()).add(group);
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
    RaftGroup raftGroup = raftGroupMap.get(raftGroupId);

    // pre-conditions: group exists and myself in this group
    if (raftGroup == null || !raftGroup.getPeers().contains(myself)) {
      return failed(new PeerNotInConsensusGroupException(groupId, myself));
    }

    RaftClient client = clientMap.get(raftGroupId);
    // send remove group to myself
    RaftClientReply reply = null;
    try {
      reply = client.getGroupManagementApi(myself.getId()).remove(raftGroupId, false, false);
    } catch (IOException e) {
      return failed(new RatisRequestFailedException(e));
    }

    if (reply.isSuccess()) {
      // delete Group information and its corresponding client
      raftGroupMap.remove(raftGroupId);
      closeRaftClient(raftGroupId);
      clientMap.remove(raftGroupId);
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
    try {
      syncGroupInfoAndRebuildClient(groupId);
    } catch (ConsensusGroupNotExistException e) {
      return failed(e);
    }
    RaftGroup group = raftGroupMap.get(raftGroupId);
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
      reply = sendReconfiguration(raftGroupId, newConfig);

      // sync again
      syncGroupInfoAndRebuildClient(groupId);
    } catch (RatisRequestFailedException | ConsensusGroupNotExistException e) {
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
    try {
      syncGroupInfoAndRebuildClient(groupId);
    } catch (ConsensusGroupNotExistException e) {
      return failed(e);
    }
    RaftGroup group = raftGroupMap.get(raftGroupId);
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
      reply = sendReconfiguration(raftGroupId, newConfig);
      // sync again
      syncGroupInfoAndRebuildClient(groupId);
    } catch (RatisRequestFailedException | ConsensusGroupNotExistException e) {
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
    raftGroupMap.put(raftGroup.getGroupId(), raftGroup);

    // build the client and store it
    buildClientAndCache(raftGroup);

    // add RaftPeer myself to this RaftGroup
    RaftClientReply reply = null;
    try {
      reply = sendReconfiguration(raftGroup.getGroupId(), new ArrayList<>(raftGroup.getPeers()));
      // sync again
      syncGroupInfoAndRebuildClient(groupId);
    } catch (ConsensusGroupNotExistException | RatisRequestFailedException e) {
      return failed(new RatisRequestFailedException(e));
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    // By default, Ratis gives every Raft Peer same priority 0
    // Ratis does not allow a peer.priority <= currentLeader.priority to becomes the leader
    // So we have to enhance to leader's priority

    // first fetch the newest information
    try {
      syncGroupInfoAndRebuildClient(groupId);
    } catch (ConsensusGroupNotExistException e) {
      return failed(e);
    }

    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftGroup raftGroup = raftGroupMap.get(raftGroupId);
    RaftClient client = clientMap.get(raftGroupId);
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
      RaftClientReply configChangeReply = client.admin().setConfiguration(newConfiguration);
      if (!configChangeReply.isSuccess()) {
        return failed(new RatisRequestFailedException(configChangeReply.getException()));
      }
      // TODO tuning for timeoutMs
      reply = client.admin().transferLeadership(newRaftLeader.getId(), 2000);
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
      logger.warn("isLeader request failed with exception: ", exception);
      isLeader = false;
    }
    return isLeader;
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

  private RaftGroup buildRaftGroup(ConsensusGroupId groupId, List<Peer> peers) {
    List<RaftPeer> raftPeers =
        peers.stream()
            .map(peer -> Utils.toRaftPeer(peer, DEFAULT_PRIORITY))
            .collect(Collectors.toList());
    return RaftGroup.valueOf(Utils.toRatisGroupId(groupId), raftPeers);
  }

  private RaftClient buildClientAndCache(RaftGroup group) {
    RaftProperties raftProperties = new RaftProperties();
    RaftClient.Builder builder =
        RaftClient.newBuilder()
            .setProperties(raftProperties)
            .setRaftGroup(group)
            .setClientRpc(
                new GrpcFactory(new Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties));
    RaftClient client = builder.build();
    closeRaftClient(group.getGroupId());
    clientMap.put(group.getGroupId(), client);
    return client;
  }

  private void closeRaftClient(RaftGroupId groupId) {
    RaftClient client = clientMap.get(groupId);
    if (client != null) {
      try {
        client.close();
      } catch (IOException exception) {
        logger.warn("client for gid {} close failure {}", groupId, exception);
      }
    }
  }

  /**
   * This function will use the previous client for groupId to query the latest group info It will
   * update the new group info into the groupMap and rebuild its client
   *
   * @throws ConsensusGroupNotExistException when cannot get the group info
   */
  private void syncGroupInfoAndRebuildClient(ConsensusGroupId groupId)
      throws ConsensusGroupNotExistException {
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftClient current = clientMap.get(raftGroupId);
    try {
      GroupInfoReply reply = current.getGroupManagementApi(myself.getId()).info(raftGroupId);

      if (!reply.isSuccess()) {
        throw new ConsensusGroupNotExistException(groupId);
      }

      raftGroupMap.put(raftGroupId, reply.getGroup());
      buildClientAndCache(raftGroupMap.get(raftGroupId));
    } catch (IOException e) {
      throw new ConsensusGroupNotExistException(groupId);
    }
  }

  private RaftClientReply sendReconfiguration(RaftGroupId raftGroupId, List<RaftPeer> peers)
      throws RatisRequestFailedException {
    RaftClient client = clientMap.get(raftGroupId);
    // notify the group leader of configuration change
    RaftClientReply reply;
    try {
      reply = client.admin().setConfiguration(peers);
    } catch (IOException e) {
      throw new RatisRequestFailedException(e);
    }
    return reply;
  }
}
