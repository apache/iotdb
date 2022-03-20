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

import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.*;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.NetUtils;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A multi-raft consensus implementation based on Ratis, currently still under development.
 *
 * <p>See jira [IOTDB-2674](https://issues.apache.org/jira/browse/IOTDB-2674) for more details.
 */
public class RatisConsensus implements IConsensus {
  // the unique net communication endpoint
  private final RaftPeer myself;

  private final RaftServer server;
  private final IRatisSerializer serializer;

  // TODO when comm with myself, use method call instead of RPC
  private final Map<RaftGroupId, RaftClient> clientMap;
  private final Map<RaftGroupId, RaftGroup> raftGroupMap;

  private ClientId localFakeId;
  private AtomicLong localFakeCallId;
  /**
   * This function will use the previous client for groupId to query the latest group info it will
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
    RaftClientReply reply = null;
    try {
      reply = client.admin().setConfiguration(peers);
    } catch (IOException e) {
      throw new RatisRequestFailedException();
    }
    return reply;
  }

  @Override
  public void start() {
    // TODO awkward, should we add IOException to method signature?
    try {
      server.start();
    } catch (IOException ignored) {
      try {
        server.close();
      } catch (IOException ignored1) {
      }
    }
  }

  @Override
  public void stop() {
    try {
      server.close();
    } catch (IOException ignored) {
    }
  }

  @Override
  public ConsensusWriteResponse write(
      ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {

    RaftClient client = clientMap.get(Utils.toRatisGroupId(groupId));
    TSStatus writeResult = null;
    try {
      ByteBufferConsensusRequest request = (ByteBufferConsensusRequest) IConsensusRequest;
      Message message = Message.valueOf(ByteString.copyFrom(request.getContent()));
      RaftClientReply reply = client.io().send(message);
      writeResult = Utils.deserializeFrom(
              reply.getMessage().getContent().asReadOnlyByteBuffer());
    } catch (IOException | TException e) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new RatisRequestFailedException())
          .build();
    }
    return ConsensusWriteResponse.newBuilder().setStatus(writeResult).build();
  }

  /**
   * Read directly from LOCAL COPY
   * notice: May read stale data (not linearizable)
   */
  @Override
  public synchronized ConsensusReadResponse read(
      ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {

    RaftClientReply reply = null;
    try {
      assert IConsensusRequest instanceof  ByteBufferConsensusRequest;
      ByteBufferConsensusRequest request = (ByteBufferConsensusRequest) IConsensusRequest;

      RaftClientRequest clientRequest = RaftClientRequest.newBuilder()
              .setServerId(server.getId())
              .setClientId(localFakeId)
              .setGroupId(Utils.toRatisGroupId(groupId))
              .setCallId(localFakeCallId.incrementAndGet())
              .setMessage(Message.valueOf(ByteString.copyFrom(request.getContent())))
              .setType(RaftClientRequest.staleReadRequestType(0))
              .build();

      reply = server.submitClientRequest(clientRequest);
    } catch (IOException e) {
      return ConsensusReadResponse.newBuilder()
          .setException(new RatisRequestFailedException())
          .build();
    }

    Message ret = reply.getMessage();
    assert ret instanceof ReadLocalMessage;
    ReadLocalMessage readLocalMessage = (ReadLocalMessage) ret;

    return ConsensusReadResponse.newBuilder()
        .setDataSet(readLocalMessage.getDataSet())
        .build();
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
      return failed(new ConsensusWrongGroupException(groupId));
    }
    raftGroupMap.put(group.getGroupId(), group);

    // build and store the corresponding client
    RaftClient client = buildClientAndCache(group);

    // add RaftPeer myself to this RaftGroup
    RaftClientReply reply = null;
    try {
      reply = client.getGroupManagementApi(myself.getId()).add(group);
    } catch (IOException e) {
      return failed(new RatisRequestFailedException());
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
      return failed(new PeerNotInGroupException(groupId));
    }

    RaftClient client = clientMap.get(raftGroupId);
    // send remove group to myself
    RaftClientReply reply = null;
    try {
      reply = client.getGroupManagementApi(myself.getId()).remove(raftGroupId, false, false);
    } catch (IOException e) {
      return failed(new RatisRequestFailedException());
    }

    if (reply.isSuccess()) {
      // delete Group information and its corresponding client
      raftGroupMap.remove(raftGroupId);
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
    RaftPeer peerToAdd = Utils.toRaftPeer(peer);

    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      return failed(new ConsensusWrongGroupException(groupId));
    }

    // pre-condition: peer not in this group
    if (group.getPeers().contains(peerToAdd)) {
      return failed(new PeerAlreadyInGroupException(groupId, peer));
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
    RaftPeer peerToRemove = Utils.toRaftPeer(peer);

    // pre-conditions: group exists and myself in this group
    if (group == null || !group.getPeers().contains(myself)) {
      return failed(new ConsensusWrongGroupException(groupId));
    }
    // pre-condition: peer is a member of groupId
    if (!group.getPeers().contains(peerToRemove)) {
      return failed(new PeerNotInGroupException(groupId));
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
      return failed(new ConsensusWrongGroupException(groupId));
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
      return failed(new RatisRequestFailedException());
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    RaftGroupId raftGroupId = Utils.toRatisGroupId(groupId);
    RaftClient client = clientMap.getOrDefault(raftGroupId, null);
    if (client == null) {
      return failed(new ConsensusWrongGroupException(groupId));
    }
    RaftPeer newRaftLeader = Utils.toRaftPeer(newLeader);

    RaftClientReply reply = null;
    try {
      // TODO tuning for timeoutMs
      reply = client.admin().transferLeadership(newRaftLeader.getId(), 2000);
    } catch (IOException e) {
      return failed(new RatisRequestFailedException());
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(reply.isSuccess()).build();
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  private ConsensusGenericResponse failed(ConsensusException e) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).setException(e).build();
  }

  private RaftGroup buildRaftGroup(ConsensusGroupId groupId, List<Peer> peers) {
    List<RaftPeer> raftPeers = peers.stream().map(Utils::toRaftPeer).collect(Collectors.toList());
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
    clientMap.put(group.getGroupId(), client);
    return client;
  }

  private RatisConsensus(
      Endpoint endpoint,
      File ratisStorageDir,
      IStateMachine.Registry registry,
      IRatisSerializer serializer)
      throws IOException {

    this.serializer = serializer;
    this.clientMap = new ConcurrentHashMap<>();
    this.raftGroupMap = new HashMap<>();
    this.localFakeId = ClientId.randomId();
    this.localFakeCallId = new AtomicLong(0);

    // create a RaftPeer as endpoint of comm
    String address = Utils.IP_PORT(endpoint);
    myself = Utils.toRaftPeer(endpoint);

    RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in RaftProperty object
    if (ratisStorageDir == null || !ratisStorageDir.isDirectory()) {
      ratisStorageDir = new File("./" + myself.getId().toString());
    }
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
                        registry.apply(Utils.toConsensusGroupId(raftGroupId)), serializer))
            .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Endpoint endpoint;
    private IStateMachine.Registry registry;
    private File storageDir;
    private IRatisSerializer serializer;

    public Builder() {
      storageDir = null;
    }

    public Builder setEndpoint(Endpoint endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public Builder setStateMachineRegistry(IStateMachine.Registry registry) {
      this.registry = registry;
      return this;
    }

    public Builder setStorageDir(File dir) {
      this.storageDir = dir;
      return this;
    }

    public Builder setSerializer(IRatisSerializer serializer) {
      this.serializer = serializer;
      return this;
    }

    public RatisConsensus build() throws IOException {
      return new RatisConsensus(endpoint, storageDir, registry, serializer);
    }
  }
}
