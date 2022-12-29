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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.rpc.AutoScalingBufferWriteTransport;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TByteBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {
  private static final int TEMP_BUFFER_SIZE = 1024;
  private static final byte PADDING_MAGIC = 0x47;

  private Utils() {}

  public static String hostAddress(TEndPoint endpoint) {
    return String.format("%s:%d", endpoint.getIp(), endpoint.getPort());
  }

  public static String fromTEndPointToString(TEndPoint endpoint) {
    return String.format("%s_%d", endpoint.getIp(), endpoint.getPort());
  }

  /** Encode the ConsensusGroupId into 6 bytes: 2 Bytes for Group Type and 4 Bytes for Group ID */
  public static long groupEncode(ConsensusGroupId consensusGroupId) {
    // use abbreviations to prevent overflow
    long groupType = consensusGroupId.getType().getValue();
    long groupCode = groupType << 32;
    groupCode += consensusGroupId.getId();
    return groupCode;
  }

  public static RaftPeerId fromNodeIdToRaftPeerId(int nodeId) {
    return RaftPeerId.valueOf(String.valueOf(nodeId));
  }

  public static TEndPoint fromRaftPeerAddressToTEndPoint(String address) {
    String[] items = address.split(":");
    return new TEndPoint(items[0], Integer.parseInt(items[1]));
  }

  public static int fromRaftPeerIdToNodeId(RaftPeerId id) {
    return Integer.parseInt(id.toString());
  }

  public static TEndPoint fromRaftPeerProtoToTEndPoint(RaftPeerProto proto) {
    String[] items = proto.getAddress().split(":");
    return new TEndPoint(items[0], Integer.parseInt(items[1]));
  }

  // priority is used as ordinal of leader election
  public static RaftPeer fromNodeInfoAndPriorityToRaftPeer(
      int nodeId, TEndPoint endpoint, int priority) {
    return RaftPeer.newBuilder()
        .setId(fromNodeIdToRaftPeerId(nodeId))
        .setAddress(hostAddress(endpoint))
        .setPriority(priority)
        .build();
  }

  public static RaftPeer fromPeerAndPriorityToRaftPeer(Peer peer, int priority) {
    return fromNodeInfoAndPriorityToRaftPeer(peer.getNodeId(), peer.getEndpoint(), priority);
  }

  public static List<RaftPeer> fromPeersAndPriorityToRaftPeers(List<Peer> peers, int priority) {
    return peers.stream()
        .map(peer -> Utils.fromPeerAndPriorityToRaftPeer(peer, priority))
        .collect(Collectors.toList());
  }

  public static int fromRaftPeerProtoToNodeId(RaftPeerProto proto) {
    return Integer.parseInt(proto.getId().toStringUtf8());
  }

  public static List<Peer> fromRaftProtoListAndRaftGroupIdToPeers(
      List<RaftPeerProto> raftProtoList, RaftGroupId id) {
    ConsensusGroupId consensusGroupId = Utils.fromRaftGroupIdToConsensusGroupId(id);
    return raftProtoList.stream()
        .map(
            peer ->
                new Peer(
                    consensusGroupId,
                    Utils.fromRaftPeerProtoToNodeId(peer),
                    Utils.fromRaftPeerProtoToTEndPoint(peer)))
        .collect(Collectors.toList());
  }

  /** Given ConsensusGroupId, generate a deterministic RaftGroupId current scheme: */
  public static RaftGroupId fromConsensusGroupIdToRaftGroupId(ConsensusGroupId consensusGroupId) {
    long groupCode = groupEncode(consensusGroupId);
    byte[] bGroupCode = ByteBuffer.allocate(Long.BYTES).putLong(groupCode).array();
    byte[] bPaddedGroupName = new byte[16];
    for (int i = 0; i < 10; i++) {
      bPaddedGroupName[i] = PADDING_MAGIC;
    }
    System.arraycopy(bGroupCode, 2, bPaddedGroupName, 10, bGroupCode.length - 2);

    return RaftGroupId.valueOf(ByteString.copyFrom(bPaddedGroupName));
  }

  /** Given raftGroupId, decrypt ConsensusGroupId out of it */
  public static ConsensusGroupId fromRaftGroupIdToConsensusGroupId(RaftGroupId raftGroupId) {
    byte[] padded = raftGroupId.toByteString().toByteArray();
    long type = (long) ((padded[10] & 0xff) << 8) + (padded[11] & 0xff);
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
    byteBuffer.put(padded, 12, 4);
    byteBuffer.flip();
    return ConsensusGroupId.Factory.create((int) type, byteBuffer.getInt());
  }

  public static ByteBuffer serializeTSStatus(TSStatus status) throws TException {
    AutoScalingBufferWriteTransport byteBuffer =
        new AutoScalingBufferWriteTransport(TEMP_BUFFER_SIZE);
    TCompactProtocol protocol = new TCompactProtocol(byteBuffer);
    status.write(protocol);
    return ByteBuffer.wrap(byteBuffer.getBuffer());
  }

  public static TSStatus deserializeFrom(ByteBuffer buffer) throws TException {
    TSStatus status = new TSStatus();
    TByteBuffer byteBuffer = new TByteBuffer(buffer);
    TCompactProtocol protocol = new TCompactProtocol(byteBuffer);
    status.read(protocol);
    return status;
  }

  public static String getMetadataFromTermIndex(TermIndex termIndex) {
    return String.format("%d_%d", termIndex.getTerm(), termIndex.getIndex());
  }

  public static TermIndex getTermIndexFromDir(File snapshotDir) {
    return getTermIndexFromMetadataString(snapshotDir.getName());
  }

  public static TermIndex getTermIndexFromMetadataString(String metadata) {
    String[] items = metadata.split("_");
    return TermIndex.valueOf(Long.parseLong(items[0]), Long.parseLong(items[1]));
  }

  public static void initRatisConfig(RaftProperties properties, RatisConfig config) {
    GrpcConfigKeys.setMessageSizeMax(properties, config.getGrpc().getMessageSizeMax());
    GrpcConfigKeys.setFlowControlWindow(properties, config.getGrpc().getFlowControlWindow());
    GrpcConfigKeys.Server.setAsyncRequestThreadPoolCached(
        properties, config.getGrpc().isAsyncRequestThreadPoolCached());
    GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(
        properties, config.getGrpc().getAsyncRequestThreadPoolSize());
    GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(
        properties, config.getGrpc().getLeaderOutstandingAppendsMax());

    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, config.getRpc().getSlownessTimeout());
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, config.getRpc().getTimeoutMin());
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, config.getRpc().getTimeoutMax());
    RaftServerConfigKeys.Rpc.setSleepTime(properties, config.getRpc().getSleepTime());
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, config.getRpc().getRequestTimeout());

    RaftServerConfigKeys.LeaderElection.setLeaderStepDownWaitTime(
        properties, config.getLeaderElection().getLeaderStepDownWaitTimeKey());
    RaftServerConfigKeys.LeaderElection.setPreVote(
        properties, config.getLeaderElection().isPreVote());

    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(
        properties, config.getSnapshot().isAutoTriggerEnabled());
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
        properties, config.getSnapshot().getAutoTriggerThreshold());
    RaftServerConfigKeys.Snapshot.setCreationGap(properties, config.getSnapshot().getCreationGap());
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(
        properties, config.getSnapshot().getRetentionFileNum());

    RaftServerConfigKeys.ThreadPool.setClientCached(
        properties, config.getThreadPool().isClientCached());
    RaftServerConfigKeys.ThreadPool.setClientSize(
        properties, config.getThreadPool().getClientSize());
    RaftServerConfigKeys.ThreadPool.setProxyCached(
        properties, config.getThreadPool().isProxyCached());
    RaftServerConfigKeys.ThreadPool.setProxySize(properties, config.getThreadPool().getProxySize());
    RaftServerConfigKeys.ThreadPool.setServerCached(
        properties, config.getThreadPool().isServerCached());
    RaftServerConfigKeys.ThreadPool.setServerSize(
        properties, config.getThreadPool().getServerSize());

    RaftServerConfigKeys.Log.setUseMemory(properties, config.getLog().isUseMemory());
    RaftServerConfigKeys.Log.setQueueElementLimit(
        properties, config.getLog().getQueueElementLimit());
    RaftServerConfigKeys.Log.setQueueByteLimit(properties, config.getLog().getQueueByteLimit());
    RaftServerConfigKeys.Log.setPurgeGap(properties, config.getLog().getPurgeGap());
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(
        properties, config.getLog().isPurgeUptoSnapshotIndex());
    RaftServerConfigKeys.Log.setPurgePreservationLogNum(
        properties, config.getLog().getPreserveNumsWhenPurge());
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, config.getLog().getSegmentSizeMax());
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(
        properties, config.getLog().getSegmentCacheNumMax());
    RaftServerConfigKeys.Log.setSegmentCacheSizeMax(
        properties, config.getLog().getSegmentCacheSizeMax());
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, config.getLog().getPreallocatedSize());
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, config.getLog().getWriteBufferSize());
    RaftServerConfigKeys.Log.setForceSyncNum(properties, config.getLog().getForceSyncNum());
    RaftServerConfigKeys.Log.setUnsafeFlushEnabled(
        properties, config.getLog().isUnsafeFlushEnabled());

    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(
        properties, config.getLeaderLogAppender().getBufferByteLimit());
    RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(
        properties, config.getLeaderLogAppender().getSnapshotChunkSizeMax());
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(
        properties, config.getLeaderLogAppender().isInstallSnapshotEnabled());

    GrpcConfigKeys.Server.setHeartbeatChannel(properties, true);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(
        properties, config.getRpc().getFirstElectionTimeoutMin());
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(
        properties, config.getRpc().getFirstElectionTimeoutMax());
  }
}
