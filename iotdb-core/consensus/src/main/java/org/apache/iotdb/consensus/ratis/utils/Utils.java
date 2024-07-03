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

package org.apache.iotdb.consensus.ratis.utils;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
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
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TByteBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

public class Utils {
  private static final int TEMP_BUFFER_SIZE = 1024;
  private static final byte PADDING_MAGIC = 0x47;
  private static final String DATA_REGION_GROUP = "group-0001";
  private static final String SCHEMA_REGION_GROUP = "group-0002";
  private static final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private static final Cache<ConsensusGroupId, RaftGroupId> cache =
      CacheBuilder.newBuilder().weakValues().expireAfterAccess(5, TimeUnit.MINUTES).build();

  private Utils() {}

  public static String hostAddress(TEndPoint endpoint) {
    return String.format("%s:%d", endpoint.getIp(), endpoint.getPort());
  }

  public static String fromTEndPointToString(TEndPoint endpoint) {
    return String.format("%s_%d", endpoint.getIp(), endpoint.getPort());
  }

  /** Encode the ConsensusGroupId into 6 bytes: 2 Bytes for Group Type and 4 Bytes for Group ID. */
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

  /** Given ConsensusGroupId, generate a deterministic RaftGroupId current scheme. */
  public static RaftGroupId fromConsensusGroupIdToRaftGroupId(ConsensusGroupId consensusGroupId) {
    try {
      return cache.get(consensusGroupId, () -> valueOf(consensusGroupId));
    } catch (ExecutionException e) {
      return valueOf(consensusGroupId);
    }
  }

  private static RaftGroupId valueOf(ConsensusGroupId consensusGroupId) {
    final long groupCode = groupEncode(consensusGroupId);
    final byte[] byteGroupCode = ByteBuffer.allocate(Long.BYTES).putLong(groupCode).array();
    final byte[] bytePaddedGroupName = new byte[16];
    for (int i = 0; i < 10; i++) {
      bytePaddedGroupName[i] = PADDING_MAGIC;
    }
    System.arraycopy(byteGroupCode, 2, bytePaddedGroupName, 10, byteGroupCode.length - 2);

    return RaftGroupId.valueOf(ByteString.copyFrom(bytePaddedGroupName));
  }

  /** Given raftGroupId, decrypt ConsensusGroupId out of it. */
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

  public static TConsensusGroupType getConsensusGroupTypeFromPrefix(String prefix) {
    TConsensusGroupType consensusGroupType;
    if (prefix.contains(DATA_REGION_GROUP)) {
      consensusGroupType = TConsensusGroupType.DataRegion;
    } else if (prefix.contains(SCHEMA_REGION_GROUP)) {
      consensusGroupType = TConsensusGroupType.SchemaRegion;
    } else {
      consensusGroupType = TConsensusGroupType.ConfigRegion;
    }
    return consensusGroupType;
  }

  public static boolean rejectWrite(TConsensusGroupType type) {
    return type == TConsensusGroupType.DataRegion && config.isReadOnly();
  }

  /**
   * Normally, the RatisConsensus should reject write when system is read-only, i.e, {@link
   * #rejectWrite(TConsensusGroupType type)}. However, Ratis RaftServer close() will wait for
   * applyIndex advancing to commitIndex. So when the system is shutting down, RatisConsensus should
   * still allow statemachine to apply while rejecting new client write requests.
   */
  public static boolean stallApply(TConsensusGroupType type) {
    return type == TConsensusGroupType.DataRegion && config.isReadOnly() && !config.isStopping();
  }

  /** return the max wait duration for retry */
  static TimeDuration getMaxRetrySleepTime(RatisConfig.Client config) {
    final int maxAttempts = config.getClientMaxRetryAttempt();
    final long baseSleepMs = config.getClientRetryInitialSleepTimeMs();
    final long maxSleepMs = config.getClientRetryMaxSleepTimeMs();
    final long timeoutMs = config.getClientRequestTimeoutMillis();

    long maxWaitMs = 0L;
    long currentSleepMs = baseSleepMs;
    for (int i = 0; i < maxAttempts; i++) {
      maxWaitMs += timeoutMs;
      maxWaitMs += currentSleepMs;
      currentSleepMs = Math.min(2 * currentSleepMs, maxSleepMs);
    }

    return TimeDuration.valueOf(maxWaitMs, TimeUnit.MILLISECONDS);
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
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, config.getRpc().getRequestTimeout());
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
    // FIXME: retain 2 copies to avoid race conditions between (delete) and (transfer)
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, 2);
    RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);

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
    final SizeInBytes writeBufferSize =
        SizeInBytes.valueOf(config.getLeaderLogAppender().getBufferByteLimit().getSizeInt() + 8);
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, writeBufferSize);
    RaftServerConfigKeys.Log.setForceSyncNum(properties, config.getLog().getForceSyncNum());
    RaftServerConfigKeys.Log.setUnsafeFlushEnabled(
        properties, config.getLog().isUnsafeFlushEnabled());
    RaftServerConfigKeys.Log.setCorruptionPolicy(
        properties, RaftServerConfigKeys.Log.CorruptionPolicy.WARN_AND_RETURN);

    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(
        properties, config.getLeaderLogAppender().getBufferByteLimit());
    RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(
        properties, config.getLeaderLogAppender().getSnapshotChunkSizeMax());
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(
        properties, config.getLeaderLogAppender().isInstallSnapshotEnabled());

    GrpcConfigKeys.Server.setHeartbeatChannel(properties, true);
    GrpcConfigKeys.Server.setLogMessageBatchDuration(properties, TimeDuration.ONE_MINUTE);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(
        properties, config.getRpc().getFirstElectionTimeoutMin());
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(
        properties, config.getRpc().getFirstElectionTimeoutMax());

    /* linearizable means we can obtain consistent data from followers.
    If we prefer latency, we can directly use staleRead */
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    RaftServerConfigKeys.Read.setTimeout(properties, config.getRead().getReadTimeout());

    RaftServerConfigKeys.setSleepDeviationThreshold(
        properties, config.getUtils().getSleepDeviationThresholdMs());
    RaftServerConfigKeys.setCloseThreshold(properties, config.getUtils().getCloseThresholdMs());

    final TimeDuration clientMaxRetryGap = getMaxRetrySleepTime(config.getClient());
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, clientMaxRetryGap);
  }

  public static boolean anyOf(BooleanSupplier... conditions) {
    for (BooleanSupplier condition : conditions) {
      if (condition.getAsBoolean()) {
        return true;
      }
    }
    return false;
  }
}
