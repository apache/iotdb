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

import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
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
  private static final int tempBufferSize = 1024;
  private static final byte PADDING_MAGIC = 0x47;

  private Utils() {}

  public static String HostAddress(TEndPoint endpoint) {
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

  public static RaftPeerId fromTEndPointToRaftPeerId(TEndPoint endpoint) {
    return RaftPeerId.valueOf(fromTEndPointToString(endpoint));
  }

  public static TEndPoint formRaftPeerIdToTEndPoint(RaftPeerId id) {
    String[] items = id.toString().split("_");
    return new TEndPoint(items[0], Integer.parseInt(items[1]));
  }

  public static TEndPoint formRaftPeerProtoToTEndPoint(RaftPeerProto proto) {
    String[] items = proto.getAddress().split(":");
    return new TEndPoint(items[0], Integer.parseInt(items[1]));
  }

  // priority is used as ordinal of leader election
  public static RaftPeer fromTEndPointAndPriorityToRaftPeer(TEndPoint endpoint, int priority) {
    return RaftPeer.newBuilder()
        .setId(fromTEndPointToRaftPeerId(endpoint))
        .setAddress(HostAddress(endpoint))
        .setPriority(priority)
        .build();
  }

  public static RaftPeer fromTEndPointAndPriorityToRaftPeer(Peer peer, int priority) {
    return fromTEndPointAndPriorityToRaftPeer(peer.getEndpoint(), priority);
  }

  public static List<RaftPeer> fromPeersAndPriorityToRaftPeers(List<Peer> peers, int priority) {
    return peers.stream()
        .map(peer -> Utils.fromTEndPointAndPriorityToRaftPeer(peer, priority))
        .collect(Collectors.toList());
  }

  public static List<Peer> fromRaftProtoListAndRaftGroupIdToPeers(
      List<RaftPeerProto> raftProtoList, RaftGroupId id) {
    ConsensusGroupId consensusGroupId = Utils.fromRaftGroupIdToConsensusGroupId(id);
    return raftProtoList.stream()
        .map(peer -> new Peer(consensusGroupId, Utils.formRaftPeerProtoToTEndPoint(peer)))
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
    // TODO Pooling ByteBuffer
    TByteBuffer byteBuffer = new TByteBuffer(ByteBuffer.allocate(tempBufferSize));
    TCompactProtocol protocol = new TCompactProtocol(byteBuffer);
    status.write(protocol);
    byteBuffer.getByteBuffer().flip();
    return byteBuffer.getByteBuffer();
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
}
