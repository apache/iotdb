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

import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.GroupType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TByteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Utils {
  private static final int tempBufferSize = 1024;
  private static final byte PADDING_MAGIC = 0x47;
  private static final String DataRegionAbbr = "DR";
  private static final String SchemaRegionAbbr = "SR";
  private static final String PartitionRegionAbbr = "PR";

  public static String IP_PORT(Endpoint endpoint) {
    return String.format("%s:%d", endpoint.getIp(), endpoint.getPort());
  }

  public static String groupFullName(ConsensusGroupId consensusGroupId) {
    // use abbreviations to prevent overflow
    String groupTypeAbbr = null;
    switch (consensusGroupId.getType()) {
      case DataRegion:
        {
          groupTypeAbbr = DataRegionAbbr;
          break;
        }
      case SchemaRegion:
        {
          groupTypeAbbr = SchemaRegionAbbr;
          break;
        }
      case PartitionRegion:
        {
          groupTypeAbbr = PartitionRegionAbbr;
          break;
        }
    }
    return String.format("%s-%d", groupTypeAbbr, consensusGroupId.getId());
  }

  public static RaftPeer toRaftPeer(Endpoint endpoint) {
    String Id = String.format("%s-%d", endpoint.getIp(), endpoint.getPort());
    return RaftPeer.newBuilder().setId(Id).setAddress(IP_PORT(endpoint)).build();
  }

  public static RaftPeer toRaftPeer(Peer peer) {
    return toRaftPeer(peer.getEndpoint());
  }

  public static Endpoint getEndPoint(RaftPeer raftPeer) {
    String address = raftPeer.getAddress(); // ip:port
    String[] split = address.split(":");
    return new Endpoint(split[0], Integer.parseInt(split[1]));
  }

  /** Given ConsensusGroupId, generate a deterministic RaftGroupId current scheme: */
  public static RaftGroupId toRatisGroupId(ConsensusGroupId consensusGroupId) {
    String groupFullName = groupFullName(consensusGroupId);
    byte[] bGroupName = groupFullName.getBytes(StandardCharsets.UTF_8);
    byte[] bPaddedGroupName = Arrays.copyOf(bGroupName, 16);
    for (int i = bGroupName.length; i < 16; i++) {
      bPaddedGroupName[i] = PADDING_MAGIC;
    }

    return RaftGroupId.valueOf(ByteString.copyFrom(bPaddedGroupName));
  }

  /** Given raftGroupId, decrypt ConsensusGroupId out of it */
  public static ConsensusGroupId toConsensusGroupId(RaftGroupId raftGroupId) {
    byte[] padded = raftGroupId.toByteString().toByteArray();
    int validOffset = padded.length - 1;
    while (padded[validOffset] == PADDING_MAGIC) {
      validOffset--;
    }
    String consensusGroupString = new String(padded, 0, validOffset + 1);
    String[] items = consensusGroupString.split("-");
    GroupType groupType = null;
    switch (items[0]) {
      case DataRegionAbbr:
        {
          groupType = GroupType.DataRegion;
          break;
        }
      case PartitionRegionAbbr:
        {
          groupType = GroupType.PartitionRegion;
          break;
        }
      case SchemaRegionAbbr:
        {
          groupType = GroupType.SchemaRegion;
          break;
        }
    }
    return new ConsensusGroupId(groupType, Long.parseLong(items[1]));
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
}
