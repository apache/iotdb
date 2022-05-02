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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.common.Peer;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TByteBuffer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

public class Utils {
  private static final int tempBufferSize = 1024;
  private static final byte PADDING_MAGIC = 0x47;
  private static final long DataRegionType = 0x01;
  private static final long SchemaRegionType = 0x02;
  private static final long PartitionRegionType = 0x03;

  public static String IPAddress(TEndPoint endpoint) {
    return String.format("%s:%d", endpoint.getIp(), endpoint.getPort());
  }

  /** Encode the ConsensusGroupId into 6 bytes 2 Bytes for Group Type 4 Bytes for Group ID */
  public static long groupEncode(ConsensusGroupId consensusGroupId) {
    // use abbreviations to prevent overflow
    long groupType = 0L;
    switch (consensusGroupId.getType()) {
      case DataRegion:
        {
          groupType = DataRegionType;
          break;
        }
      case SchemaRegion:
        {
          groupType = SchemaRegionType;
          break;
        }
      case PartitionRegion:
        {
          groupType = PartitionRegionType;
          break;
        }
      default:
        {
          return -1;
        }
    }
    long groupCode = groupType << 32;
    groupCode += (long) consensusGroupId.getId();
    return groupCode;
  }

  public static String RatisPeerId(TEndPoint endpoint) {
    return String.format("%s-%d", endpoint.getIp(), endpoint.getPort());
  }

  public static TEndPoint parseFromRatisId(String ratisId) {
    String[] items = ratisId.split("-");
    return new TEndPoint(items[0], Integer.parseInt(items[1]));
  }

  // priority is used as ordinal of leader election
  public static RaftPeer toRaftPeer(TEndPoint endpoint, int priority) {
    return RaftPeer.newBuilder()
        .setId(RatisPeerId(endpoint))
        .setAddress(IPAddress(endpoint))
        .setPriority(priority)
        .build();
  }

  public static RaftPeer toRaftPeer(Peer peer, int priority) {
    return toRaftPeer(peer.getEndpoint(), priority);
  }

  public static TEndPoint getEndpoint(RaftPeer raftPeer) {
    String address = raftPeer.getAddress(); // ip:port
    String[] split = address.split(":");
    return new TEndPoint(split[0], Integer.parseInt(split[1]));
  }

  /** Given ConsensusGroupId, generate a deterministic RaftGroupId current scheme: */
  public static RaftGroupId toRatisGroupId(ConsensusGroupId consensusGroupId) {
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
  public static ConsensusGroupId toConsensusGroupId(RaftGroupId raftGroupId) {
    byte[] padded = raftGroupId.toByteString().toByteArray();
    long type = (padded[10] << 8) + padded[11];
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
    byteBuffer.put(padded, 12, 4);
    byteBuffer.flip();
    int gid = byteBuffer.getInt();
    ConsensusGroupId id;

    if (type == DataRegionType) {
      id = new DataRegionId(gid);
    } else if (type == PartitionRegionType) {
      id = new PartitionRegionId(gid);
    } else if (type == SchemaRegionType) {
      id = new SchemaRegionId(gid);
    } else {
      throw new IllegalArgumentException(
          String.format("Unexpected consensusGroupId Type %d", type));
    }
    return id;
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

  public static ByteBuffer getMetadataFromTermIndex(TermIndex termIndex) {
    String ordinal = String.format("%d_%d", termIndex.getTerm(), termIndex.getIndex());
    ByteBuffer metadata = ByteBuffer.wrap(ordinal.getBytes());
    return metadata;
  }

  public static TermIndex getTermIndexFromMetadata(ByteBuffer metadata) {
    Charset charset = Charset.defaultCharset();
    CharBuffer charBuffer = charset.decode(metadata);
    String ordinal = charBuffer.toString();
    String[] items = ordinal.split("_");
    return TermIndex.valueOf(Long.parseLong(items[0]), Long.parseLong(items[1]));
  }
}
