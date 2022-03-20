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

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;

public class Utils {
  private static final String IOTDB_RATIS_KEY = "iotdb@_ratis_key";
  private static final SecretKeySpec key = new SecretKeySpec(IOTDB_RATIS_KEY.getBytes(), "AES");

  public static String IP_PORT(Endpoint endpoint) {
    return String.format("%s:%d", endpoint.getIp(), endpoint.getPort());
  }

  public static String groupFullName(ConsensusGroupId consensusGroupId) {
    return String.format("%s-%d", consensusGroupId.getType().toString(), consensusGroupId.getId());
  }

  public static RaftPeer toRaftPeer(Endpoint endpoint) {
    String Id = String.format("%s-%d", endpoint.getIp(), endpoint.getPort());
    return RaftPeer.newBuilder().setId(Id).setAddress(IP_PORT(endpoint)).build();
  }

  public static RaftPeer toRaftPeer(Peer peer) {
    return toRaftPeer(peer.getEndpoint());
  }

  /**
   * Given ConsensusGroupId, generate a deterministic RaftGroupId current scheme:
   * AES/ECB/PKCS5Padding of (GroupType-Id), key = iotdb@_ratis_key
   */
  public static RaftGroupId toRatisGroupId(ConsensusGroupId consensusGroupId) {
    String groupFullName = groupFullName(consensusGroupId);

    byte[] AESEncrypted = new byte[] {};
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      AESEncrypted = cipher.doFinal(groupFullName.getBytes());
    } catch (Exception ignored) {
    }

    return RaftGroupId.valueOf(ByteString.copyFrom(AESEncrypted));
  }

  /** Given raftGroupId, decrypt ConsensusGroupId out of it */
  public static ConsensusGroupId toConsensusGroupId(RaftGroupId raftGroupId) {
    byte[] AESDecrypted = new byte[] {};
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.DECRYPT_MODE, key);
      AESDecrypted = cipher.doFinal(raftGroupId.toByteString().toByteArray());
    } catch (Exception ignored) {
    }
    String consensusGroupString = new String(AESDecrypted);
    String[] items = consensusGroupString.split("-");
    return new ConsensusGroupId(GroupType.valueOf(items[0]), Long.parseLong(items[1]));
  }

  public static ByteBuffer serializeTSStatus(TSStatus status) throws TException {
    // TODO Pooling ByteBuffer
    TByteBuffer byteBuffer = new TByteBuffer(ByteBuffer.allocate(1000));
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
