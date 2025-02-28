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

package org.apache.iotdb.consensus.common;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

public class Peer implements Comparable<Peer> {

  private final Logger logger = LoggerFactory.getLogger(Peer.class);
  private final ConsensusGroupId groupId;
  private final int nodeId;
  private final TEndPoint endpoint;

  public Peer(ConsensusGroupId groupId, int nodeId, TEndPoint endpoint) {
    this.groupId = groupId;
    this.nodeId = nodeId;
    this.endpoint = endpoint;
  }

  public ConsensusGroupId getGroupId() {
    return groupId;
  }

  public TEndPoint getEndpoint() {
    return endpoint;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void serialize(DataOutputStream stream) {
    try {
      ThriftCommonsSerDeUtils.serializeTConsensusGroupId(
          groupId.convertToTConsensusGroupId(), stream);
      BasicStructureSerDeUtil.write(nodeId, stream);
      ThriftCommonsSerDeUtils.serializeTEndPoint(endpoint, stream);
    } catch (IOException e) {
      logger.error("Failed to serialize Peer", e);
    }
  }

  public static Peer deserialize(ByteBuffer buffer) {
    return new Peer(
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer)),
        BasicStructureSerDeUtil.readInt(buffer),
        ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Peer peer = (Peer) o;
    return nodeId == peer.nodeId
        && Objects.equals(groupId, peer.groupId)
        && Objects.equals(endpoint, peer.endpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, endpoint, nodeId);
  }

  @Override
  public String toString() {
    return "Peer{" + "groupId=" + groupId + ", endpoint=" + endpoint + ", nodeId=" + nodeId + '}';
  }

  @Override
  public int compareTo(Peer peer) {
    return Comparator.comparing(Peer::getGroupId)
        .thenComparingInt(Peer::getNodeId)
        .thenComparing(Peer::getEndpoint)
        .compare(this, peer);
  }

  public static Peer valueOf(
      TConsensusGroupId consensusGroupId, TDataNodeLocation dataNodeLocation) {
    if (consensusGroupId.getType() == TConsensusGroupType.SchemaRegion) {
      return new Peer(
          new SchemaRegionId(consensusGroupId.getId()),
          dataNodeLocation.getDataNodeId(),
          dataNodeLocation.getSchemaRegionConsensusEndPoint());
    } else if (consensusGroupId.getType() == TConsensusGroupType.DataRegion) {
      return new Peer(
          new DataRegionId(consensusGroupId.getId()),
          dataNodeLocation.getDataNodeId(),
          dataNodeLocation.getDataRegionConsensusEndPoint());
    }
    throw new UnsupportedOperationException();
  }
}
