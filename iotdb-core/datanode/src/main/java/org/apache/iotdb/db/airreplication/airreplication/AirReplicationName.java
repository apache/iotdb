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

package org.apache.iotdb.consensus.air.airreplication;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.consensus.common.Peer;

import java.util.Objects;

public class AirReplicationName {
  private static final String REPLICATION_AIR_NAME_SPLITTER_CHAR = "_";
  private final ConsensusGroupId consensusGroupId;
  private final int senderDataNodeId;
  private final int receiverDataNodeId;

  public AirReplicationName(Peer senderPeer, Peer receiverPeer) {
    this.consensusGroupId = senderPeer.getGroupId();
    this.senderDataNodeId = senderPeer.getNodeId();
    this.receiverDataNodeId = receiverPeer.getNodeId();
  }

  public AirReplicationName(
      ConsensusGroupId consensusGroupId, int senderDataNodeId, int receiverDataNodeId) {
    this.consensusGroupId = consensusGroupId;
    this.senderDataNodeId = senderDataNodeId;
    this.receiverDataNodeId = receiverDataNodeId;
  }

  public AirReplicationName(String airName) throws IllegalArgumentException {
    if (!airName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      throw new IllegalArgumentException("Invalid air name: " + airName);
    }
    String[] airNameParts =
        airName
            .substring(PipeStaticMeta.CONSENSUS_PIPE_PREFIX.length())
            .split(REPLICATION_AIR_NAME_SPLITTER_CHAR);
    if (airNameParts.length != 3) {
      throw new IllegalArgumentException("Invalid air name: " + airName);
    }
    this.consensusGroupId = ConsensusGroupId.Factory.createFromString(airNameParts[0]);
    this.senderDataNodeId = Integer.parseInt(airNameParts[1]);
    this.receiverDataNodeId = Integer.parseInt(airNameParts[2]);
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public int getSenderDataNodeId() {
    return senderDataNodeId;
  }

  public int getReceiverDataNodeId() {
    return receiverDataNodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AirReplicationName that = (AirReplicationName) o;
    return Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(senderDataNodeId, that.senderDataNodeId)
        && Objects.equals(receiverDataNodeId, that.receiverDataNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consensusGroupId, senderDataNodeId, receiverDataNodeId);
  }

  @Override
  public String toString() {
    return String.join(
        REPLICATION_AIR_NAME_SPLITTER_CHAR,
        PipeStaticMeta.CONSENSUS_PIPE_PREFIX + consensusGroupId,
        String.valueOf(senderDataNodeId),
        String.valueOf(receiverDataNodeId));
  }
}
