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
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import java.util.List;

public class RatisConsensus implements IConsensus {

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public ConsensusWriteResponse Write(
      ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {
    return null;
  }

  @Override
  public ConsensusReadResponse Read(ConsensusGroupId groupId, IConsensusRequest IConsensusRequest) {
    return null;
  }

  @Override
  public ConsensusGenericResponse AddConsensusGroup(ConsensusGroupId groupId, List<Peer> peers) {
    return null;
  }

  @Override
  public ConsensusGenericResponse RemoveConsensusGroup(ConsensusGroupId groupId) {
    return null;
  }

  @Override
  public ConsensusGenericResponse AddPeer(ConsensusGroupId groupId, Peer peer) {
    return null;
  }

  @Override
  public ConsensusGenericResponse RemovePeer(ConsensusGroupId groupId, Peer peer) {
    return null;
  }

  @Override
  public ConsensusGenericResponse ChangePeer(ConsensusGroupId groupId, List<Peer> peers) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse TransferLeader(ConsensusGroupId groupId, Peer newPeer) {
    return null;
  }

  @Override
  public ConsensusGenericResponse TriggerSnapshot(ConsensusGroupId groupId) {
    return null;
  }
}
