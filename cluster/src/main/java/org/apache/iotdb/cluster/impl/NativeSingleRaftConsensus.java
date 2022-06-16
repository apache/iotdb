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

package org.apache.iotdb.cluster.impl;

import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.consensus.ISingleConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import java.io.IOException;
import java.util.List;

public class NativeSingleRaftConsensus implements ISingleConsensus {

  private RaftMember raftMember;

  @Override
  public void start() throws IOException {
    raftMember.start();
  }

  @Override
  public void stop() throws IOException {
    raftMember.stop();
  }

  @Override
  public ConsensusWriteResponse write(IConsensusRequest request) {
    return raftMember.executeRequest(request);
  }

  @Override
  public ConsensusReadResponse read(IConsensusRequest request) {
    return new ConsensusReadResponse(null, raftMember.read(request));
  }

  @Override
  public ConsensusGenericResponse addPeer(Peer peer) {
    return null;
  }

  @Override
  public ConsensusGenericResponse removePeer(Peer peer) {
    return null;
  }

  @Override
  public ConsensusGenericResponse changePeer(List<Peer> newPeers) {
    return null;
  }

  @Override
  public ConsensusGenericResponse transferLeader(Peer newLeader) {
    return null;
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot() {
    return null;
  }

  @Override
  public boolean isLeader() {
    return raftMember.isLeader();
  }

  @Override
  public Peer getLeader() {
    return raftMember.getLeaderPeer();
  }
}
