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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** DataNode Consensus layer manager */
public class ConsensusManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusManager.class);

  private IConsensus consensusImpl;

  public ConsensusManager() throws IOException {
    consensusImpl = ConsensusImpl.getInstance();
    consensusImpl.start();
  }

  public void addConsensusGroup(RegionReplicaSet regionReplicaSet) {
    ConsensusGroupId consensusGroupId = regionReplicaSet.getConsensusGroupId();
    List<Peer> peerList = new ArrayList<>();
    for (DataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeList()) {
      peerList.add(new Peer(consensusGroupId, dataNodeLocation.getEndPoint()));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);
  }

  /** Transmit FragmentInstance to datanode.consensus.statemachine */
  public ConsensusWriteResponse write(ConsensusGroupId consensusGroupId, IConsensusRequest plan) {
    return consensusImpl.write(consensusGroupId, plan);
  }

  /** Transmit FragmentInstance to datanode.consensus.statemachine */
  public ConsensusReadResponse read(ConsensusGroupId consensusGroupId, FragmentInstance plan) {
    return consensusImpl.read(consensusGroupId, plan);
  }

  public void close() throws IOException {
    consensusImpl.stop();
  }
}
