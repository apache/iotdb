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

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.ConsensusType;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.GroupType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.ratis.RatisConsensus;
import org.apache.iotdb.consensus.standalone.StandAloneConsensus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.BaseStateMachine;
import org.apache.iotdb.db.consensus.statemachine.DataRegionStateMachine;
import org.apache.iotdb.db.consensus.statemachine.SchemaRegionStateMachine;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConsensusLayerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusLayerManager.class);

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private static final String rpcAddress = conf.getRpcAddress();

  private static final int rpcPort = conf.getRpcPort();

  private static final ConsensusType consensusType = conf.getConsensusType();

  private static final String consensusDir = conf.getConsensusDir();

  private IConsensus consensusImpl;

  private ConsensusGroupId consensusGroupId;

  private RegionReplicaSet regionReplicaSet;

  public ConsensusLayerManager(GroupType groupType) throws IOException {
    // either GroupType.DataRegion or GroupType.SchemaRegion
    BaseStateMachine stateMachine =
        groupType == GroupType.DataRegion
            ? new DataRegionStateMachine()
            : new SchemaRegionStateMachine();
    switch (consensusType) {
      case STANDALONE:
        consensusImpl = new StandAloneConsensus(id -> stateMachine);
        break;
      case RATIS:
        consensusImpl =
            RatisConsensus.newBuilder()
                .setEndpoint(new Endpoint(rpcAddress, rpcPort))
                .setStateMachineRegistry(id -> stateMachine)
                .setStorageDir(new File(consensusDir))
                .build();
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized ConsensusType: " + consensusType.getTypeName());
    }
    consensusImpl.start();
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public ConsensusLayerManager setConsensusGroupId(ConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    return this;
  }

  public RegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public ConsensusLayerManager setRegionReplicaSet(RegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
    return this;
  }

  public void addConsensusGroup() {
    switch (consensusType) {
      case STANDALONE:
        consensusImpl.addConsensusGroup(
            consensusGroupId,
            Collections.singletonList(
                new Peer(consensusGroupId, new Endpoint(rpcAddress, rpcPort))));
        break;
      case RATIS:
        LOGGER.info(
            "Set DataNode consensus group {}.", regionReplicaSet.getEndPointList().toString());
        List<Peer> peerList = new ArrayList<>();
        for (Endpoint endpoint : regionReplicaSet.getEndPointList()) {
          peerList.add(new Peer(consensusGroupId, endpoint));
        }
        consensusImpl.addConsensusGroup(consensusGroupId, peerList);
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecongnized ConsensusType: " + consensusType.getTypeName());
    }
  }

  public ConsensusType getConsensusType() {
    return consensusType;
  }

  /** Transmit FragmentInstance to datanode.consensus.statemachine */
  public ConsensusWriteResponse write(FragmentInstance plan) {
    return consensusImpl.write(consensusGroupId, plan);
  }

  /** Transmit FragmentInstance to datanode.consensus.statemachine */
  public ConsensusReadResponse read(FragmentInstance plan) {
    return consensusImpl.read(consensusGroupId, plan);
  }
}
