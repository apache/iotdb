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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** ConsensusManager maintains consensus class, request will redirect to consensus layer */
public class ConsensusManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusManager.class);
  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();

  private ConsensusGroupId consensusGroupId;
  private IConsensus consensusImpl;

  public ConsensusManager() throws IOException {
    setConsensusLayer();
  }

  public void close() throws IOException {
    consensusImpl.stop();
  }

  /** Build ConfigNodeGroup ConsensusLayer */
  private void setConsensusLayer() throws IOException {
    // There is only one ConfigNodeGroup
    consensusGroupId = new PartitionRegionId(0);

    // Ratis consensus local implement
    consensusImpl =
        ConsensusFactory.getConsensusImpl(
                conf.getConfigNodeConsensusProtocolClass(),
                new TEndPoint(conf.getRpcAddress(), conf.getInternalPort()),
                new File(conf.getConsensusDir()),
                gid -> new PartitionRegionStateMachine())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            conf.getConfigNodeConsensusProtocolClass())));
    consensusImpl.start();

    // Build ratis group from user properties
    LOGGER.info(
        "Set ConfigNode consensus group {}...",
        Arrays.toString(conf.getConfigNodeGroupAddressList()));
    List<Peer> peerList = new ArrayList<>();
    for (TEndPoint endpoint : conf.getConfigNodeGroupAddressList()) {
      peerList.add(new Peer(consensusGroupId, endpoint));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusWriteResponse write(ConfigRequest plan) {
    return consensusImpl.write(consensusGroupId, plan);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusReadResponse read(ConfigRequest plan) {
    return consensusImpl.read(consensusGroupId, plan);
  }

  public boolean isLeader() {
    return consensusImpl.isLeader(consensusGroupId);
  }

  public Peer getLeader() {
    return consensusImpl.getLeader(consensusGroupId);
  }

  // TODO: Interfaces for LoadBalancer control
}
