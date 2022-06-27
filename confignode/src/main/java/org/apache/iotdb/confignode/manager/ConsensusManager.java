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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodeReq;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.config.ConsensusConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** ConsensusManager maintains consensus class, request will redirect to consensus layer */
public class ConsensusManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusManager.class);
  private static final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private final IManager configManager;

  private ConsensusGroupId consensusGroupId;
  private IConsensus consensusImpl;

  public ConsensusManager(IManager configManager, PartitionRegionStateMachine stateMachine)
      throws IOException {
    this.configManager = configManager;
    setConsensusLayer(stateMachine);
  }

  public void close() throws IOException {
    consensusImpl.stop();
  }

  /** Build ConfigNodeGroup ConsensusLayer */
  private void setConsensusLayer(PartitionRegionStateMachine stateMachine) throws IOException {
    // There is only one ConfigNodeGroup
    consensusGroupId = new PartitionRegionId(conf.getPartitionRegionId());

    // Consensus local implement
    consensusImpl =
        ConsensusFactory.getConsensusImpl(
                conf.getConfigNodeConsensusProtocolClass(),
                ConsensusConfig.newBuilder()
                    .setThisNode(new TEndPoint(conf.getRpcAddress(), conf.getConsensusPort()))
                    .setStorageDir(conf.getConsensusDir())
                    .build(),
                gid -> stateMachine)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            conf.getConfigNodeConsensusProtocolClass())));
    consensusImpl.start();

    // if does not start firstly, or is seed-node. will add ConsensusGroup.
    if (!conf.isNeedApply()) {
      addConsensusGroup(conf.getConfigNodeList());
    }
  }

  /**
   * after register config node, leader will call addConsensusGroup remotely. execute in new node
   *
   * @param configNodeLocations all config node
   */
  public void addConsensusGroup(List<TConfigNodeLocation> configNodeLocations) {
    if (configNodeLocations.size() == 0) {
      LOGGER.warn("configNodeLocations is null");
      return;
    }

    LOGGER.info("Set ConfigNode consensus group {}...", configNodeLocations);
    List<Peer> peerList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      peerList.add(new Peer(consensusGroupId, configNodeLocation.getConsensusEndPoint()));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);

    // Set config node list
    conf.setConfigNodeList(configNodeLocations);
  }

  /**
   * Apply new ConfigNode Peer into PartitionRegion
   *
   * @param applyConfigNodeReq ApplyConfigNodeReq
   * @return True if successfully addPeer. False if another ConfigNode is being added to the
   *     PartitionRegion
   */
  public boolean addConfigNodePeer(ApplyConfigNodeReq applyConfigNodeReq) {
    return consensusImpl
        .addPeer(
            consensusGroupId,
            new Peer(
                consensusGroupId,
                applyConfigNodeReq.getConfigNodeLocation().getConsensusEndPoint()))
        .isSuccess();
  }

  /**
   * Remove a ConfigNode Peer out of PartitionRegion
   *
   * @param removeConfigNodeReq RemoveConfigNodeReq
   * @return True if successfully removePeer. False if another ConfigNode is being removed to the
   *     PartitionRegion
   */
  public boolean removeConfigNodePeer(RemoveConfigNodeReq removeConfigNodeReq) {
    return consensusImpl
        .removePeer(
            consensusGroupId,
            new Peer(
                consensusGroupId,
                removeConfigNodeReq.getConfigNodeLocation().getConsensusEndPoint()))
        .isSuccess();
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusWriteResponse write(ConfigRequest req) {
    return consensusImpl.write(consensusGroupId, req);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusReadResponse read(ConfigRequest req) {
    return consensusImpl.read(consensusGroupId, req);
  }

  public boolean isLeader() {
    return consensusImpl.isLeader(consensusGroupId);
  }

  /** @return ConfigNode-leader's location if leader exists, null otherwise. */
  public TConfigNodeLocation getLeader() {
    for (int retry = 0; retry < 50; retry++) {
      Peer leaderPeer = consensusImpl.getLeader(consensusGroupId);
      if (leaderPeer != null) {
        List<TConfigNodeLocation> onlineConfigNodes = getNodeManager().getOnlineConfigNodes();
        TConfigNodeLocation leaderLocation =
            onlineConfigNodes.stream()
                .filter(leader -> leader.getConsensusEndPoint().equals(leaderPeer.getEndpoint()))
                .findFirst()
                .orElse(null);
        if (leaderLocation != null) {
          return leaderLocation;
        }
      }

      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.warn("ConsensusManager getLeader been interrupted, ", e);
      }
    }
    return null;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public IConsensus getConsensusImpl() {
    return consensusImpl;
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  @TestOnly
  public void singleCopyMayWaitUntilLeaderReady() {
    if (conf.getConfigNodeList().size() == 1) {
      long startTime = System.currentTimeMillis();
      long maxWaitTime = 1000 * 60; // milliseconds, which is 60s
      try {
        while (!consensusImpl.isLeader(consensusGroupId)) {
          TimeUnit.MILLISECONDS.sleep(100);
          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > maxWaitTime) {
            return;
          }
        }
      } catch (InterruptedException ignored) {
      }
    }
  }
}
