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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.confignode.exception.AddPeerException;
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
import java.util.Collections;
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

  /** ConsensusLayer local implementation */
  private void setConsensusLayer(PartitionRegionStateMachine stateMachine) throws IOException {
    // There is only one ConfigNodeGroup
    consensusGroupId = new PartitionRegionId(conf.getPartitionRegionId());

    // Implement local ConsensusLayer by ConfigNodeConfig
    consensusImpl =
        ConsensusFactory.getConsensusImpl(
                conf.getConfigNodeConsensusProtocolClass(),
                ConsensusConfig.newBuilder()
                    .setThisNode(new TEndPoint(conf.getInternalAddress(), conf.getConsensusPort()))
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

    if (SystemPropertiesUtils.isRestarted()) {
      try {
        // Create ConsensusGroup from confignode-system.properties file when restart
        // TODO: Check and notify if current ConfigNode's ip or port has changed
        addConsensusGroup(SystemPropertiesUtils.loadConfigNodeList());
      } catch (BadNodeUrlException e) {
        throw new IOException(e);
      }
    } else if (ConfigNodeDescriptor.getInstance().isSeedConfigNode()) {
      // Create ConsensusGroup that contains only itself
      // if the current ConfigNode is Seed-ConfigNode
      addConsensusGroup(
          Collections.singletonList(
              new TConfigNodeLocation(
                  0,
                  new TEndPoint(conf.getInternalAddress(), conf.getInternalPort()),
                  new TEndPoint(conf.getInternalAddress(), conf.getConsensusPort()))));
    }
  }

  /**
   * Add the current ConfigNode to the ConsensusGroup
   *
   * @param configNodeLocations All registered ConfigNodes
   */
  public void addConsensusGroup(List<TConfigNodeLocation> configNodeLocations) {
    if (configNodeLocations.size() == 0) {
      LOGGER.warn("configNodeLocations is null, addConsensusGroup failed.");
      return;
    }

    LOGGER.info("Set ConfigNode consensus group {}...", configNodeLocations);
    List<Peer> peerList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      peerList.add(new Peer(consensusGroupId, configNodeLocation.getConsensusEndPoint()));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);
  }

  /**
   * Add new ConfigNode Peer into PartitionRegion
   *
   * @param configNodeLocation The new ConfigNode
   * @throws AddPeerException When addPeer doesn't success
   */
  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
    boolean result =
        consensusImpl
            .addPeer(
                consensusGroupId,
                new Peer(consensusGroupId, configNodeLocation.getConsensusEndPoint()))
            .isSuccess();

    if (!result) {
      throw new AddPeerException(configNodeLocation);
    }
  }

  /**
   * Remove a ConfigNode Peer out of PartitionRegion
   *
   * @param removeConfigNodePlan RemoveConfigNodeReq
   * @return True if successfully removePeer. False if another ConfigNode is being removed to the
   *     PartitionRegion
   */
  public boolean removeConfigNodePeer(RemoveConfigNodePlan removeConfigNodePlan) {
    return consensusImpl
        .removePeer(
            consensusGroupId,
            new Peer(
                consensusGroupId,
                removeConfigNodePlan.getConfigNodeLocation().getConsensusEndPoint()))
        .isSuccess();
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusWriteResponse write(ConfigPhysicalPlan req) {
    return consensusImpl.write(consensusGroupId, req);
  }

  /** Transmit PhysicalPlan to confignode.consensus.statemachine */
  public ConsensusReadResponse read(ConfigPhysicalPlan req) {
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
        List<TConfigNodeLocation> onlineConfigNodes = getNodeManager().getRegisteredConfigNodes();
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
