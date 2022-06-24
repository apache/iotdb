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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
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
import org.apache.iotdb.rpc.TSStatusCode;

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
  private ConsensusGroupId consensusGroupId;
  private IConsensus consensusImpl;

  public ConsensusManager(PartitionRegionStateMachine stateMachine) throws IOException {
    setConsensusLayer(stateMachine);
  }

  public void close() throws IOException {
    consensusImpl.stop();
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

    // Build consensus group from iotdb-confignode.properties
    LOGGER.info("Set ConfigNode consensus group {}...", conf.getConfigNodeList());
    List<Peer> peerList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : conf.getConfigNodeList()) {
      peerList.add(new Peer(consensusGroupId, configNodeLocation.getConsensusEndPoint()));
    }
    consensusImpl.addConsensusGroup(consensusGroupId, peerList);

    // Apply ConfigNode if necessary
    if (conf.isNeedApply()) {
      TSStatus status =
          SyncConfigNodeClientPool.getInstance()
              .applyConfigNode(
                  conf.getTargetConfigNode(),
                  new TConfigNodeLocation(
                      -1,
                      new TEndPoint(conf.getRpcAddress(), conf.getRpcPort()),
                      new TEndPoint(conf.getRpcAddress(), conf.getConsensusPort())));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(status.getMessage());
        throw new IOException("Apply ConfigNode failed:");
      }
    }
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

  public Peer getLeader(List<TConfigNodeLocation> onlineConfigNodes) {
    Peer leader = consensusImpl.getLeader(consensusGroupId);

    TConfigNodeLocation nodeLocation =
        onlineConfigNodes.stream()
            .filter(e -> e.getConsensusEndPoint().equals(leader.getEndpoint()))
            .findFirst()
            .get();
    return new Peer(consensusGroupId, nodeLocation.getInternalEndPoint());
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public IConsensus getConsensusImpl() {
    return consensusImpl;
  }
}
