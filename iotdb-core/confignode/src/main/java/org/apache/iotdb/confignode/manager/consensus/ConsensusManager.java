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

package org.apache.iotdb.confignode.manager.consensus;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.consensus.statemachine.ConfigRegionStateMachine;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;

/** ConsensusManager maintains consensus class, request will redirect to consensus layer. */
public class ConsensusManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusManager.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONF = CommonDescriptor.getInstance().getConfig();
  private static final int SEED_CONFIG_NODE_ID = 0;
  private static final long MAX_WAIT_READY_TIME_MS =
      CommonDescriptor.getInstance().getConfig().getConnectionTimeoutInMS() / 2;
  private static final long RETRY_WAIT_TIME_MS = 100;

  /** There is only one ConfigNodeGroup */
  public static final ConsensusGroupId DEFAULT_CONSENSUS_GROUP_ID =
      new ConfigRegionId(CONF.getConfigRegionId());

  private final IManager configManager;
  private IConsensus consensusImpl;

  private boolean isInitialized;

  public ConsensusManager(IManager configManager, ConfigRegionStateMachine stateMachine) {
    this.configManager = configManager;
    setConsensusLayer(stateMachine);
  }

  public void start() throws IOException {
    consensusImpl.start();
    if (SystemPropertiesUtils.isRestarted()) {
      LOGGER.info("Init ConsensusManager successfully when restarted");
    } else if (ConfigNodeDescriptor.getInstance().isSeedConfigNode()) {
      // Create ConsensusGroup that contains only itself
      // if the current ConfigNode is Seed-ConfigNode
      try {
        createPeerForConsensusGroup(
            Collections.singletonList(
                new TConfigNodeLocation(
                    SEED_CONFIG_NODE_ID,
                    new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
                    new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort()))));
      } catch (ConsensusException e) {
        LOGGER.error(
            "Something wrong happened while calling consensus layer's createLocalPeer API.", e);
      }
    }
    isInitialized = true;
  }

  public void close() throws IOException {
    consensusImpl.stop();
  }

  /** ConsensusLayer local implementation. */
  private void setConsensusLayer(ConfigRegionStateMachine stateMachine) {
    if (SIMPLE_CONSENSUS.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      upgrade();
      consensusImpl =
          ConsensusFactory.getConsensusImpl(
                  SIMPLE_CONSENSUS,
                  ConsensusConfig.newBuilder()
                      .setThisNode(
                          new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort()))
                      .setStorageDir(CONF.getConsensusDir())
                      .setConsensusGroupType(TConsensusGroupType.ConfigRegion)
                      .build(),
                  gid -> stateMachine)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(ConsensusFactory.CONSTRUCT_FAILED_MSG, SIMPLE_CONSENSUS)));
    } else {
      // Implement local ConsensusLayer by ConfigNodeConfig
      consensusImpl =
          ConsensusFactory.getConsensusImpl(
                  CONF.getConfigNodeConsensusProtocolClass(),
                  ConsensusConfig.newBuilder()
                      .setThisNodeId(CONF.getConfigNodeId())
                      .setThisNode(
                          new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort()))
                      .setConsensusGroupType(TConsensusGroupType.ConfigRegion)
                      .setRatisConfig(
                          RatisConfig.newBuilder()
                              .setLeaderLogAppender(
                                  RatisConfig.LeaderLogAppender.newBuilder()
                                      .setBufferByteLimit(
                                          CONF.getConfigNodeRatisConsensusLogAppenderBufferSize())
                                      .build())
                              .setSnapshot(
                                  RatisConfig.Snapshot.newBuilder()
                                      .setAutoTriggerThreshold(
                                          CONF.getConfigNodeRatisSnapshotTriggerThreshold())
                                      .build())
                              .setLog(
                                  RatisConfig.Log.newBuilder()
                                      .setUnsafeFlushEnabled(
                                          CONF.isConfigNodeRatisLogUnsafeFlushEnable())
                                      .setForceSyncNum(CONF.getConfigNodeRatisLogForceSyncNum())
                                      .setSegmentCacheSizeMax(
                                          SizeInBytes.valueOf(
                                              CONF.getConfigNodeRatisLogSegmentSizeMax()))
                                      .setPreserveNumsWhenPurge(
                                          CONF.getConfigNodeRatisPreserveLogsWhenPurge())
                                      .build())
                              .setGrpc(
                                  RatisConfig.Grpc.newBuilder()
                                      .setFlowControlWindow(
                                          SizeInBytes.valueOf(
                                              CONF.getConfigNodeRatisGrpcFlowControlWindow()))
                                      .setLeaderOutstandingAppendsMax(
                                          CONF.getConfigNodeRatisGrpcLeaderOutstandingAppendsMax())
                                      .build())
                              .setRpc(
                                  RatisConfig.Rpc.newBuilder()
                                      .setTimeoutMin(
                                          TimeDuration.valueOf(
                                              CONF
                                                  .getConfigNodeRatisRpcLeaderElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setTimeoutMax(
                                          TimeDuration.valueOf(
                                              CONF
                                                  .getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setRequestTimeout(
                                          TimeDuration.valueOf(
                                              CONF.getConfigNodeRatisRequestTimeoutMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setSlownessTimeout(
                                          TimeDuration.valueOf(
                                              CONF.getConfigNodeRatisRequestTimeoutMs() * 6,
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMin(
                                          TimeDuration.valueOf(
                                              CONF.getRatisFirstElectionTimeoutMinMs(),
                                              TimeUnit.MILLISECONDS))
                                      .setFirstElectionTimeoutMax(
                                          TimeDuration.valueOf(
                                              CONF.getRatisFirstElectionTimeoutMaxMs(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .setClient(
                                  RatisConfig.Client.newBuilder()
                                      .setClientRequestTimeoutMillis(
                                          CONF.getConfigNodeRatisRequestTimeoutMs())
                                      .setClientMaxRetryAttempt(
                                          CONF.getConfigNodeRatisMaxRetryAttempts())
                                      .setClientRetryInitialSleepTimeMs(
                                          CONF.getConfigNodeRatisInitialSleepTimeMs())
                                      .setClientRetryMaxSleepTimeMs(
                                          CONF.getConfigNodeRatisMaxSleepTimeMs())
                                      .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                                      .build())
                              .setImpl(
                                  RatisConfig.Impl.newBuilder()
                                      .setRaftLogSizeMaxThreshold(CONF.getConfigNodeRatisLogMax())
                                      .setForceSnapshotInterval(
                                          CONF.getConfigNodeRatisPeriodicSnapshotInterval())
                                      .setRetryTimesMax(10)
                                      .setRetryWaitMillis(
                                          COMMON_CONF.getConnectionTimeoutInMS() / 10)
                                      .build())
                              .setRead(
                                  RatisConfig.Read.newBuilder()
                                      // use thrift connection timeout to unify read timeout
                                      .setReadTimeout(
                                          TimeDuration.valueOf(
                                              COMMON_CONF.getConnectionTimeoutInMS(),
                                              TimeUnit.MILLISECONDS))
                                      .build())
                              .build())
                      .setStorageDir(CONF.getConsensusDir())
                      .build(),
                  gid -> stateMachine)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              CONF.getConfigNodeConsensusProtocolClass())));
    }
  }

  /**
   * In version 1.1, we fixed a 1.0 SimpleConsensus bug that incorrectly set the consensus
   * directory. For backward compatibility, we added this function, which we may remove in version
   * 2.x
   */
  private void upgrade() {
    File consensusDir = new File(CONF.getConsensusDir());
    if (consensusDir.exists()) {
      File oldWalDir = new File(consensusDir, "simple");
      if (oldWalDir.exists() && !oldWalDir.renameTo(new File(getConfigRegionDir()))) {
        LOGGER.warn(
            "upgrade ConfigNode consensus wal dir for SimpleConsensus from version/1.0 to version/1.1 failed, "
                + "you maybe need to rename the simple dir to 0_0 manually.");
      }
    }
  }

  /**
   * Create peer in new node to build consensus group.
   *
   * @param configNodeLocations All registered ConfigNodes
   * @throws ConsensusException When addPeer doesn't success
   */
  public void createPeerForConsensusGroup(List<TConfigNodeLocation> configNodeLocations)
      throws ConsensusException {
    LOGGER.info("createPeerForConsensusGroup {}...", configNodeLocations);

    List<Peer> peerList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      peerList.add(
          new Peer(
              DEFAULT_CONSENSUS_GROUP_ID,
              configNodeLocation.getConfigNodeId(),
              configNodeLocation.getConsensusEndPoint()));
    }
    consensusImpl.createLocalPeer(DEFAULT_CONSENSUS_GROUP_ID, peerList);
  }

  /**
   * Add a new ConfigNode Peer into ConfigRegion.
   *
   * @param configNodeLocation The new ConfigNode
   * @throws AddPeerException When addPeer doesn't success
   */
  public void addConfigNodePeer(TConfigNodeLocation configNodeLocation) throws AddPeerException {
    try {
      consensusImpl.addRemotePeer(
          DEFAULT_CONSENSUS_GROUP_ID,
          new Peer(
              DEFAULT_CONSENSUS_GROUP_ID,
              configNodeLocation.getConfigNodeId(),
              configNodeLocation.getConsensusEndPoint()));
    } catch (ConsensusException e) {
      throw new AddPeerException(configNodeLocation);
    }
  }

  /**
   * Remove a ConfigNode Peer out of ConfigRegion.
   *
   * @param configNodeLocation config node location
   * @return True if successfully removePeer. False if another ConfigNode is being removed to the
   *     ConfigRegion
   */
  public boolean removeConfigNodePeer(TConfigNodeLocation configNodeLocation) {
    try {
      consensusImpl.removeRemotePeer(
          DEFAULT_CONSENSUS_GROUP_ID,
          new Peer(
              DEFAULT_CONSENSUS_GROUP_ID,
              configNodeLocation.getConfigNodeId(),
              configNodeLocation.getConsensusEndPoint()));
      return true;
    } catch (ConsensusException e) {
      return false;
    }
  }

  /**
   * Transmit PhysicalPlan to confignode.consensus.statemachine
   *
   * @throws ConsensusException When write doesn't success
   */
  public TSStatus write(final ConfigPhysicalPlan plan) throws ConsensusException {
    return consensusImpl.write(DEFAULT_CONSENSUS_GROUP_ID, plan);
  }

  /**
   * Transmit PhysicalPlan to confignode.consensus.statemachine
   *
   * @throws ConsensusException When read doesn't success
   */
  public DataSet read(final ConfigPhysicalReadPlan plan) throws ConsensusException {
    return consensusImpl.read(DEFAULT_CONSENSUS_GROUP_ID, plan);
  }

  public boolean isLeader() {
    return consensusImpl.isLeader(DEFAULT_CONSENSUS_GROUP_ID);
  }

  public boolean isLeaderReady() {
    return consensusImpl.isLeaderReady(DEFAULT_CONSENSUS_GROUP_ID);
  }

  /**
   * @return ConfigNode-leader peer if leader exists, null otherwise.
   */
  private Peer getLeaderPeer() {
    for (int retry = 0; retry < 50; retry++) {
      Peer leaderPeer = consensusImpl.getLeader(DEFAULT_CONSENSUS_GROUP_ID);
      if (leaderPeer != null) {
        return leaderPeer;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(RETRY_WAIT_TIME_MS);
      } catch (InterruptedException e) {
        LOGGER.warn("ConsensusManager getLeaderPeer been interrupted, ", e);
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  /**
   * @return ConfigNode-leader's location if leader exists, null otherwise.
   */
  public TConfigNodeLocation getLeaderLocation() {
    Peer leaderPeer = getLeaderPeer();
    if (leaderPeer != null) {
      return getNodeManager().getRegisteredConfigNodes().stream()
          .filter(leader -> leader.getConfigNodeId() == leaderPeer.getNodeId())
          .findFirst()
          .orElse(null);
    }
    return null;
  }

  /**
   * @return true if ConfigNode-leader is elected, false otherwise.
   */
  public boolean isLeaderExist() {
    return getLeaderPeer() != null;
  }

  /**
   * Confirm the current ConfigNode's leadership.
   *
   * @return SUCCESS_STATUS if the current ConfigNode is leader and has been ready yet,
   *     NEED_REDIRECTION otherwise
   */
  public TSStatus confirmLeader() {
    TSStatus result = new TSStatus();
    if (isLeaderReady()) {
      result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      result.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
      if (isLeader()) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < MAX_WAIT_READY_TIME_MS) {
          if (isLeaderReady()) {
            result.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            return result;
          }
          try {
            Thread.sleep(RETRY_WAIT_TIME_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Unexpected interruption during waiting for configNode leader ready.");
            break;
          }
        }
        result.setMessage(
            "The current ConfigNode is leader but not ready yet, please try again later.");
      } else {
        result.setMessage(
            "The current ConfigNode is not leader, please redirect to a new ConfigNode.");
      }
      TConfigNodeLocation leaderLocation = getLeaderLocation();
      if (leaderLocation != null) {
        result.setRedirectNode(leaderLocation.getInternalEndPoint());
      }
    }
    return result;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return DEFAULT_CONSENSUS_GROUP_ID;
  }

  public static String getConfigRegionDir() {
    return CONF.getConsensusDir()
        + File.separator
        + ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID.getType().getValue()
        + "_"
        + ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID.getId();
  }

  public IConsensus getConsensusImpl() {
    return consensusImpl;
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  public void manuallyTakeSnapshot() throws ConsensusException {
    consensusImpl.triggerSnapshot(ConfigNodeInfo.CONFIG_REGION_ID, true);
  }

  public boolean isInitialized() {
    return isInitialized;
  }
}
