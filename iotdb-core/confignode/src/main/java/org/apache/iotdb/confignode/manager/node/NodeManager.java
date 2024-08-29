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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.ainode.GetAINodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.UpdateAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.ainode.AINodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.ainode.AINodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.ConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.TTLManager;
import org.apache.iotdb.confignode.manager.TriggerManager;
import org.apache.iotdb.confignode.manager.UDFManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** {@link NodeManager} manages cluster node addition and removal requests. */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  public static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();

  private final IManager configManager;
  protected final NodeInfo nodeInfo;

  private final ReentrantLock removeConfigNodeLock;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public NodeManager(IManager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.removeConfigNodeLock = new ReentrantLock();
  }

  /**
   * Get system configurations.
   *
   * @return ConfigurationResp. The TSStatus will be set to SUCCESS_STATUS.
   */
  public DataSet getSystemConfiguration() {
    ConfigurationResp dataSet = new ConfigurationResp();
    dataSet.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    setGlobalConfig(dataSet);
    setRatisConfig(dataSet);
    setCQConfig(dataSet);
    return dataSet;
  }

  private void setGlobalConfig(ConfigurationResp dataSet) {
    // Set TGlobalConfig
    final ConfigNodeConfig configNodeConfig = ConfigNodeDescriptor.getInstance().getConf();
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    TGlobalConfig globalConfig = new TGlobalConfig();
    globalConfig.setDataRegionConsensusProtocolClass(
        configNodeConfig.getDataRegionConsensusProtocolClass());
    globalConfig.setSchemaRegionConsensusProtocolClass(
        configNodeConfig.getSchemaRegionConsensusProtocolClass());
    globalConfig.setSeriesPartitionSlotNum(configNodeConfig.getSeriesSlotNum());
    globalConfig.setSeriesPartitionExecutorClass(
        configNodeConfig.getSeriesPartitionExecutorClass());
    globalConfig.setTimePartitionInterval(commonConfig.getTimePartitionInterval());
    globalConfig.setTimePartitionOrigin(commonConfig.getTimePartitionOrigin());
    globalConfig.setReadConsistencyLevel(configNodeConfig.getReadConsistencyLevel());
    globalConfig.setDiskSpaceWarningThreshold(commonConfig.getDiskSpaceWarningThreshold());
    globalConfig.setTimestampPrecision(commonConfig.getTimestampPrecision());
    globalConfig.setSchemaEngineMode(commonConfig.getSchemaEngineMode());
    globalConfig.setTagAttributeTotalSize(commonConfig.getTagAttributeTotalSize());
    dataSet.setGlobalConfig(globalConfig);
  }

  private void setRatisConfig(ConfigurationResp dataSet) {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TRatisConfig ratisConfig = new TRatisConfig();

    ratisConfig.setDataAppenderBufferSize(conf.getDataRegionRatisConsensusLogAppenderBufferSize());
    ratisConfig.setSchemaAppenderBufferSize(
        conf.getSchemaRegionRatisConsensusLogAppenderBufferSize());

    ratisConfig.setDataSnapshotTriggerThreshold(conf.getDataRegionRatisSnapshotTriggerThreshold());
    ratisConfig.setSchemaSnapshotTriggerThreshold(
        conf.getSchemaRegionRatisSnapshotTriggerThreshold());

    ratisConfig.setDataLogUnsafeFlushEnable(conf.isDataRegionRatisLogUnsafeFlushEnable());
    ratisConfig.setSchemaLogUnsafeFlushEnable(conf.isSchemaRegionRatisLogUnsafeFlushEnable());

    ratisConfig.setDataLogSegmentSizeMax(conf.getDataRegionRatisLogSegmentSizeMax());
    ratisConfig.setSchemaLogSegmentSizeMax(conf.getSchemaRegionRatisLogSegmentSizeMax());

    ratisConfig.setDataGrpcFlowControlWindow(conf.getDataRegionRatisGrpcFlowControlWindow());
    ratisConfig.setSchemaGrpcFlowControlWindow(conf.getSchemaRegionRatisGrpcFlowControlWindow());
    ratisConfig.setDataRegionGrpcLeaderOutstandingAppendsMax(
        conf.getDataRegionRatisGrpcLeaderOutstandingAppendsMax());
    ratisConfig.setSchemaRegionGrpcLeaderOutstandingAppendsMax(
        conf.getSchemaRegionRatisGrpcLeaderOutstandingAppendsMax());

    ratisConfig.setDataRegionLogForceSyncNum(conf.getDataRegionRatisLogForceSyncNum());
    ratisConfig.setSchemaRegionLogForceSyncNum(conf.getSchemaRegionRatisLogForceSyncNum());

    ratisConfig.setDataLeaderElectionTimeoutMin(
        conf.getDataRegionRatisRpcLeaderElectionTimeoutMinMs());
    ratisConfig.setSchemaLeaderElectionTimeoutMin(
        conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs());

    ratisConfig.setDataLeaderElectionTimeoutMax(
        conf.getDataRegionRatisRpcLeaderElectionTimeoutMaxMs());
    ratisConfig.setSchemaLeaderElectionTimeoutMax(
        conf.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs());

    ratisConfig.setDataRequestTimeout(conf.getDataRegionRatisRequestTimeoutMs());
    ratisConfig.setSchemaRequestTimeout(conf.getSchemaRegionRatisRequestTimeoutMs());

    ratisConfig.setDataMaxRetryAttempts(conf.getDataRegionRatisMaxRetryAttempts());
    ratisConfig.setDataInitialSleepTime(conf.getDataRegionRatisInitialSleepTimeMs());
    ratisConfig.setDataMaxSleepTime(conf.getDataRegionRatisMaxSleepTimeMs());
    ratisConfig.setSchemaMaxRetryAttempts(conf.getSchemaRegionRatisMaxRetryAttempts());
    ratisConfig.setSchemaInitialSleepTime(conf.getSchemaRegionRatisInitialSleepTimeMs());
    ratisConfig.setSchemaMaxSleepTime(conf.getSchemaRegionRatisMaxSleepTimeMs());

    ratisConfig.setSchemaPreserveWhenPurge(conf.getSchemaRegionRatisPreserveLogsWhenPurge());
    ratisConfig.setDataPreserveWhenPurge(conf.getDataRegionRatisPreserveLogsWhenPurge());

    ratisConfig.setFirstElectionTimeoutMin(conf.getRatisFirstElectionTimeoutMinMs());
    ratisConfig.setFirstElectionTimeoutMax(conf.getRatisFirstElectionTimeoutMaxMs());

    ratisConfig.setSchemaRegionRatisLogMax(conf.getSchemaRegionRatisLogMax());
    ratisConfig.setDataRegionRatisLogMax(conf.getDataRegionRatisLogMax());

    ratisConfig.setSchemaRegionPeriodicSnapshotInterval(
        conf.getSchemaRegionRatisPeriodicSnapshotInterval());
    ratisConfig.setDataRegionPeriodicSnapshotInterval(
        conf.getDataRegionRatisPeriodicSnapshotInterval());

    dataSet.setRatisConfig(ratisConfig);
  }

  private void setCQConfig(ConfigurationResp dataSet) {
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TCQConfig cqConfig = new TCQConfig();
    cqConfig.setCqMinEveryIntervalInMs(conf.getCqMinEveryIntervalInMs());

    dataSet.setCqConfig(cqConfig);
  }

  private TRuntimeConfiguration getRuntimeConfiguration() {
    getPipeManager().getPipePluginCoordinator().lock();
    getTriggerManager().getTriggerInfo().acquireTriggerTableLock();
    getUDFManager().getUdfInfo().acquireUDFTableLock();
    try {
      final TRuntimeConfiguration runtimeConfiguration = new TRuntimeConfiguration();
      runtimeConfiguration.setTemplateInfo(getClusterSchemaManager().getAllTemplateSetInfo());
      runtimeConfiguration.setAllTriggerInformation(
          getTriggerManager().getTriggerTable(false).getAllTriggerInformation());
      runtimeConfiguration.setAllUDFInformation(
          getUDFManager().getUDFTable().getAllUDFInformation());
      runtimeConfiguration.setAllPipeInformation(
          getPipeManager().getPipePluginCoordinator().getPipePluginTable().getAllPipePluginMeta());
      runtimeConfiguration.setAllTTLInformation(
          DataNodeRegisterResp.convertAllTTLInformation(getTTLManager().getAllTTL()));
      runtimeConfiguration.setTableInfo(
          getClusterSchemaManager().getAllTableInfoForDataNodeActivation());
      runtimeConfiguration.setClusterId(getClusterManager().getClusterId());
      return runtimeConfiguration;
    } finally {
      getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
      getUDFManager().getUdfInfo().releaseUDFTableLock();
      getPipeManager().getPipePluginCoordinator().unlock();
    }
  }

  /**
   * Register DataNode.
   *
   * @param req TDataNodeRegisterReq
   * @return DataNodeConfigurationDataSet. The {@link TSStatus} will be set to {@link
   *     TSStatusCode#SUCCESS_STATUS} when register success.
   */
  public DataSet registerDataNode(TDataNodeRegisterReq req) {
    DataNodeRegisterResp resp = new DataNodeRegisterResp();
    resp.setConfigNodeList(getRegisteredConfigNodes());

    // Create a new DataNodeHeartbeatCache and force update NodeStatus
    int dataNodeId = nodeInfo.generateNextNodeId();
    getLoadManager().getLoadCache().createNodeHeartbeatCache(NodeType.DataNode, dataNodeId);
    // TODO: invoke a force heartbeat to update new DataNode's status immediately

    RegisterDataNodePlan registerDataNodePlan =
        new RegisterDataNodePlan(req.getDataNodeConfiguration());
    // Register new DataNode
    registerDataNodePlan.getDataNodeConfiguration().getLocation().setDataNodeId(dataNodeId);
    try {
      getConsensusManager().write(registerDataNodePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }

    // update datanode's versionInfo
    UpdateVersionInfoPlan updateVersionInfoPlan =
        new UpdateVersionInfoPlan(req.getVersionInfo(), dataNodeId);
    try {
      getConsensusManager().write(updateVersionInfoPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }

    // Bind DataNode metrics
    PartitionMetrics.bindDataNodePartitionMetricsWhenUpdate(
        MetricService.getInstance(), configManager, dataNodeId);

    // Adjust the maximum RegionGroup number of each Database
    getClusterSchemaManager().adjustMaxRegionGroupNum();

    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_REGISTRATION);
    resp.setDataNodeId(
        registerDataNodePlan.getDataNodeConfiguration().getLocation().getDataNodeId());
    resp.setRuntimeConfiguration(getRuntimeConfiguration());
    return resp;
  }

  public TDataNodeRestartResp updateDataNodeIfNecessary(TDataNodeRestartReq req) {
    final String clusterId =
        configManager
            .getClusterManager()
            .getClusterIdWithRetry(
                CommonDescriptor.getInstance().getConfig().getConnectionTimeoutInMS() / 2);
    TDataNodeRestartResp resp = new TDataNodeRestartResp();
    resp.setConfigNodeList(getRegisteredConfigNodes());
    if (clusterId == null) {
      resp.setStatus(
          new TSStatus(TSStatusCode.GET_CLUSTER_ID_ERROR.getStatusCode())
              .setMessage("clusterId has not generated"));
      return resp;
    }

    int nodeId = req.getDataNodeConfiguration().getLocation().getDataNodeId();
    TDataNodeConfiguration dataNodeConfiguration = getRegisteredDataNode(nodeId);
    if (!req.getDataNodeConfiguration().equals(dataNodeConfiguration)) {
      // Update DataNodeConfiguration when modified during restart
      UpdateDataNodePlan updateDataNodePlan =
          new UpdateDataNodePlan(req.getDataNodeConfiguration());
      try {
        getConsensusManager().write(updateDataNodePlan);
      } catch (ConsensusException e) {
        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      }
    }
    TNodeVersionInfo versionInfo = nodeInfo.getVersionInfo(nodeId);
    if (!req.getVersionInfo().equals(versionInfo)) {
      // Update versionInfo when modified during restart
      UpdateVersionInfoPlan updateVersionInfoPlan =
          new UpdateVersionInfoPlan(req.getVersionInfo(), nodeId);
      try {
        getConsensusManager().write(updateVersionInfoPlan);
      } catch (ConsensusException e) {
        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      }
    }

    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_RESTART);
    resp.setRuntimeConfiguration(getRuntimeConfiguration());
    List<TConsensusGroupId> consensusGroupIds =
        getPartitionManager().getAllReplicaSets(nodeId).stream()
            .map(TRegionReplicaSet::getRegionId)
            .collect(Collectors.toList());
    resp.setConsensusGroupIds(consensusGroupIds);
    return resp;
  }

  /**
   * Remove DataNodes.
   *
   * @param removeDataNodePlan removeDataNodePlan
   * @return DataNodeToStatusResp, The TSStatus will be SUCCEED_STATUS if the request is accepted,
   *     DATANODE_NOT_EXIST when some datanode does not exist.
   */
  public DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    LOGGER.info("NodeManager start to remove DataNode {}", removeDataNodePlan);

    RegionMaintainHandler handler = new RegionMaintainHandler((ConfigManager) configManager);
    DataNodeToStatusResp preCheckStatus = handler.checkRemoveDataNodeRequest(removeDataNodePlan);
    if (preCheckStatus.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(
          "The remove DataNode request check failed. req: {}, check result: {}",
          removeDataNodePlan,
          preCheckStatus.getStatus());
      return preCheckStatus;
    }

    // Do transfer of the DataNodes before remove
    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
    if (configManager.transfer(removeDataNodePlan.getDataNodeLocations()).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(
          new TSStatus(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode())
              .setMessage("Fail to do transfer of the DataNodes"));
      return dataSet;
    }

    // Add request to queue, then return to client
    boolean removeSucceed = configManager.getProcedureManager().removeDataNode(removeDataNodePlan);
    TSStatus status;
    if (removeSucceed) {
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("Server accepted the request");
    } else {
      status = new TSStatus(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode());
      status.setMessage("Server rejected the request, maybe requests are too many");
    }
    dataSet.setStatus(status);

    LOGGER.info(
        "NodeManager submit RemoveDataNodePlan finished, removeDataNodePlan: {}",
        removeDataNodePlan);
    return dataSet;
  }

  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    int nodeId = nodeInfo.generateNextNodeId();
    req.getConfigNodeLocation().setConfigNodeId(nodeId);
    configManager.getProcedureManager().addConfigNode(req);
    return new TConfigNodeRegisterResp()
        .setStatus(ClusterNodeStartUtils.ACCEPT_NODE_REGISTRATION)
        .setConfigNodeId(nodeId);
  }

  public TSStatus updateConfigNodeIfNecessary(int configNodeId, TNodeVersionInfo versionInfo) {
    TNodeVersionInfo recordVersionInfo = nodeInfo.getVersionInfo(configNodeId);
    if (!recordVersionInfo.equals(versionInfo)) {
      // Update versionInfo when modified during restart
      UpdateVersionInfoPlan updateConfigNodePlan =
          new UpdateVersionInfoPlan(versionInfo, configNodeId);
      try {
        return getConsensusManager().write(updateConfigNodePlan);
      } catch (ConsensusException e) {
        return new TSStatus(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode());
      }
    }
    return ClusterNodeStartUtils.ACCEPT_NODE_RESTART;
  }

  public List<TAINodeInfo> getRegisteredAINodeInfoList() {
    List<TAINodeInfo> aiNodeInfoList = new ArrayList<>();
    for (TAINodeConfiguration aiNodeConfiguration : getRegisteredAINodes()) {
      TAINodeInfo aiNodeInfo = new TAINodeInfo();
      aiNodeInfo.setAiNodeId(aiNodeConfiguration.getLocation().getAiNodeId());
      aiNodeInfo.setStatus(getLoadManager().getNodeStatusWithReason(aiNodeInfo.getAiNodeId()));
      aiNodeInfo.setInternalAddress(aiNodeConfiguration.getLocation().getInternalEndPoint().ip);
      aiNodeInfo.setInternalPort(aiNodeConfiguration.getLocation().getInternalEndPoint().port);
      aiNodeInfoList.add(aiNodeInfo);
    }
    return aiNodeInfoList;
  }

  /**
   * @return All registered AINodes
   */
  public List<TAINodeConfiguration> getRegisteredAINodes() {
    return nodeInfo.getRegisteredAINodes();
  }

  public TAINodeConfiguration getRegisteredAINode(int aiNodeId) {
    return nodeInfo.getRegisteredAINode(aiNodeId);
  }

  /**
   * Register AINode. Use synchronized to make sure
   *
   * @param req TAINodeRegisterReq
   * @return AINodeConfigurationDataSet. The {@link TSStatus} will be set to {@link
   *     TSStatusCode#SUCCESS_STATUS} when register success.
   */
  public synchronized DataSet registerAINode(TAINodeRegisterReq req) {

    if (!nodeInfo.getRegisteredAINodes().isEmpty()) {
      AINodeRegisterResp dataSet = new AINodeRegisterResp();
      dataSet.setConfigNodeList(Collections.emptyList());
      dataSet.setStatus(
          new TSStatus(TSStatusCode.REGISTER_AI_NODE_ERROR.getStatusCode())
              .setMessage("There is already one AINode in the cluster."));
      return dataSet;
    }

    int aiNodeId = nodeInfo.generateNextNodeId();
    getLoadManager().getLoadCache().createNodeHeartbeatCache(NodeType.AINode, aiNodeId);
    RegisterAINodePlan registerAINodePlan = new RegisterAINodePlan(req.getAiNodeConfiguration());
    // Register new DataNode
    registerAINodePlan.getAINodeConfiguration().getLocation().setAiNodeId(aiNodeId);
    try {
      getConsensusManager().write(registerAINodePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }

    // update datanode's versionInfo
    UpdateVersionInfoPlan updateVersionInfoPlan =
        new UpdateVersionInfoPlan(req.getVersionInfo(), aiNodeId);
    try {
      getConsensusManager().write(updateVersionInfoPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }

    AINodeRegisterResp resp = new AINodeRegisterResp();
    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_REGISTRATION);
    resp.setConfigNodeList(getRegisteredConfigNodes());
    resp.setAINodeId(registerAINodePlan.getAINodeConfiguration().getLocation().getAiNodeId());
    return resp;
  }

  /**
   * Remove AINodes.
   *
   * @param removeAINodePlan removeDataNodePlan
   */
  public TSStatus removeAINode(RemoveAINodePlan removeAINodePlan) {
    LOGGER.info("NodeManager start to remove AINode {}", removeAINodePlan);

    // check if the node exists
    if (!nodeInfo.containsAINode(removeAINodePlan.getAINodeLocation().getAiNodeId())) {
      return new TSStatus(TSStatusCode.REMOVE_AI_NODE_ERROR.getStatusCode())
          .setMessage("AINode doesn't exist.");
    }

    // Add request to queue, then return to client
    boolean removeSucceed = configManager.getProcedureManager().removeAINode(removeAINodePlan);
    TSStatus status;
    if (removeSucceed) {
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("Server accepted the request");
    } else {
      status = new TSStatus(TSStatusCode.REMOVE_AI_NODE_ERROR.getStatusCode());
      status.setMessage("Server rejected the request, maybe requests are too many");
    }

    LOGGER.info(
        "NodeManager submit RemoveAINodePlan finished, removeAINodePlan: {}", removeAINodePlan);
    return status;
  }

  public TAINodeRestartResp updateAINodeIfNecessary(TAINodeRestartReq req) {
    int nodeId = req.getAiNodeConfiguration().getLocation().getAiNodeId();
    TAINodeConfiguration aiNodeConfiguration = getRegisteredAINode(nodeId);
    if (!req.getAiNodeConfiguration().equals(aiNodeConfiguration)) {
      // Update AINodeConfiguration when modified during restart
      UpdateAINodePlan updateAINodePlan = new UpdateAINodePlan(req.getAiNodeConfiguration());
      try {
        getConsensusManager().write(updateAINodePlan);
      } catch (ConsensusException e) {
        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      }
    }
    TNodeVersionInfo versionInfo = nodeInfo.getVersionInfo(nodeId);
    if (!req.getVersionInfo().equals(versionInfo)) {
      // Update versionInfo when modified during restart
      UpdateVersionInfoPlan updateVersionInfoPlan =
          new UpdateVersionInfoPlan(req.getVersionInfo(), nodeId);
      try {
        getConsensusManager().write(updateVersionInfoPlan);
      } catch (ConsensusException e) {
        LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      }
    }

    TAINodeRestartResp resp = new TAINodeRestartResp();
    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_RESTART);
    resp.setConfigNodeList(getRegisteredConfigNodes());
    return resp;
  }

  public AINodeConfigurationResp getAINodeConfiguration(GetAINodeConfigurationPlan req) {
    try {
      return (AINodeConfigurationResp) getConsensusManager().read(req);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      AINodeConfigurationResp response = new AINodeConfigurationResp();
      response.setStatus(res);
      return response;
    }
  }

  /**
   * Get TDataNodeConfiguration.
   *
   * @param req GetDataNodeConfigurationPlan
   * @return The specific DataNode's configuration or all DataNodes' configuration if dataNodeId in
   *     GetDataNodeConfigurationPlan is -1
   */
  public DataNodeConfigurationResp getDataNodeConfiguration(GetDataNodeConfigurationPlan req) {
    try {
      return (DataNodeConfigurationResp) getConsensusManager().read(req);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      DataNodeConfigurationResp response = new DataNodeConfigurationResp();
      response.setStatus(res);
      return response;
    }
  }

  /**
   * Only leader use this interface.
   *
   * @return The number of registered Nodes
   */
  public int getRegisteredNodeCount() {
    return nodeInfo.getRegisteredNodeCount();
  }

  /**
   * Only leader use this interface.
   *
   * @return The number of registered DataNodes
   */
  public int getRegisteredDataNodeCount() {
    return nodeInfo.getRegisteredDataNodeCount();
  }

  /**
   * Only leader use this interface.
   *
   * @return All registered DataNodes
   */
  public List<TDataNodeConfiguration> getRegisteredDataNodes() {
    return nodeInfo.getRegisteredDataNodes();
  }

  /**
   * Only leader use this interface.
   *
   * <p>Notice: The result will be an empty TDataNodeConfiguration if the specified DataNode doesn't
   * register
   *
   * @param dataNodeId The specified DataNode's index
   * @return The specified registered DataNode
   */
  public TDataNodeConfiguration getRegisteredDataNode(int dataNodeId) {
    return nodeInfo.getRegisteredDataNode(dataNodeId);
  }

  public Map<Integer, TDataNodeLocation> getRegisteredDataNodeLocations() {
    Map<Integer, TDataNodeLocation> dataNodeLocations = new ConcurrentHashMap<>();
    nodeInfo
        .getRegisteredDataNodes()
        .forEach(
            dataNodeConfiguration ->
                dataNodeLocations.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    return dataNodeLocations;
  }

  public Map<Integer, TConfigNodeLocation> getRegisteredConfigNodeLocations() {
    return nodeInfo.getRegisteredConfigNodes().stream()
        .collect(Collectors.toMap(TConfigNodeLocation::getConfigNodeId, location -> location));
  }

  public List<TDataNodeInfo> getRegisteredDataNodeInfoList() {
    List<TDataNodeInfo> dataNodeInfoList = new ArrayList<>();
    List<TDataNodeConfiguration> registeredDataNodes = this.getRegisteredDataNodes();
    if (registeredDataNodes != null) {
      registeredDataNodes.forEach(
          registeredDataNode -> {
            TDataNodeInfo dataNodeInfo = new TDataNodeInfo();
            int dataNodeId = registeredDataNode.getLocation().getDataNodeId();
            dataNodeInfo.setDataNodeId(dataNodeId);
            dataNodeInfo.setStatus(getLoadManager().getNodeStatusWithReason(dataNodeId));
            dataNodeInfo.setRpcAddresss(
                registeredDataNode.getLocation().getClientRpcEndPoint().getIp());
            dataNodeInfo.setRpcPort(
                registeredDataNode.getLocation().getClientRpcEndPoint().getPort());
            dataNodeInfo.setDataRegionNum(0);
            dataNodeInfo.setSchemaRegionNum(0);
            dataNodeInfo.setCpuCoreNum(registeredDataNode.getResource().getCpuCoreNum());
            dataNodeInfoList.add(dataNodeInfo);
          });
    }

    // Map<DataNodeId, DataRegionNum>
    Map<Integer, AtomicInteger> dataRegionNumMap = new HashMap<>();
    // Map<DataNodeId, SchemaRegionNum>
    Map<Integer, AtomicInteger> schemaRegionNumMap = new HashMap<>();
    List<TRegionReplicaSet> regionReplicaSets = getPartitionManager().getAllReplicaSets();
    regionReplicaSets.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation -> {
                      switch (regionReplicaSet.getRegionId().getType()) {
                        case SchemaRegion:
                          schemaRegionNumMap
                              .computeIfAbsent(
                                  dataNodeLocation.getDataNodeId(), key -> new AtomicInteger())
                              .getAndIncrement();
                          break;
                        case DataRegion:
                        default:
                          dataRegionNumMap
                              .computeIfAbsent(
                                  dataNodeLocation.getDataNodeId(), key -> new AtomicInteger())
                              .getAndIncrement();
                      }
                    }));
    AtomicInteger zero = new AtomicInteger(0);
    dataNodeInfoList.forEach(
        (dataNodesInfo -> {
          dataNodesInfo.setSchemaRegionNum(
              schemaRegionNumMap.getOrDefault(dataNodesInfo.getDataNodeId(), zero).get());
          dataNodesInfo.setDataRegionNum(
              dataRegionNumMap.getOrDefault(dataNodesInfo.getDataNodeId(), zero).get());
        }));

    dataNodeInfoList.sort(Comparator.comparingInt(TDataNodeInfo::getDataNodeId));
    return dataNodeInfoList;
  }

  public int getDataNodeCpuCoreCount() {
    return nodeInfo.getDataNodeTotalCpuCoreCount();
  }

  public List<TConfigNodeLocation> getRegisteredConfigNodes() {
    return nodeInfo.getRegisteredConfigNodes();
  }

  public Map<Integer, TNodeVersionInfo> getNodeVersionInfo() {
    return nodeInfo.getNodeVersionInfo();
  }

  public List<TConfigNodeInfo> getRegisteredConfigNodeInfoList() {
    List<TConfigNodeInfo> configNodeInfoList = new ArrayList<>();
    List<TConfigNodeLocation> registeredConfigNodes = this.getRegisteredConfigNodes();
    if (registeredConfigNodes != null) {
      registeredConfigNodes.forEach(
          configNodeLocation -> {
            TConfigNodeInfo info = new TConfigNodeInfo();
            int configNodeId = configNodeLocation.getConfigNodeId();
            info.setConfigNodeId(configNodeId);
            info.setStatus(getLoadManager().getNodeStatusWithReason(configNodeId));
            info.setInternalAddress(configNodeLocation.getInternalEndPoint().getIp());
            info.setInternalPort(configNodeLocation.getInternalEndPoint().getPort());
            info.setRoleType(
                configNodeLocation.getConfigNodeId() == ConfigNodeHeartbeatCache.CURRENT_NODE_ID
                    ? RegionRoleType.Leader.name()
                    : RegionRoleType.Follower.name());
            configNodeInfoList.add(info);
          });
    }
    configNodeInfoList.sort(Comparator.comparingInt(TConfigNodeInfo::getConfigNodeId));
    return configNodeInfoList;
  }

  /**
   * Only leader use this interface, record the new ConfigNode's information.
   *
   * @param configNodeLocation The new ConfigNode.
   * @param versionInfo The new ConfigNode's versionInfo.
   */
  public void applyConfigNode(
      TConfigNodeLocation configNodeLocation, TNodeVersionInfo versionInfo) {
    ApplyConfigNodePlan applyConfigNodePlan = new ApplyConfigNodePlan(configNodeLocation);
    try {
      getConsensusManager().write(applyConfigNodePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
    UpdateVersionInfoPlan updateVersionInfoPlan =
        new UpdateVersionInfoPlan(versionInfo, configNodeLocation.getConfigNodeId());
    try {
      getConsensusManager().write(updateVersionInfoPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
  }

  /**
   * Only leader use this interface, check the ConfigNode before remove it.
   *
   * @param removeConfigNodePlan RemoveConfigNodePlan
   */
  public TSStatus checkConfigNodeBeforeRemove(RemoveConfigNodePlan removeConfigNodePlan) {
    removeConfigNodeLock.lock();
    try {
      // Check OnlineConfigNodes number
      if (filterConfigNodeThroughStatus(NodeStatus.Running).size() <= 1) {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
            .setMessage(
                "Remove ConfigNode failed because there is only one ConfigNode in current Cluster.");
      }

      // Check whether the registeredConfigNodes contain the ConfigNode to be removed.
      if (!getRegisteredConfigNodes().contains(removeConfigNodePlan.getConfigNodeLocation())) {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
            .setMessage("Remove ConfigNode failed because the ConfigNode not in current Cluster.");
      }

      // Check whether the remove ConfigNode is leader
      TConfigNodeLocation leader = getConsensusManager().getLeaderLocation();
      if (leader == null) {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
            .setMessage(
                "Remove ConfigNode failed because the ConfigNodeGroup is on leader election, please retry.");
      }

      if (leader
          .getInternalEndPoint()
          .equals(removeConfigNodePlan.getConfigNodeLocation().getInternalEndPoint())) {
        // transfer leader
        return transferLeader(removeConfigNodePlan, getConsensusManager().getConsensusGroupId());
      }

    } finally {
      removeConfigNodeLock.unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage("Successfully remove confignode.");
  }

  private TSStatus transferLeader(
      RemoveConfigNodePlan removeConfigNodePlan, ConsensusGroupId groupId) {
    Optional<TConfigNodeLocation> optional =
        filterConfigNodeThroughStatus(NodeStatus.Running).stream()
            .filter(e -> !e.equals(removeConfigNodePlan.getConfigNodeLocation()))
            .findAny();
    TConfigNodeLocation newLeader = null;
    if (optional.isPresent()) {
      newLeader = optional.get();
    } else {
      return new TSStatus(TSStatusCode.TRANSFER_LEADER_ERROR.getStatusCode())
          .setMessage(
              "Transfer ConfigNode leader failed because can not find any running ConfigNode.");
    }
    try {
      getConsensusManager()
          .getConsensusImpl()
          .transferLeader(
              groupId,
              new Peer(groupId, newLeader.getConfigNodeId(), newLeader.getConsensusEndPoint()));
    } catch (ConsensusException e) {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
          .setMessage("Remove ConfigNode failed because transfer ConfigNode leader failed.");
    }
    return new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
        .setRedirectNode(newLeader.getInternalEndPoint())
        .setMessage(
            "The ConfigNode to be removed is leader, already transfer Leader to "
                + newLeader
                + ".");
  }

  public List<TSStatus> merge() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnRequestType.MERGE, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> flush(TFlushReq req) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TFlushReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnRequestType.FLUSH, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> clearCache() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnRequestType.CLEAR_CACHE, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> setConfiguration(TSetConfigurationReq req) {
    List<TSStatus> responseList = new ArrayList<>();

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    Map<Integer, TDataNodeLocation> targetDataNodes = new HashMap<>();
    int nodeId = req.getNodeId();
    // send to datanode
    if (dataNodeLocationMap.containsKey(nodeId)) {
      targetDataNodes.put(nodeId, dataNodeLocationMap.get(nodeId));
    } else if (nodeId < 0) {
      targetDataNodes.putAll(dataNodeLocationMap);
    }
    if (!targetDataNodes.isEmpty()) {
      DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
          new DataNodeAsyncRequestContext<>(
              CnToDnRequestType.SET_CONFIGURATION, req, dataNodeLocationMap);
      CnToDnInternalServiceAsyncRequestManager.getInstance()
          .sendAsyncRequestWithRetry(clientHandler);
      responseList.addAll(clientHandler.getResponseList());
    }

    // send to config node
    List<TConfigNodeLocation> configNodes = getRegisteredConfigNodes();
    for (TConfigNodeLocation configNode : configNodes) {
      if (configNode.getConfigNodeId() == CONF.getConfigNodeId()) {
        continue;
      }
      if (nodeId >= 0 && nodeId != configNode.getConfigNodeId()) {
        continue;
      }
      TSStatus status = null;
      try {
        status =
            (TSStatus)
                SyncConfigNodeClientPool.getInstance()
                    .sendSyncRequestToConfigNodeWithRetry(
                        configNode.getInternalEndPoint(),
                        new TSetConfigurationReq(req.getConfigs(), configNode.getConfigNodeId()),
                        CnToCnNodeRequestType.SET_CONFIGURATION);
      } catch (Exception e) {
        status =
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), e.getMessage());
      }
      responseList.add(status);
    }
    return responseList;
  }

  public List<TSStatus> startRpairData() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnRequestType.START_REPAIR_DATA, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> stopRepairData() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnRequestType.STOP_REPAIR_DATA, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> submitLoadConfigurationTask() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> dataNodeRequestContext =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.LOAD_CONFIGURATION, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(dataNodeRequestContext);
    return dataNodeRequestContext.getResponseList();
  }

  public TShowConfigurationResp showConfiguration(int nodeId) {
    TShowConfigurationResp resp = new TShowConfigurationResp();

    // data node
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    if (dataNodeLocationMap.containsKey(nodeId)) {
      TDataNodeLocation dataNodeLocation = dataNodeLocationMap.get(nodeId);
      return (TShowConfigurationResp)
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeLocation.getInternalEndPoint(),
                  null,
                  CnToDnRequestType.SHOW_CONFIGURATION);
    }

    // other config node
    for (TConfigNodeLocation registeredConfigNode : getRegisteredConfigNodes()) {
      if (registeredConfigNode.getConfigNodeId() != nodeId) {
        continue;
      }
      resp =
          (TShowConfigurationResp)
              SyncConfigNodeClientPool.getInstance()
                  .sendSyncRequestToConfigNodeWithRetry(
                      registeredConfigNode.getInternalEndPoint(),
                      nodeId,
                      CnToCnNodeRequestType.SHOW_CONFIGURATION);
      return resp;
    }
    return resp;
  }

  public List<TSStatus> setSystemStatus(String status) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<String, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.SET_SYSTEM_STATUS, status, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq setDataNodeStatusReq) {
    return (TSStatus)
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                setDataNodeStatusReq.getTargetDataNode().getInternalEndPoint(),
                setDataNodeStatusReq.getStatus(),
                CnToDnRequestType.SET_SYSTEM_STATUS);
  }

  /**
   * Kill read on DataNode.
   *
   * @param queryId the id of specific read need to be killed, it will be NULL if kill all queries
   * @param dataNodeId the DataNode obtains target read, -1 means we will kill all queries on all
   *     DataNodes
   */
  public TSStatus killQuery(String queryId, int dataNodeId) {
    if (dataNodeId < 0) {
      return killAllQueries();
    } else {
      return killSpecificQuery(queryId, getRegisteredDataNodeLocations().get(dataNodeId));
    }
  }

  private TSStatus killAllQueries() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<String, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.KILL_QUERY_INSTANCE, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
  }

  private TSStatus killSpecificQuery(String queryId, TDataNodeLocation dataNodeLocation) {
    if (dataNodeLocation == null) {
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "The target DataNode is not existed, please ensure your input <queryId> is correct");
    } else {
      return (TSStatus)
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeLocation.getInternalEndPoint(),
                  queryId,
                  CnToDnRequestType.KILL_QUERY_INSTANCE);
    }
  }

  /**
   * Filter ConfigNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered ConfigNodes with the specified NodeStatus
   */
  public List<TConfigNodeLocation> filterConfigNodeThroughStatus(NodeStatus... status) {
    return nodeInfo.getRegisteredConfigNodes(
        getLoadManager().filterConfigNodeThroughStatus(status));
  }

  /**
   * Filter DataNodes through the specified NodeStatus.
   *
   * @param status The specified NodeStatus
   * @return Filtered DataNodes with the specified NodeStatus
   */
  public List<TDataNodeConfiguration> filterDataNodeThroughStatus(NodeStatus... status) {
    return nodeInfo.getRegisteredDataNodes(getLoadManager().filterDataNodeThroughStatus(status));
  }

  /**
   * Get the DataNodeLocation of the DataNode which has the lowest loadScore.
   *
   * @return TDataNodeLocation with the lowest loadScore
   */
  public Optional<TDataNodeLocation> getLowestLoadDataNode() {
    // TODO get real lowest load data node after scoring algorithm being implemented
    int dataNodeId = getLoadManager().getLowestLoadDataNode();
    return dataNodeId < 0
        ? Optional.empty()
        : Optional.of(getRegisteredDataNode(dataNodeId).getLocation());
  }

  /**
   * Get the DataNodeLocation which has the lowest loadScore within input.
   *
   * @return TDataNodeLocation with the lowest loadScore
   */
  public TDataNodeLocation getLowestLoadDataNode(Set<Integer> nodes) {
    int dataNodeId = getLoadManager().getLowestLoadDataNode(new ArrayList<>(nodes));
    return getRegisteredDataNode(dataNodeId).getLocation();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private ClusterManager getClusterManager() {
    return configManager.getClusterManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  private TriggerManager getTriggerManager() {
    return configManager.getTriggerManager();
  }

  private PipeManager getPipeManager() {
    return configManager.getPipeManager();
  }

  private UDFManager getUDFManager() {
    return configManager.getUDFManager();
  }

  private TTLManager getTTLManager() {
    return configManager.getTTLManager();
  }
}
