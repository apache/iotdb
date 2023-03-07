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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.datanode.ConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.TriggerManager;
import org.apache.iotdb.confignode.manager.UDFManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.BaseNodeCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** NodeManager manages cluster node addition and removal requests */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  public static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();

  private final IManager configManager;
  private final NodeInfo nodeInfo;

  private final ReentrantLock removeConfigNodeLock;

  /** Heartbeat executor service */
  // Monitor for leadership change
  private final Object scheduleMonitor = new Object();
  // Map<NodeId, INodeCache>
  private final Map<Integer, BaseNodeCache> nodeCacheMap;
  private final AtomicInteger heartbeatCounter = new AtomicInteger(0);
  private Future<?> currentHeartbeatFuture;
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-Heartbeat-Service");

  private final Random random;

  public NodeManager(IManager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.removeConfigNodeLock = new ReentrantLock();
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.random = new Random(System.currentTimeMillis());
  }

  /**
   * Get system configurations
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
    globalConfig.setTimePartitionInterval(configNodeConfig.getTimePartitionInterval());
    globalConfig.setReadConsistencyLevel(configNodeConfig.getReadConsistencyLevel());
    globalConfig.setDiskSpaceWarningThreshold(commonConfig.getDiskSpaceWarningThreshold());
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

    dataSet.setRatisConfig(ratisConfig);
  }

  private void setCQConfig(ConfigurationResp dataSet) {
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TCQConfig cqConfig = new TCQConfig();
    cqConfig.setCqMinEveryIntervalInMs(conf.getCqMinEveryIntervalInMs());

    dataSet.setCqConfig(cqConfig);
  }

  private TRuntimeConfiguration getRuntimeConfiguration() {
    getTriggerManager().getTriggerInfo().acquireTriggerTableLock();
    getUDFManager().getUdfInfo().acquireUDFTableLock();

    try {
      TRuntimeConfiguration runtimeConfiguration = new TRuntimeConfiguration();
      runtimeConfiguration.setTemplateInfo(getClusterSchemaManager().getAllTemplateSetInfo());
      runtimeConfiguration.setAllTriggerInformation(
          getTriggerManager().getTriggerTable(false).getAllTriggerInformation());
      runtimeConfiguration.setAllUDFInformation(
          getUDFManager().getUDFTable().getAllUDFInformation());
      runtimeConfiguration.setAllTTLInformation(
          DataNodeRegisterResp.convertAllTTLInformation(getClusterSchemaManager().getAllTTLInfo()));
      return runtimeConfiguration;
    } finally {
      getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
      getUDFManager().getUdfInfo().releaseUDFTableLock();
    }
  }

  /**
   * Register DataNode
   *
   * @param registerDataNodePlan RegisterDataNodeReq
   * @return DataNodeConfigurationDataSet. The TSStatus will be set to SUCCESS_STATUS when register
   *     success, and DATANODE_ALREADY_REGISTERED when the DataNode is already exist.
   */
  public DataSet registerDataNode(RegisterDataNodePlan registerDataNodePlan) {
    int dataNodeId = nodeInfo.generateNextNodeId();
    DataNodeRegisterResp resp = new DataNodeRegisterResp();

    // Register new DataNode
    registerDataNodePlan.getDataNodeConfiguration().getLocation().setDataNodeId(dataNodeId);
    getConsensusManager().write(registerDataNodePlan);

    // Bind DataNode metrics
    PartitionMetrics.bindDataNodePartitionMetrics(configManager, dataNodeId);

    // Adjust the maximum RegionGroup number of each StorageGroup
    getClusterSchemaManager().adjustMaxRegionGroupNum();

    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_REGISTRATION);
    resp.setConfigNodeList(getRegisteredConfigNodes());
    resp.setDataNodeId(
        registerDataNodePlan.getDataNodeConfiguration().getLocation().getDataNodeId());
    resp.setRuntimeConfiguration(getRuntimeConfiguration());
    return resp;
  }

  public TDataNodeRestartResp updateDataNodeIfNecessary(
      TDataNodeConfiguration dataNodeConfiguration) {
    TDataNodeConfiguration recordConfiguration =
        getRegisteredDataNode(dataNodeConfiguration.getLocation().getDataNodeId());
    if (!recordConfiguration.equals(dataNodeConfiguration)) {
      // Update DataNodeConfiguration when modified during restart
      UpdateDataNodePlan updateDataNodePlan = new UpdateDataNodePlan(dataNodeConfiguration);
      getConsensusManager().write(updateDataNodePlan);
    }

    TDataNodeRestartResp resp = new TDataNodeRestartResp();
    resp.setStatus(ClusterNodeStartUtils.ACCEPT_NODE_RESTART);
    resp.setConfigNodeList(getRegisteredConfigNodes());
    resp.setRuntimeConfiguration(getRuntimeConfiguration());
    return resp;
  }

  /**
   * Remove DataNodes
   *
   * @param removeDataNodePlan removeDataNodePlan
   * @return DataNodeToStatusResp, The TSStatus will be SUCCEED_STATUS if the request is accepted,
   *     DATANODE_NOT_EXIST when some datanode does not exist.
   */
  public DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    LOGGER.info("NodeManager start to remove DataNode {}", removeDataNodePlan);

    DataNodeRemoveHandler dataNodeRemoveHandler =
        new DataNodeRemoveHandler((ConfigManager) configManager);
    DataNodeToStatusResp preCheckStatus =
        dataNodeRemoveHandler.checkRemoveDataNodeRequest(removeDataNodePlan);
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

  public TSStatus restartConfigNode(TConfigNodeLocation configNodeLocation) {
    // TODO: @Itami-Sho, update peer if necessary
    return ClusterNodeStartUtils.ACCEPT_NODE_RESTART;
  }

  /**
   * Get TDataNodeConfiguration
   *
   * @param req GetDataNodeConfigurationPlan
   * @return The specific DataNode's configuration or all DataNodes' configuration if dataNodeId in
   *     GetDataNodeConfigurationPlan is -1
   */
  public DataNodeConfigurationResp getDataNodeConfiguration(GetDataNodeConfigurationPlan req) {
    return (DataNodeConfigurationResp) getConsensusManager().read(req).getDataset();
  }

  /**
   * Only leader use this interface
   *
   * @return The number of registered DataNodes
   */
  public int getRegisteredDataNodeCount() {
    return nodeInfo.getRegisteredDataNodeCount();
  }

  /**
   * Only leader use this interface
   *
   * @return The number of total cpu cores in online DataNodes
   */
  public int getTotalCpuCoreCount() {
    return nodeInfo.getTotalCpuCoreCount();
  }

  /**
   * Only leader use this interface
   *
   * @return All registered DataNodes
   */
  public List<TDataNodeConfiguration> getRegisteredDataNodes() {
    return nodeInfo.getRegisteredDataNodes();
  }

  /**
   * Only leader use this interface
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

  public List<TDataNodeInfo> getRegisteredDataNodeInfoList() {
    List<TDataNodeInfo> dataNodeInfoList = new ArrayList<>();
    List<TDataNodeConfiguration> registeredDataNodes = this.getRegisteredDataNodes();
    if (registeredDataNodes != null) {
      registeredDataNodes.forEach(
          (registeredDataNode) -> {
            TDataNodeInfo dataNodeInfo = new TDataNodeInfo();
            int dataNodeId = registeredDataNode.getLocation().getDataNodeId();
            dataNodeInfo.setDataNodeId(dataNodeId);
            dataNodeInfo.setStatus(getNodeStatusWithReason(dataNodeId));
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

  public List<TConfigNodeLocation> getRegisteredConfigNodes() {
    return nodeInfo.getRegisteredConfigNodes();
  }

  public List<TConfigNodeInfo> getRegisteredConfigNodeInfoList() {
    List<TConfigNodeInfo> configNodeInfoList = new ArrayList<>();
    List<TConfigNodeLocation> registeredConfigNodes = this.getRegisteredConfigNodes();
    if (registeredConfigNodes != null) {
      registeredConfigNodes.forEach(
          (configNodeLocation) -> {
            TConfigNodeInfo info = new TConfigNodeInfo();
            int configNodeId = configNodeLocation.getConfigNodeId();
            info.setConfigNodeId(configNodeId);
            info.setStatus(getNodeStatusWithReason(configNodeId));
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
   * Only leader use this interface, record the new ConfigNode's information
   *
   * @param configNodeLocation The new ConfigNode
   */
  public void applyConfigNode(TConfigNodeLocation configNodeLocation) {
    ApplyConfigNodePlan applyConfigNodePlan = new ApplyConfigNodePlan(configNodeLocation);
    getConsensusManager().write(applyConfigNodePlan);
  }

  /**
   * Only leader use this interface, check the ConfigNode before remove it
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
      TConfigNodeLocation leader = getConsensusManager().getLeader();
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
    TConfigNodeLocation newLeader =
        filterConfigNodeThroughStatus(NodeStatus.Running).stream()
            .filter(e -> !e.equals(removeConfigNodePlan.getConfigNodeLocation()))
            .findAny()
            .get();
    ConsensusGenericResponse resp =
        getConsensusManager()
            .getConsensusImpl()
            .transferLeader(
                groupId,
                new Peer(groupId, newLeader.getConfigNodeId(), newLeader.getConsensusEndPoint()));
    if (!resp.isSuccess()) {
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
    AsyncClientHandler<Object, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.MERGE, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> flush(TFlushReq req) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TFlushReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.FLUSH, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> clearCache() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<Object, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CLEAR_CACHE, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> loadConfiguration() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<Object, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.LOAD_CONFIGURATION, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public List<TSStatus> setSystemStatus(String status) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<String, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.SET_SYSTEM_STATUS, status, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq setDataNodeStatusReq) {
    return SyncDataNodeClientPool.getInstance()
        .sendSyncRequestToDataNodeWithRetry(
            setDataNodeStatusReq.getTargetDataNode().getInternalEndPoint(),
            setDataNodeStatusReq.getStatus(),
            DataNodeRequestType.SET_SYSTEM_STATUS);
  }

  /**
   * Kill query on DataNode
   *
   * @param queryId the id of specific query need to be killed, it will be NULL if kill all queries
   * @param dataNodeId the DataNode obtains target query, -1 means we will kill all queries on all
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
    AsyncClientHandler<String, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.KILL_QUERY_INSTANCE, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
  }

  private TSStatus killSpecificQuery(String queryId, TDataNodeLocation dataNodeLocation) {
    if (dataNodeLocation == null) {
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(
              "The target DataNode is not existed, please ensure your input <queryId> is correct");
    } else {
      return SyncDataNodeClientPool.getInstance()
          .sendSyncRequestToDataNodeWithRetry(
              dataNodeLocation.getInternalEndPoint(),
              queryId,
              DataNodeRequestType.KILL_QUERY_INSTANCE);
    }
  }

  /** Start the heartbeat service */
  public void startHeartbeatService() {
    synchronized (scheduleMonitor) {
      if (currentHeartbeatFuture == null) {
        currentHeartbeatFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                heartBeatExecutor,
                this::heartbeatLoopBody,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("Heartbeat service is started successfully.");
      }
    }
  }

  /** loop body of the heartbeat thread */
  private void heartbeatLoopBody() {
    // The consensusManager of configManager may not be fully initialized at this time
    Optional.ofNullable(getConsensusManager())
        .ifPresent(
            consensusManager -> {
              if (getConsensusManager().isLeader()) {
                // Generate HeartbeatReq
                THeartbeatReq heartbeatReq = genHeartbeatReq();
                // Send heartbeat requests to all the registered DataNodes
                pingRegisteredDataNodes(heartbeatReq, getRegisteredDataNodes());
                // Send heartbeat requests to all the registered ConfigNodes
                pingRegisteredConfigNodes(heartbeatReq, getRegisteredConfigNodes());
              }
            });
  }

  private THeartbeatReq genHeartbeatReq() {
    /* Generate heartbeat request */
    THeartbeatReq heartbeatReq = new THeartbeatReq();
    heartbeatReq.setHeartbeatTimestamp(System.currentTimeMillis());
    // Always sample RegionGroups' leadership as the Region heartbeat
    heartbeatReq.setNeedJudgeLeader(true);
    // We sample DataNode's load in every 10 heartbeat loop
    heartbeatReq.setNeedSamplingLoad(heartbeatCounter.get() % 10 == 0);

    /* Update heartbeat counter */
    heartbeatCounter.getAndUpdate((x) -> (x + 1) % 10);
    return heartbeatReq;
  }

  /**
   * Send heartbeat requests to all the Registered DataNodes
   *
   * @param registeredDataNodes DataNodes that registered in cluster
   */
  private void pingRegisteredDataNodes(
      THeartbeatReq heartbeatReq, List<TDataNodeConfiguration> registeredDataNodes) {
    // Send heartbeat requests
    for (TDataNodeConfiguration dataNodeInfo : registeredDataNodes) {
      DataNodeHeartbeatHandler handler =
          new DataNodeHeartbeatHandler(
              dataNodeInfo.getLocation(),
              (DataNodeHeartbeatCache)
                  nodeCacheMap.computeIfAbsent(
                      dataNodeInfo.getLocation().getDataNodeId(),
                      empty -> new DataNodeHeartbeatCache()),
              getPartitionManager().getRegionGroupCacheMap(),
              getLoadManager().getRouteBalancer());
      AsyncDataNodeHeartbeatClientPool.getInstance()
          .getDataNodeHeartBeat(
              dataNodeInfo.getLocation().getInternalEndPoint(), heartbeatReq, handler);
    }
  }

  /**
   * Send heartbeat requests to all the Registered ConfigNodes
   *
   * @param registeredConfigNodes ConfigNodes that registered in cluster
   */
  private void pingRegisteredConfigNodes(
      THeartbeatReq heartbeatReq, List<TConfigNodeLocation> registeredConfigNodes) {
    // Send heartbeat requests
    for (TConfigNodeLocation configNodeLocation : registeredConfigNodes) {
      if (configNodeLocation.getConfigNodeId() == ConfigNodeHeartbeatCache.CURRENT_NODE_ID) {
        // Skip itself
        continue;
      }

      ConfigNodeHeartbeatHandler handler =
          new ConfigNodeHeartbeatHandler(
              (ConfigNodeHeartbeatCache)
                  nodeCacheMap.computeIfAbsent(
                      configNodeLocation.getConfigNodeId(),
                      empty -> new ConfigNodeHeartbeatCache(configNodeLocation.getConfigNodeId())));
      AsyncConfigNodeHeartbeatClientPool.getInstance()
          .getConfigNodeHeartBeat(
              configNodeLocation.getInternalEndPoint(),
              heartbeatReq.getHeartbeatTimestamp(),
              handler);
    }
  }

  /** Stop the heartbeat service */
  public void stopHeartbeatService() {
    synchronized (scheduleMonitor) {
      if (currentHeartbeatFuture != null) {
        currentHeartbeatFuture.cancel(false);
        currentHeartbeatFuture = null;
        nodeCacheMap.clear();
        LOGGER.info("Heartbeat service is stopped successfully.");
      }
    }
  }

  public Map<Integer, BaseNodeCache> getNodeCacheMap() {
    return nodeCacheMap;
  }

  public void removeNodeCache(int nodeId) {
    nodeCacheMap.remove(nodeId);
  }

  /**
   * Safely get the specific Node's current status for showing cluster
   *
   * @param nodeId The specific Node's index
   * @return The specific Node's current status if the nodeCache contains it, Unknown otherwise
   */
  private String getNodeStatusWithReason(int nodeId) {
    BaseNodeCache nodeCache = nodeCacheMap.get(nodeId);
    return nodeCache == null
        ? NodeStatus.Unknown.getStatus() + "(NoHeartbeat)"
        : nodeCache.getNodeStatusWithReason();
  }

  /**
   * Filter the registered ConfigNodes through the specific NodeStatus
   *
   * @param status The specific NodeStatus
   * @return Filtered ConfigNodes with the specific NodeStatus
   */
  public List<TConfigNodeLocation> filterConfigNodeThroughStatus(NodeStatus... status) {
    return getRegisteredConfigNodes().stream()
        .filter(
            registeredConfigNode -> {
              int configNodeId = registeredConfigNode.getConfigNodeId();
              return nodeCacheMap.containsKey(configNodeId)
                  && Arrays.stream(status)
                      .anyMatch(s -> s.equals(nodeCacheMap.get(configNodeId).getNodeStatus()));
            })
        .collect(Collectors.toList());
  }

  /**
   * Get NodeStatus by nodeId
   *
   * @param nodeId The specific NodeId
   * @return NodeStatus of the specific node. If node does not exist, return null.
   */
  public NodeStatus getNodeStatusByNodeId(int nodeId) {
    BaseNodeCache baseNodeCache = nodeCacheMap.get(nodeId);
    return baseNodeCache == null ? null : baseNodeCache.getNodeStatus();
  }

  /**
   * Filter the registered DataNodes through the specific NodeStatus
   *
   * @param status The specific NodeStatus
   * @return Filtered DataNodes with the specific NodeStatus
   */
  public List<TDataNodeConfiguration> filterDataNodeThroughStatus(NodeStatus... status) {
    return getRegisteredDataNodes().stream()
        .filter(
            registeredDataNode -> {
              int dataNodeId = registeredDataNode.getLocation().getDataNodeId();
              return nodeCacheMap.containsKey(dataNodeId)
                  && Arrays.stream(status)
                      .anyMatch(s -> s.equals(nodeCacheMap.get(dataNodeId).getNodeStatus()));
            })
        .collect(Collectors.toList());
  }

  /**
   * Get the loadScore of each DataNode
   *
   * @return Map<DataNodeId, loadScore>
   */
  public Map<Integer, Long> getAllLoadScores() {
    Map<Integer, Long> result = new ConcurrentHashMap<>();

    nodeCacheMap.forEach(
        (dataNodeId, heartbeatCache) -> result.put(dataNodeId, heartbeatCache.getLoadScore()));

    return result;
  }

  /**
   * Get the free disk space of the specified DataNode
   *
   * @param dataNodeId The index of the specified DataNode
   * @return The free disk space that sample through heartbeat, 0 if no heartbeat received
   */
  public double getFreeDiskSpace(int dataNodeId) {
    DataNodeHeartbeatCache dataNodeHeartbeatCache =
        (DataNodeHeartbeatCache) nodeCacheMap.get(dataNodeId);
    return dataNodeHeartbeatCache == null ? 0d : dataNodeHeartbeatCache.getFreeDiskSpace();
  }

  /**
   * Get the DataNodeLocation of the DataNode which has the lowest loadScore
   *
   * @return TDataNodeLocation with the lowest loadScore
   */
  public Optional<TDataNodeLocation> getLowestLoadDataNode() {
    // TODO get real lowest load data node after scoring algorithm being implemented
    List<TDataNodeConfiguration> targetDataNodeList =
        filterDataNodeThroughStatus(NodeStatus.Running);

    if (targetDataNodeList == null || targetDataNodeList.isEmpty()) {
      return Optional.empty();
    } else {
      int index = random.nextInt(targetDataNodeList.size());
      return Optional.of(targetDataNodeList.get(index).location);
    }
  }

  /**
   * Get the DataNodeLocation which has the lowest loadScore within input
   *
   * @return TDataNodeLocation with the lowest loadScore
   */
  public TDataNodeLocation getLowestLoadDataNode(Set<Integer> nodes) {
    AtomicInteger result = new AtomicInteger();
    AtomicLong lowestLoadScore = new AtomicLong(Long.MAX_VALUE);

    nodes.forEach(
        nodeID -> {
          BaseNodeCache cache = nodeCacheMap.get(nodeID);
          long score = (cache == null) ? Long.MAX_VALUE : cache.getLoadScore();
          if (score < lowestLoadScore.get()) {
            result.set(nodeID);
            lowestLoadScore.set(score);
          }
        });

    LOGGER.info(
        "get the lowest load DataNode, NodeID: [{}], LoadScore: [{}]", result, lowestLoadScore);
    return configManager.getNodeManager().getRegisteredDataNodeLocations().get(result.get());
  }

  /** Initialize the nodeCacheMap when the ConfigNode-Leader is switched */
  public void initNodeHeartbeatCache() {
    final int CURRENT_NODE_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
    nodeCacheMap.clear();

    // Init ConfigNodeHeartbeatCache
    getRegisteredConfigNodes()
        .forEach(
            configNodeLocation -> {
              if (configNodeLocation.getConfigNodeId() != CURRENT_NODE_ID) {
                nodeCacheMap.put(
                    configNodeLocation.getConfigNodeId(),
                    new ConfigNodeHeartbeatCache(configNodeLocation.getConfigNodeId()));
              }
            });
    // Force set itself and never update
    nodeCacheMap.put(
        ConfigNodeHeartbeatCache.CURRENT_NODE_ID,
        new ConfigNodeHeartbeatCache(
            CURRENT_NODE_ID, ConfigNodeHeartbeatCache.CURRENT_NODE_STATISTICS));

    // Init DataNodeHeartbeatCache
    getRegisteredDataNodes()
        .forEach(
            dataNodeConfiguration ->
                nodeCacheMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    new DataNodeHeartbeatCache()));
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
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

  private UDFManager getUDFManager() {
    return configManager.getUDFManager();
  }
}
