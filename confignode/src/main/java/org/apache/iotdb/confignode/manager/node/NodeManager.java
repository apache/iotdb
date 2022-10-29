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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
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
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.BaseNodeCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.NodeStatistics;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.metric.NodeInfoMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.rpc.thrift.TCQConfig;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
  public static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatInterval();
  private static final long UNKNOWN_DATANODE_DETECT_INTERVAL =
      CONF.getUnknownDataNodeDetectInterval();

  public static final TEndPoint CURRENT_NODE =
      new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort());

  // when fail to register a new node, set node id to -1
  private static final int ERROR_STATUS_NODE_ID = -1;

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

  /** Unknown DataNode Detector */
  private Future<?> currentUnknownDataNodeDetectFuture;

  private final ScheduledExecutorService unknownDataNodeDetectExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Unknown-DataNode-Detector");
  private final Set<TDataNodeLocation> oldUnknownNodes;

  private final Random random;

  public NodeManager(IManager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.removeConfigNodeLock = new ReentrantLock();
    this.nodeCacheMap = new ConcurrentHashMap<>();
    this.oldUnknownNodes = new HashSet<>();
    this.random = new Random(System.currentTimeMillis());
  }

  private void setGlobalConfig(DataNodeRegisterResp dataSet) {
    // Set TGlobalConfig
    final ConfigNodeConfig configNodeConfig = ConfigNodeDescriptor.getInstance().getConf();
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    TGlobalConfig globalConfig = new TGlobalConfig();
    globalConfig.setDataRegionConsensusProtocolClass(
        configNodeConfig.getDataRegionConsensusProtocolClass());
    globalConfig.setSchemaRegionConsensusProtocolClass(
        configNodeConfig.getSchemaRegionConsensusProtocolClass());
    globalConfig.setSeriesPartitionSlotNum(configNodeConfig.getSeriesPartitionSlotNum());
    globalConfig.setSeriesPartitionExecutorClass(
        configNodeConfig.getSeriesPartitionExecutorClass());
    globalConfig.setTimePartitionInterval(configNodeConfig.getTimePartitionInterval());
    globalConfig.setReadConsistencyLevel(configNodeConfig.getReadConsistencyLevel());
    globalConfig.setDiskSpaceWarningThreshold(commonConfig.getDiskSpaceWarningThreshold());
    dataSet.setGlobalConfig(globalConfig);
  }

  private void setRatisConfig(DataNodeRegisterResp dataSet) {
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
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

    ratisConfig.setDataInitialSleepTime(conf.getDataRegionRatisInitialSleepTimeMs());
    ratisConfig.setDataMaxSleepTime(conf.getDataRegionRatisMaxSleepTimeMs());
    ratisConfig.setSchemaInitialSleepTime(conf.getSchemaRegionRatisInitialSleepTimeMs());
    ratisConfig.setSchemaMaxSleepTime(conf.getSchemaRegionRatisMaxSleepTimeMs());

    ratisConfig.setSchemaPreserveWhenPurge(conf.getPartitionRegionRatisPreserveLogsWhenPurge());
    ratisConfig.setDataPreserveWhenPurge(conf.getDataRegionRatisPreserveLogsWhenPurge());

    ratisConfig.setFirstElectionTimeoutMin(conf.getRatisFirstElectionTimeoutMinMs());
    ratisConfig.setFirstElectionTimeoutMax(conf.getRatisFirstElectionTimeoutMaxMs());

    dataSet.setRatisConfig(ratisConfig);
  }

  private void setCQConfig(DataNodeRegisterResp dataSet) {
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TCQConfig cqConfig = new TCQConfig();
    cqConfig.setCqMinEveryIntervalInMs(conf.getCqMinEveryIntervalInMs());

    dataSet.setCqConfig(cqConfig);
  }

  /**
   * Register DataNode
   *
   * @param registerDataNodePlan RegisterDataNodeReq
   * @return DataNodeConfigurationDataSet. The TSStatus will be set to SUCCESS_STATUS when register
   *     success, and DATANODE_ALREADY_REGISTERED when the DataNode is already exist.
   */
  public DataSet registerDataNode(RegisterDataNodePlan registerDataNodePlan) {
    DataNodeRegisterResp dataSet = new DataNodeRegisterResp();
    TSStatus status = new TSStatus();

    if (nodeInfo.isRegisteredDataNode(
        registerDataNodePlan.getDataNodeConfiguration().getLocation())) {
      status.setCode(TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode());
      status.setMessage("DataNode already registered.");
    } else if (registerDataNodePlan.getDataNodeConfiguration().getLocation().getDataNodeId() < 0) {
      // Generating a new dataNodeId only when current DataNode doesn't exist yet
      registerDataNodePlan
          .getDataNodeConfiguration()
          .getLocation()
          .setDataNodeId(nodeInfo.generateNextNodeId());
      getConsensusManager().write(registerDataNodePlan);

      // Adjust the maximum RegionGroup number of each StorageGroup
      getClusterSchemaManager().adjustMaxRegionGroupCount();

      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("registerDataNode success.");
    }

    dataSet.setStatus(status);
    dataSet.setDataNodeId(
        registerDataNodePlan.getDataNodeConfiguration().getLocation().getDataNodeId());
    dataSet.setConfigNodeList(getRegisteredConfigNodes());
    setGlobalConfig(dataSet);
    setRatisConfig(dataSet);
    setCQConfig(dataSet);
    return dataSet;
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
          "The remove DataNode request check failed.  req: {}, check result: {}",
          removeDataNodePlan,
          preCheckStatus.getStatus());
      return preCheckStatus;
    }

    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
    // do transfer of the DataNodes before remove
    if (configManager.transfer(removeDataNodePlan.getDataNodeLocations()).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(
          new TSStatus(TSStatusCode.NODE_DELETE_FAILED_ERROR.getStatusCode())
              .setMessage("Fail to do transfer of the DataNodes"));
      return dataSet;
    }
    // if add request to queue, then return to client
    boolean registerSucceed =
        configManager.getProcedureManager().removeDataNode(removeDataNodePlan);
    TSStatus status;
    if (registerSucceed) {
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("Server accepted the request");
    } else {
      status = new TSStatus(TSStatusCode.NODE_DELETE_FAILED_ERROR.getStatusCode());
      status.setMessage("Server rejected the request, maybe requests are too many");
    }
    dataSet.setStatus(status);

    LOGGER.info("NodeManager finished to remove DataNode {}", removeDataNodePlan);
    return dataSet;
  }

  /**
   * Update the specified DataNode‘s location
   *
   * @param updateDataNodePlan UpdateDataNodePlan
   * @return TSStatus. The TSStatus will be set to SUCCESS_STATUS when update success, and
   *     DATANODE_NOT_EXIST when some datanode is not exist, UPDATE_DATANODE_FAILED when update
   *     failed.
   */
  public DataSet updateDataNode(UpdateDataNodePlan updateDataNodePlan) {
    LOGGER.info("NodeManager start to update DataNode {}", updateDataNodePlan);

    DataNodeRegisterResp dataSet = new DataNodeRegisterResp();
    TSStatus status;
    // check if node is already exist
    boolean found = false;
    List<TDataNodeConfiguration> configurationList = getRegisteredDataNodes();
    for (TDataNodeConfiguration configuration : configurationList) {
      if (configuration.getLocation().getDataNodeId()
          == updateDataNodePlan.getDataNodeLocation().getDataNodeId()) {
        found = true;
        break;
      }
    }
    if (found) {
      getConsensusManager().write(updateDataNodePlan);
      status =
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
              .setMessage("updateDataNode(nodeId=%d) success.");
    } else {
      status =
          new TSStatus(TSStatusCode.DATANODE_NOT_EXIST.getStatusCode())
              .setMessage(
                  String.format(
                      "The specified DataNode(nodeId=%d) doesn't exist",
                      updateDataNodePlan.getDataNodeLocation().getDataNodeId()));
    }
    dataSet.setStatus(status);
    dataSet.setDataNodeId(updateDataNodePlan.getDataNodeLocation().getDataNodeId());
    dataSet.setConfigNodeList(getRegisteredConfigNodes());
    setGlobalConfig(dataSet);
    setRatisConfig(dataSet);
    return dataSet;
  }

  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    // Check global configuration
    TSStatus status = configManager.getConsensusManager().confirmLeader();

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      TSStatus errorStatus = configManager.checkConfigNodeGlobalConfig(req);
      if (errorStatus != null) {
        return new TConfigNodeRegisterResp()
            .setStatus(errorStatus)
            .setConfigNodeId(ERROR_STATUS_NODE_ID);
      }

      int nodeId = nodeInfo.generateNextNodeId();
      req.getConfigNodeLocation().setConfigNodeId(nodeId);

      configManager.getProcedureManager().addConfigNode(req);
      return new TConfigNodeRegisterResp().setStatus(StatusUtils.OK).setConfigNodeId(nodeId);
    }

    return new TConfigNodeRegisterResp().setStatus(status).setConfigNodeId(ERROR_STATUS_NODE_ID);
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
          (dataNodeInfo) -> {
            TDataNodeInfo info = new TDataNodeInfo();
            int dataNodeId = dataNodeInfo.getLocation().getDataNodeId();
            info.setDataNodeId(dataNodeId);
            info.setStatus(getNodeStatusWithReason(dataNodeId));
            info.setRpcAddresss(dataNodeInfo.getLocation().getClientRpcEndPoint().getIp());
            info.setRpcPort(dataNodeInfo.getLocation().getClientRpcEndPoint().getPort());
            info.setDataRegionNum(0);
            info.setSchemaRegionNum(0);
            dataNodeInfoList.add(info);
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
                configNodeLocation.getInternalEndPoint().equals(CURRENT_NODE)
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

  public void addMetrics() {
    MetricService.getInstance().addMetricSet(new NodeInfoMetrics(nodeInfo));
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
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
            .setMessage(
                "Remove ConfigNode failed because there is only one ConfigNode in current Cluster.");
      }

      // Check whether the registeredConfigNodes contain the ConfigNode to be removed.
      if (!getRegisteredConfigNodes().contains(removeConfigNodePlan.getConfigNodeLocation())) {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
            .setMessage("Remove ConfigNode failed because the ConfigNode not in current Cluster.");
      }

      // Check whether the remove ConfigNode is leader
      TConfigNodeLocation leader = getConsensusManager().getLeader();
      if (leader == null) {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
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
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
          .setMessage("Remove ConfigNode failed because transfer ConfigNode leader failed.");
    }
    return new TSStatus(TSStatusCode.NEED_REDIRECTION.getStatusCode())
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
      if (configNodeLocation.getInternalEndPoint().equals(CURRENT_NODE)) {
        // Skip itself
        continue;
      }

      ConfigNodeHeartbeatHandler handler =
          new ConfigNodeHeartbeatHandler(
              (ConfigNodeHeartbeatCache)
                  nodeCacheMap.computeIfAbsent(
                      configNodeLocation.getConfigNodeId(),
                      empty -> new ConfigNodeHeartbeatCache(configNodeLocation)));
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

  /** Start unknownDataNodeDetector */
  public void startUnknownDataNodeDetector() {
    synchronized (scheduleMonitor) {
      if (currentUnknownDataNodeDetectFuture == null) {
        currentUnknownDataNodeDetectFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                unknownDataNodeDetectExecutor,
                this::detectTask,
                0,
                UNKNOWN_DATANODE_DETECT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("Unknown-DataNode-Detector is started successfully.");
      }
    }
  }

  /**
   * The detectTask executed periodically to find newest UnknownDataNodes
   *
   * <p>1.If one DataNode is continuing Unknown, we shouldn't always activate Transfer of this Node.
   *
   * <p>2.The selected DataNodes may not truly need to transfer, so you should ensure safety of the
   * Data when implement transferMethod in Manager.
   */
  private void detectTask() {
    List<TDataNodeLocation> newUnknownNodes = new ArrayList<>();

    getRegisteredDataNodes()
        .forEach(
            DataNodeConfiguration -> {
              TDataNodeLocation dataNodeLocation = DataNodeConfiguration.getLocation();
              BaseNodeCache newestNodeInformation = nodeCacheMap.get(dataNodeLocation.dataNodeId);
              if (newestNodeInformation != null) {
                if (newestNodeInformation.getNodeStatus() == NodeStatus.Running) {
                  oldUnknownNodes.remove(dataNodeLocation);
                } else if (!oldUnknownNodes.contains(dataNodeLocation)
                    && newestNodeInformation.getNodeStatus() == NodeStatus.Unknown) {
                  newUnknownNodes.add(dataNodeLocation);
                }
              }
            });

    if (!newUnknownNodes.isEmpty()) {
      TSStatus transferResult = configManager.transfer(newUnknownNodes);
      if (transferResult.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        oldUnknownNodes.addAll(newUnknownNodes);
      }
    }
  }

  /** Stop the heartbeat service */
  public void stopUnknownDataNodeDetector() {
    synchronized (scheduleMonitor) {
      if (currentUnknownDataNodeDetectFuture != null) {
        currentUnknownDataNodeDetectFuture.cancel(false);
        currentUnknownDataNodeDetectFuture = null;
        LOGGER.info("Unknown-DataNode-Detector is stopped successfully.");
      }
    }
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

  /** Recover the nodeCacheMap when the ConfigNode-Leader is switched */
  public void recoverNodeCacheMap() {
    Map<Integer, NodeStatistics> nodeStatisticsMap = nodeInfo.getNodeStatisticsMap();
    nodeCacheMap.clear();
    LOGGER.info("[InheritLoadStatistics] Start to inherit NodeStatistics...");

    // Force update ConfigNode-leader
    nodeCacheMap.put(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        new ConfigNodeHeartbeatCache(
            new TConfigNodeLocation(
                ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
                CURRENT_NODE,
                new TEndPoint(
                    ConfigNodeDescriptor.getInstance().getConf().getInternalAddress(),
                    ConfigNodeDescriptor.getInstance().getConf().getConsensusPort())),
            ConfigNodeHeartbeatCache.CURRENT_NODE_STATISTICS));

    // Inherit ConfigNodeStatistics
    getRegisteredConfigNodes()
        .forEach(
            configNodeLocation -> {
              int configNodeId = configNodeLocation.getConfigNodeId();
              if (!configNodeLocation.getInternalEndPoint().equals(CURRENT_NODE)
                  && nodeStatisticsMap.containsKey(configNodeId)) {
                nodeCacheMap.put(configNodeId, new ConfigNodeHeartbeatCache(configNodeLocation));
                nodeCacheMap
                    .get(configNodeId)
                    .forceUpdate(
                        nodeStatisticsMap.get(configNodeId).convertToNodeHeartbeatSample());
                LOGGER.info(
                    "[InheritLoadStatistics]\t {}={}",
                    "nodeId{" + configNodeId + "}",
                    nodeCacheMap.get(configNodeId).getStatistics());
              }
            });

    // Inherit DataNodeStatistics
    getRegisteredDataNodes()
        .forEach(
            dataNodeConfiguration -> {
              int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
              if (nodeStatisticsMap.containsKey(dataNodeId)) {
                nodeCacheMap.put(dataNodeId, new DataNodeHeartbeatCache());
                nodeCacheMap
                    .get(dataNodeId)
                    .forceUpdate(nodeStatisticsMap.get(dataNodeId).convertToNodeHeartbeatSample());
                LOGGER.info(
                    "[InheritLoadStatistics]\t {}={}",
                    "nodeId{" + dataNodeId + "}",
                    nodeCacheMap.get(dataNodeId).getStatistics());
              }
            });

    LOGGER.info("[InheritLoadStatistics] Inherit NodeStatistics finish");
  }

  /**
   * @param nodeId The specified Node's index
   * @param isLatest Is the NodeStatistics latest
   * @return NodeStatistics in NodeCache if the isLatest is set to True, NodeStatistics in NodeInfo
   *     otherwise
   */
  public NodeStatistics getNodeStatistics(int nodeId, boolean isLatest) {
    if (isLatest) {
      return nodeCacheMap.containsKey(nodeId)
          ? nodeCacheMap.get(nodeId).getStatistics()
          : NodeStatistics.generateDefaultNodeStatistics();
    } else {
      return nodeInfo.getNodeStatisticsMap().containsKey(nodeId)
          ? nodeInfo.getNodeStatisticsMap().get(nodeId)
          : NodeStatistics.generateDefaultNodeStatistics();
    }
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
}
