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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.confignode.AsyncConfigNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.ConfigNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.heartbeat.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.persistence.metric.NodeInfoMetrics;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TRatisConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** NodeManager manages cluster node addition and removal requests */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  public static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatInterval();

  public static final TEndPoint CURRENT_NODE =
      new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort());

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
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(LoadManager.class.getSimpleName());

  public NodeManager(IManager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.removeConfigNodeLock = new ReentrantLock();
    this.nodeCacheMap = new ConcurrentHashMap<>();
  }

  private void setGlobalConfig(DataNodeRegisterResp dataSet) {
    // Set TGlobalConfig
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TGlobalConfig globalConfig = new TGlobalConfig();
    globalConfig.setDataRegionConsensusProtocolClass(conf.getDataRegionConsensusProtocolClass());
    globalConfig.setSchemaRegionConsensusProtocolClass(
        conf.getSchemaRegionConsensusProtocolClass());
    globalConfig.setSeriesPartitionSlotNum(conf.getSeriesPartitionSlotNum());
    globalConfig.setSeriesPartitionExecutorClass(conf.getSeriesPartitionExecutorClass());
    globalConfig.setTimePartitionInterval(conf.getTimePartitionInterval());
    globalConfig.setReadConsistencyLevel(conf.getReadConsistencyLevel());
    dataSet.setGlobalConfig(globalConfig);
  }

  private void setRatisConfig(DataNodeRegisterResp dataSet) {
    final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TRatisConfig ratisConfig = new TRatisConfig();
    ratisConfig.setAppenderBufferSize(conf.getRatisConsensusLogAppenderBufferSize());
    dataSet.setRatisConfig(ratisConfig);
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

    if (nodeInfo.isRegisteredDataNode(registerDataNodePlan.getInfo().getLocation())) {
      status.setCode(TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode());
      status.setMessage("DataNode already registered.");
    } else if (registerDataNodePlan.getInfo().getLocation().getDataNodeId() < 0) {
      // Generating a new dataNodeId only when current DataNode doesn't exist yet
      registerDataNodePlan.getInfo().getLocation().setDataNodeId(nodeInfo.generateNextNodeId());
      getConsensusManager().write(registerDataNodePlan);

      // Adjust the maximum RegionGroup number of each StorageGroup
      getClusterSchemaManager().adjustMaxRegionGroupCount();

      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("registerDataNode success.");
    }

    dataSet.setStatus(status);
    dataSet.setDataNodeId(registerDataNodePlan.getInfo().getLocation().getDataNodeId());
    dataSet.setConfigNodeList(getRegisteredConfigNodes());
    setGlobalConfig(dataSet);
    setRatisConfig(dataSet);
    return dataSet;
  }

  /**
   * Remove DataNodes
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return DataNodeToStatusResp, The TSStatue will be SUCCEED_STATUS when request is accept,
   *     DATANODE_NOT_EXIST when some datanode not exist.
   */
  public DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    LOGGER.info("NodeManager start to remove DataNode {}", removeDataNodePlan);

    DataNodeRemoveHandler dataNodeRemoveHandler =
        new DataNodeRemoveHandler((ConfigManager) configManager);
    DataNodeToStatusResp preCheckStatus =
        dataNodeRemoveHandler.checkRemoveDataNodeRequest(removeDataNodePlan);
    if (preCheckStatus.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.error(
          "the remove Data Node request check failed.  req: {}, check result: {}",
          removeDataNodePlan,
          preCheckStatus.getStatus());
      return preCheckStatus;
    }
    // if add request to queue, then return to client
    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
    boolean registerSucceed =
        configManager.getProcedureManager().removeDataNode(removeDataNodePlan);
    TSStatus status;
    if (registerSucceed) {
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("Server accept the request");
    } else {
      status = new TSStatus(TSStatusCode.NODE_DELETE_FAILED_ERROR.getStatusCode());
      status.setMessage("Server reject the request, maybe request is too much");
    }
    dataSet.setStatus(status);

    LOGGER.info("NodeManager finished to remove DataNode {}", removeDataNodePlan);
    return dataSet;
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
            info.setStatus(getNodeStatus(dataNodeId));
            info.setRpcAddresss(dataNodeInfo.getLocation().getClientRpcEndPoint().getIp());
            info.setRpcPort(dataNodeInfo.getLocation().getClientRpcEndPoint().getPort());
            info.setDataRegionNum(0);
            info.setSchemaRegionNum(0);
            dataNodeInfoList.add(info);
          });
    }
    return dataNodeInfoList;
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
            info.setStatus(getNodeStatus(configNodeId));
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
    // Generate new ConfigNode's index
    configNodeLocation.setConfigNodeId(nodeInfo.generateNextNodeId());
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
    removeConfigNodeLock.tryLock();
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
        .setMessage("Success remove confignode.");
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
            .transferLeader(groupId, new Peer(groupId, newLeader.getConsensusEndPoint()));
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
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            null, dataNodeLocationMap, DataNodeRequestType.MERGE, dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  public List<TSStatus> flush(TFlushReq req) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            req, dataNodeLocationMap, DataNodeRequestType.FLUSH, dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  public List<TSStatus> clearCache() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            null, dataNodeLocationMap, DataNodeRequestType.CLEAR_CACHE, dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  public List<TSStatus> loadConfiguration() {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            null,
            dataNodeLocationMap,
            DataNodeRequestType.LOAD_CONFIGURATION,
            dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  public List<TSStatus> setSystemStatus(String status) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            status,
            dataNodeLocationMap,
            DataNodeRequestType.SET_SYSTEM_STATUS,
            dataNodeResponseStatus);
    return dataNodeResponseStatus;
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
    // the consensusManager of configManager may not be fully initialized at this time
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
    // We update RegionGroups' leadership in every 5 heartbeat loop
    heartbeatReq.setNeedJudgeLeader(heartbeatCounter.get() % 5 == 0);
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
              getPartitionManager().getRegionGroupCacheMap());
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
        nodeCacheMap.putIfAbsent(
            configNodeLocation.getConfigNodeId(), new ConfigNodeHeartbeatCache(configNodeLocation));
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

  public void removeNodeCache(int nodeId) {
    nodeCacheMap.remove(nodeId);
  }

  /**
   * Safely get the specific Node's current status
   *
   * @param nodeId The specific Node's index
   * @return The specific Node's current status if the nodeCache contains it, Unknown otherwise
   */
  private String getNodeStatus(int nodeId) {
    BaseNodeCache nodeCache = nodeCacheMap.get(nodeId);
    return nodeCache == null
        ? NodeStatus.Unknown.getStatus()
        : nodeCache.getNodeStatus().getStatus();
  }

  /**
   * Filter the registered ConfigNodes through the specific NodeStatus
   *
   * @param status The specific NodeStatus
   * @return Filtered ConfigNodes with the specific NodeStatus
   */
  public List<TConfigNodeLocation> filterConfigNodeThroughStatus(NodeStatus status) {
    return getRegisteredConfigNodes().stream()
        .filter(
            registeredConfigNode -> {
              int configNodeId = registeredConfigNode.getConfigNodeId();
              return nodeCacheMap.containsKey(configNodeId)
                  && status.equals(nodeCacheMap.get(configNodeId).getNodeStatus());
            })
        .collect(Collectors.toList());
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
              int id = registeredDataNode.getLocation().getDataNodeId();
              return nodeCacheMap.containsKey(id)
                  && Arrays.stream(status)
                      .anyMatch(s -> s.equals(nodeCacheMap.get(id).getNodeStatus()));
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

  public boolean isNodeRemoving(int dataNodeId) {
    DataNodeHeartbeatCache cache =
        (DataNodeHeartbeatCache) configManager.getNodeManager().getNodeCacheMap().get(dataNodeId);
    if (cache != null) {
      return cache.isRemoving();
    }
    return false;
  }

  public void setNodeRemovingStatus(int dataNodeId, boolean isRemoving) {
    DataNodeHeartbeatCache cache =
        (DataNodeHeartbeatCache) configManager.getNodeManager().getNodeCacheMap().get(dataNodeId);
    if (cache != null) {
      cache.setRemoving(isRemoving);
    }
  }

  public List<TConfigNodeLocation> getRegisteredConfigNodes() {
    return nodeInfo.getRegisteredConfigNodes();
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
}
