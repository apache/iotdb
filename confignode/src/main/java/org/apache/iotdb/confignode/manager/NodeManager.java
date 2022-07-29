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

import org.apache.iotdb.common.rpc.thrift.TClearCacheReq;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
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
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/** NodeManager manages cluster node addition and removal requests */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private final IManager configManager;
  private final NodeInfo nodeInfo;

  private final ReentrantLock removeConfigNodeLock;

  /** TODO:do some operate after add node or remove node */
  private final List<ChangeServerListener> listeners = new CopyOnWriteArrayList<>();

  public NodeManager(IManager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.removeConfigNodeLock = new ReentrantLock();
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
    LOGGER.info("Node manager start to remove DataNode {}", removeDataNodePlan);

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

    LOGGER.info("Node manager finished to remove DataNode {}", removeDataNodePlan);
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
   * @param dataNodeId Specific DataNodeId
   * @return All registered DataNodes if dataNodeId equals -1. And return the specific DataNode
   *     otherwise.
   */
  public List<TDataNodeConfiguration> getRegisteredDataNodes(int dataNodeId) {
    return nodeInfo.getRegisteredDataNodes(dataNodeId);
  }

  public Map<Integer, TDataNodeLocation> getRegisteredDataNodeLocations(int dataNodeId) {
    Map<Integer, TDataNodeLocation> dataNodeLocations = new ConcurrentHashMap<>();
    nodeInfo
        .getRegisteredDataNodes(dataNodeId)
        .forEach(
            dataNodeConfiguration ->
                dataNodeLocations.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    return dataNodeLocations;
  }

  public List<TDataNodeInfo> getRegisteredDataNodeInfoList() {
    List<TDataNodeInfo> dataNodeInfoList = new ArrayList<>();
    List<TDataNodeConfiguration> registeredDataNodes = this.getRegisteredDataNodes(-1);
    if (registeredDataNodes != null) {
      registeredDataNodes.forEach(
          (dataNodeInfo) -> {
            TDataNodeInfo info = new TDataNodeInfo();
            int dataNodeId = dataNodeInfo.getLocation().getDataNodeId();
            info.setDataNodeId(dataNodeId);
            info.setStatus(
                getLoadManager().getNodeCacheMap().get(dataNodeId).getNodeStatus().getStatus());
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
            info.setStatus(
                getLoadManager().getNodeCacheMap().get(configNodeId).getNodeStatus().getStatus());
            info.setInternalAddress(configNodeLocation.getInternalEndPoint().getIp());
            info.setInternalPort(configNodeLocation.getInternalEndPoint().getPort());
            configNodeInfoList.add(info);
          });
    }
    configNodeInfoList.sort(Comparator.comparingInt(TConfigNodeInfo::getConfigNodeId));
    return configNodeInfoList;
  }

  /**
   * Provides ConfigNodeGroup information for the newly registered ConfigNode
   *
   * @param req TConfigNodeRegisterReq
   * @return TConfigNodeRegisterResp with PartitionRegionId and online ConfigNodes
   */
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    TConfigNodeRegisterResp resp = new TConfigNodeRegisterResp();

    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    // Return PartitionRegionId
    resp.setPartitionRegionId(
        getConsensusManager().getConsensusGroupId().convertToTConsensusGroupId());

    resp.setConfigNodeList(nodeInfo.getRegisteredConfigNodes());
    return resp;
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
    nodeInfo.addMetrics();
  }

  public TSStatus removeConfigNodePeer(TConfigNodeLocation tConfigNodeLocation) {
    removeConfigNodeLock.tryLock();
    try {
      // Execute removePeer
      if (getConsensusManager().removeConfigNodePeer(tConfigNodeLocation)) {
        configManager
            .getLoadManager()
            .removeNodeHeartbeatHandCache(tConfigNodeLocation.getConfigNodeId());
        return getConsensusManager()
            .write(new RemoveConfigNodePlan(tConfigNodeLocation))
            .getStatus();
      } else {
        return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
            .setMessage(
                "Remove ConfigNode failed because update ConsensusGroup peer information failed.");
      }
    } finally {
      removeConfigNodeLock.unlock();
    }
  }

  public TSStatus checkConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    removeConfigNodeLock.tryLock();
    try {
      // Check OnlineConfigNodes number
      if (getLoadManager().getOnlineConfigNodes().size() <= 1) {
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
        getLoadManager().getOnlineConfigNodes().stream()
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

  public List<TConfigNodeLocation> getRegisteredConfigNodes() {
    return nodeInfo.getRegisteredConfigNodes();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  public void registerListener(final ChangeServerListener serverListener) {
    listeners.add(serverListener);
  }

  public boolean unregisterListener(final ChangeServerListener serverListener) {
    return listeners.remove(serverListener);
  }

  /** TODO: wait data node register, wait */
  public void waitForDataNodes() {
    listeners.stream().forEach(serverListener -> serverListener.waiting());
  }

  private class ServerStartListenerThread extends Thread implements ChangeServerListener {
    private boolean changed = false;

    ServerStartListenerThread() {
      setDaemon(true);
    }

    @Override
    public void addDataNode(TDataNodeLocation DataNodeInfo) {
      serverChanged();
    }

    @Override
    public void removeDataNode(TDataNodeLocation dataNodeInfo) {
      // TODO: When removing a datanode, do the following
      //  configManager.getLoadManager().removeNodeHeartbeatHandCache(dataNodeId);
      serverChanged();
    }

    private synchronized void serverChanged() {
      changed = true;
      this.notify();
    }

    @Override
    public void run() {
      while (!configManager.isStopped()) {}
    }
  }

  /** TODO: For listener for add or remove data node */
  public interface ChangeServerListener {

    /** Started waiting on DataNode to check */
    default void waiting() {};

    /**
     * The server has joined the cluster
     *
     * @param dataNodeInfo datanode info
     */
    void addDataNode(final TDataNodeLocation dataNodeInfo);

    /**
     * remove data node
     *
     * @param dataNodeInfo data node info
     */
    void removeDataNode(final TDataNodeLocation dataNodeInfo);
  }

  public List<TSStatus> flush(TFlushReq req) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations(req.dataNodeId);
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));

    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            req, dataNodeLocationMap, DataNodeRequestType.FLUSH, dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  public List<TSStatus> clearCache(TClearCacheReq req) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations(req.dataNodeId);
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            req, dataNodeLocationMap, DataNodeRequestType.CLEAR_CACHE, dataNodeResponseStatus);
    return dataNodeResponseStatus;
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
