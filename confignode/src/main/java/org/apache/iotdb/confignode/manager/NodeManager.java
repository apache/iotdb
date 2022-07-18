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
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodesInfo;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.async.handlers.FlushHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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

  private void setGlobalConfig(DataNodeConfigurationResp dataSet) {
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
    DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
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
    DataNodeToStatusResp preCheckStatus =
        configManager.getDataNodeRemoveManager().checkRemoveDataNodeRequest(removeDataNodePlan);
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
        configManager.getDataNodeRemoveManager().registerRequest(removeDataNodePlan);
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
   * Get DataNode info
   *
   * @param req QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeInfosResp getDataNodeInfo(GetDataNodeInfoPlan req) {
    return (DataNodeInfosResp) getConsensusManager().read(req).getDataset();
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
  public List<TDataNodeInfo> getRegisteredDataNodes(int dataNodeId) {
    return nodeInfo.getRegisteredDataNodes(dataNodeId);
  }

  public List<TDataNodesInfo> getRegisteredDataNodesInfoList() {
    List<TDataNodesInfo> dataNodesLocations = new ArrayList<>();
    List<TDataNodeInfo> registeredDataNodes = this.getRegisteredDataNodes(-1);
    if (registeredDataNodes != null) {
      registeredDataNodes.forEach(
          (dataNodeInfo) -> {
            TDataNodesInfo tDataNodesLocation = new TDataNodesInfo();
            int dataNodeId = dataNodeInfo.getLocation().getDataNodeId();
            tDataNodesLocation.setDataNodeId(dataNodeId);
            tDataNodesLocation.setStatus(
                getLoadManager().getNodeCacheMap().get(dataNodeId).getNodeStatus().getStatus());
            tDataNodesLocation.setRpcAddresss(
                dataNodeInfo.getLocation().getClientRpcEndPoint().getIp());
            tDataNodesLocation.setRpcPort(
                dataNodeInfo.getLocation().getClientRpcEndPoint().getPort());
            tDataNodesLocation.setDataRegionNum(0);
            tDataNodesLocation.setSchemaRegionNum(0);
            dataNodesLocations.add(tDataNodesLocation);
          });
    }
    return dataNodesLocations;
  }

  /**
   * get data node remove request queue
   *
   * @return LinkedBlockingQueue
   */
  public LinkedBlockingQueue<RemoveDataNodePlan> getDataNodeRemoveRequestQueue() {
    return nodeInfo.getDataNodeRemoveRequestQueue();
  }

  /**
   * get head data node remove request
   *
   * @return RemoveDataNodeReq
   */
  public RemoveDataNodePlan getHeadRequestForDataNodeRemove() {
    return nodeInfo.getHeadRequestForDataNodeRemove();
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

  public TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    if (removeConfigNodeLock.tryLock()) {
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
              .setMessage(
                  "Remove ConfigNode failed because the ConfigNode not in current Cluster.");
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

        // Execute removePeer
        if (getConsensusManager().removeConfigNodePeer(removeConfigNodePlan)) {
          configManager
              .getLoadManager()
              .removeNodeHeartbeatHandCache(
                  removeConfigNodePlan.getConfigNodeLocation().getConfigNodeId());
          return getConsensusManager().write(removeConfigNodePlan).getStatus();
        } else {
          return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
              .setMessage(
                  "Remove ConfigNode failed because update ConsensusGroup peer information failed.");
        }
      } finally {
        removeConfigNodeLock.unlock();
      }
    } else {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
          .setMessage("A ConfigNode is removing. Please wait or try again.");
    }
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
    List<TDataNodeInfo> registeredDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes(req.dataNodeId);
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(registeredDataNodes.size()));
    CountDownLatch countDownLatch = new CountDownLatch(registeredDataNodes.size());
    Map<Integer, AbstractRetryHandler> handlerMap = new HashMap<>();
    Map<Integer, TDataNodeLocation> dataNodeLocations = new ConcurrentHashMap<>();
    AtomicInteger index = new AtomicInteger();
    for (TDataNodeInfo dataNodeInfo : registeredDataNodes) {
      handlerMap.put(
          index.get(),
          new FlushHandler(
              dataNodeInfo.getLocation(),
              countDownLatch,
              DataNodeRequestType.FLUSH,
              dataNodeResponseStatus,
              dataNodeLocations,
              index.get()));
      dataNodeLocations.put(index.getAndIncrement(), dataNodeInfo.getLocation());
    }
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(req, handlerMap, dataNodeLocations);
    return dataNodeResponseStatus;
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
