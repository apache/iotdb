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
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.FlushHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** NodeManager manages cluster node addition and removal requests */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private final Manager configManager;
  private final NodeInfo nodeInfo;

  /** TODO:do some operate after add node or remove node */
  private final List<ChangeServerListener> listeners = new CopyOnWriteArrayList<>();

  public NodeManager(Manager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
  }

  private void setGlobalConfig(DataNodeConfigurationResp dataSet) {
    // Set TGlobalConfig
    TGlobalConfig globalConfig = new TGlobalConfig();
    globalConfig.setDataRegionConsensusProtocolClass(
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass());
    globalConfig.setSchemaRegionConsensusProtocolClass(
        ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionConsensusProtocolClass());
    globalConfig.setSeriesPartitionSlotNum(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    globalConfig.setSeriesPartitionExecutorClass(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass());
    globalConfig.setTimePartitionInterval(
        ConfigNodeDescriptor.getInstance().getConf().getTimePartitionInterval());
    dataSet.setGlobalConfig(globalConfig);
  }

  /**
   * Register DataNode
   *
   * @param req RegisterDataNodeReq
   * @return DataNodeConfigurationDataSet. The TSStatus will be set to SUCCESS_STATUS when register
   *     success, and DATANODE_ALREADY_REGISTERED when the DataNode is already exist.
   */
  public DataSet registerDataNode(RegisterDataNodeReq req) {
    DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();

    if (nodeInfo.isOnlineDataNode(req.getInfo().getLocation())) {
      // Reset client
      AsyncDataNodeClientPool.getInstance()
          .resetClient(req.getInfo().getLocation().getInternalEndPoint());

      TSStatus status = new TSStatus(TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode());
      status.setMessage("DataNode already registered.");
      dataSet.setStatus(status);
    } else {
      // Persist DataNodeInfo
      req.getInfo().getLocation().setDataNodeId(nodeInfo.generateNextNodeId());
      ConsensusWriteResponse resp = getConsensusManager().write(req);
      dataSet.setStatus(resp.getStatus());
    }

    dataSet.setDataNodeId(req.getInfo().getLocation().getDataNodeId());
    dataSet.setConfigNodeList(nodeInfo.getOnlineConfigNodes());
    setGlobalConfig(dataSet);
    return dataSet;
  }

  /**
   * Get DataNode info
   *
   * @param req QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeInfosResp getDataNodeInfo(GetDataNodeInfoReq req) {
    return (DataNodeInfosResp) getConsensusManager().read(req).getDataset();
  }

  /**
   * Only leader use this interface
   *
   * @return The number of online DataNodes
   */
  public int getOnlineDataNodeCount() {
    return nodeInfo.getOnlineDataNodeCount();
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
   * @return All online DataNodes if dataNodeId equals -1. And return the specific DataNode
   *     otherwise.
   */
  public List<TDataNodeInfo> getOnlineDataNodes(int dataNodeId) {
    return nodeInfo.getOnlineDataNodes(dataNodeId);
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

    // Return online ConfigNodes
    resp.setConfigNodeList(nodeInfo.getOnlineConfigNodes());
    resp.getConfigNodeList().add(req.getConfigNodeLocation());

    return resp;
  }

  public TSStatus applyConfigNode(ApplyConfigNodeReq applyConfigNodeReq) {
    if (getConsensusManager().addConfigNodePeer(applyConfigNodeReq)) {
      // Generate new ConfigNode's index
      applyConfigNodeReq.getConfigNodeLocation().setConfigNodeId(nodeInfo.generateNextNodeId());
      return getConsensusManager().write(applyConfigNodeReq).getStatus();
    } else {
      return new TSStatus(TSStatusCode.APPLY_CONFIGNODE_FAILED.getStatusCode())
          .setMessage("Apply ConfigNode failed because there is another ConfigNode being applied.");
    }
  }

  public void addMetrics() {
    nodeInfo.addMetrics();
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    return nodeInfo.getOnlineConfigNodes();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
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
    List<TDataNodeInfo> onlineDataNodes =
        configManager.getNodeManager().getOnlineDataNodes(req.dataNodeId);
    List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(onlineDataNodes.size()));
    CountDownLatch countDownLatch = new CountDownLatch(onlineDataNodes.size());
    for (TDataNodeInfo dataNodeInfo : onlineDataNodes) {
      AsyncDataNodeClientPool.getInstance()
          .flush(
              dataNodeInfo.getLocation().getInternalEndPoint(),
              req,
              new FlushHandler(dataNodeInfo.getLocation(), countDownLatch, dataNodeResponseStatus));
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("NodeManager was interrupted during flushing on data nodes", e);
    }
    return dataNodeResponseStatus;
  }
}
