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

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.persistence.DataNodeInfoPersistence;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Manager server info of data node, add node or remove node */
public class DataNodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeManager.class);

  private static final DataNodeInfoPersistence dataNodeInfoPersistence =
      DataNodeInfoPersistence.getInstance();

  private final Manager configManager;

  /** TODO:do some operate after add node or remove node */
  private final List<ChangeServerListener> listeners = new CopyOnWriteArrayList<>();

  public DataNodeManager(Manager configManager) {
    this.configManager = configManager;
  }

  /**
   * Register DataNode
   *
   * @param plan RegisterDataNodePlan
   * @return SUCCESS_STATUS if DataNode first register, and DATANODE_ALREADY_REGISTERED if DataNode
   *     is already registered
   */
  public TSStatus registerDataNode(RegisterDataNodePlan plan) {
    TSStatus result;
    DataNodeLocation info = plan.getInfo();

    if (dataNodeInfoPersistence.containsValue(info)) {
      result = new TSStatus(TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode());
      DataNodeInfoPersistence.setRegisterDataNodeMessages(result, info.getDataNodeID());
      return result;
    } else {
      info.setDataNodeID(dataNodeInfoPersistence.generateNextDataNodeId());
      ConsensusWriteResponse consensusWriteResponse = getConsensusManager().write(plan);
      return consensusWriteResponse.getStatus();
    }
  }

  /**
   * Get DataNode info
   *
   * @param plan QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodesInfoDataSet getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    return (DataNodesInfoDataSet) getConsensusManager().read(plan).getDataset();
  }

  public int getOnlineDataNodeCount() {
    return dataNodeInfoPersistence.getOnlineDataNodeCount();
  }

  public List<DataNodeLocation> getOnlineDataNodes() {
    return dataNodeInfoPersistence.getOnlineDataNodes();
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
    public void addDataNode(DataNodeLocation DataNodeInfo) {
      serverChanged();
    }

    @Override
    public void removeDataNode(DataNodeLocation dataNodeInfo) {
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
    void addDataNode(final DataNodeLocation dataNodeInfo);

    /**
     * remove data node
     *
     * @param dataNodeInfo data node info
     */
    void removeDataNode(final DataNodeLocation dataNodeInfo);
  }
}
