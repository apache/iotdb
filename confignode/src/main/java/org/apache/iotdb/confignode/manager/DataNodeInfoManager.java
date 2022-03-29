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

import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.partition.DataNodeInfo;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Manager server info of data node, add node or remove node */
public class DataNodeInfoManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeInfoManager.class);

  /** online data nodes */
  private final ConcurrentNavigableMap<Integer, DataNodeInfo> onlineDataNodes =
      new ConcurrentSkipListMap();

  /** For remove node or draning node */
  private Set<DataNodeInfo> drainingDataNodes = new HashSet<>();

  private Manager configNodeServices;

  /** TODO:do some operate after add node or remove node */
  private List<ChangeServerListener> listeners = new CopyOnWriteArrayList<>();

  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;

  private int nextDataNodeId;

  public DataNodeInfoManager(Manager configNodeServices) {
    this.configNodeServices = configNodeServices;
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
  }

  /**
   * register dta node info when data node start
   *
   * @param plan RegisterDataNodePlan
   * @return success if data node regist first
   */
  public TSStatus registerDataNode(RegisterDataNodePlan plan) {
    TSStatus result;
    DataNodeInfo info = plan.getInfo();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      if (onlineDataNodes.containsValue(info)) {
        result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        result.setMessage(
            String.format(
                "DataNode %s is already registered.", plan.getInfo().getEndPoint().toString()));
      } else {
        info.setDataNodeID(nextDataNodeId);
        onlineDataNodes.put(nextDataNodeId, info);
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        result.setMessage(String.valueOf(nextDataNodeId));
        nextDataNodeId++;
        LOGGER.info("Register data node success, data node is {}", plan);
      }
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * get dta node info
   *
   * @param plan QueryDataNodeInfoPlan
   * @return all data node info if dataNodeId of plan is -1
   */
  public DataNodesInfoDataSet getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    DataNodesInfoDataSet result = new DataNodesInfoDataSet();
    int dataNodeId = plan.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setInfoList(new ArrayList<>(onlineDataNodes.values()));
      } else {
        result.setInfoList(Collections.singletonList(onlineDataNodes.get(dataNodeId)));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public Set<Integer> getDataNodeId() {
    return onlineDataNodes.keySet();
  }

  /**
   * Add schema region group
   *
   * @param dataNodeId data node id
   * @param schemaRegionGroup schema region group
   */
  public void addSchemaRegionGroup(int dataNodeId, int schemaRegionGroup) {
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      if (onlineDataNodes.containsKey(dataNodeId)) {
        onlineDataNodes.get(dataNodeId).addSchemaRegionGroup(schemaRegionGroup);
      }
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  /**
   * Add data region group
   *
   * @param dataNodeId data node id
   * @param dataRegionGroup data region group
   */
  public void addDataRegionGroup(int dataNodeId, int dataRegionGroup) {
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      if (onlineDataNodes.containsKey(dataNodeId)) {
        onlineDataNodes.get(dataNodeId).addSchemaRegionGroup(dataRegionGroup);
      }
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
  }

  public ConcurrentNavigableMap<Integer, DataNodeInfo> getOnlineDataNodes() {
    return onlineDataNodes;
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
    public void addDataNode(DataNodeInfo DataNodeInfo) {
      serverChanged();
    }

    @Override
    public void removeDataNode(DataNodeInfo dataNodeInfo) {
      serverChanged();
    }

    private synchronized void serverChanged() {
      changed = true;
      this.notify();
    }

    @Override
    public void run() {
      while (!configNodeServices.isStopped()) {}
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
    void addDataNode(final DataNodeInfo dataNodeInfo);

    /**
     * remove data node
     *
     * @param dataNodeInfo data node info
     */
    void removeDataNode(final DataNodeInfo dataNodeInfo);
  }
}
