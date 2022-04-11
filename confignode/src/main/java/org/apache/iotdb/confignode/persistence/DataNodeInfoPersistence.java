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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeInfoPersistence {

  private final ReentrantReadWriteLock dataNodeInfoReadWriteLock;

  // TODO: serialize and deserialize
  private int nextDataNodeId = 0;

  /** online data nodes */
  // TODO: serialize and deserialize
  private final ConcurrentNavigableMap<Integer, DataNodeLocation> onlineDataNodes =
      new ConcurrentSkipListMap();

  /** For remove node or draining node */
  private final Set<DataNodeLocation> drainingDataNodes = new HashSet<>();

  private DataNodeInfoPersistence() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
  }

  public boolean containsValue(DataNodeLocation info) {
    boolean result = false;
    dataNodeInfoReadWriteLock.readLock().lock();

    try {
      for (Map.Entry<Integer, DataNodeLocation> entry : onlineDataNodes.entrySet()) {
        if (entry.getValue().getEndPoint().equals(info.getEndPoint())) {
          result = true;
          info.setDataNodeID(entry.getKey());
          break;
        }
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public void put(int dataNodeID, DataNodeLocation info) {
    onlineDataNodes.put(dataNodeID, info);
  }

  /**
   * Persist DataNode info
   *
   * @param plan RegisterDataNodePlan
   * @return SUCCESS_STATUS
   */
  public TSStatus registerDataNode(RegisterDataNodePlan plan) {
    TSStatus result;
    DataNodeLocation info = plan.getInfo();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      nextDataNodeId = Math.max(nextDataNodeId, info.getDataNodeID());
      onlineDataNodes.put(info.getDataNodeID(), info);
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Get DataNode info
   *
   * @param plan QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodesInfoDataSet getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    DataNodesInfoDataSet result = new DataNodesInfoDataSet();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = plan.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeList(new ArrayList<>(onlineDataNodes.values()));
      } else {
        result.setDataNodeList(Collections.singletonList(onlineDataNodes.get(dataNodeId)));
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public int getOnlineDataNodeCount() {
    int result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = onlineDataNodes.size();
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public List<DataNodeLocation> getOnlineDataNodes() {
    List<DataNodeLocation> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(onlineDataNodes.values());
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public int generateNextDataNodeId() {
    int result;

    try {
      dataNodeInfoReadWriteLock.writeLock().lock();
      result = nextDataNodeId;
      nextDataNodeId += 1;
    } finally {
      dataNodeInfoReadWriteLock.writeLock().unlock();
    }

    return result;
  }

  @TestOnly
  public void clear() {
    nextDataNodeId = 0;
    onlineDataNodes.clear();
    drainingDataNodes.clear();
  }

  private static class DataNodeInfoPersistenceHolder {

    private static final DataNodeInfoPersistence INSTANCE = new DataNodeInfoPersistence();

    private DataNodeInfoPersistenceHolder() {
      // empty constructor
    }
  }

  public static DataNodeInfoPersistence getInstance() {
    return DataNodeInfoPersistence.DataNodeInfoPersistenceHolder.INSTANCE;
  }
}
