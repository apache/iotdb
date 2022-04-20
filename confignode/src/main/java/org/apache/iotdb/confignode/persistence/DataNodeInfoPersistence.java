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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.DataNodeLocationsDataSet;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
  private final ConcurrentNavigableMap<Integer, TDataNodeLocation> onlineDataNodes =
      new ConcurrentSkipListMap();

  /** For remove node or draining node */
  private final Set<TDataNodeLocation> drainingDataNodes = new HashSet<>();

  private DataNodeInfoPersistence() {
    this.dataNodeInfoReadWriteLock = new ReentrantReadWriteLock();
  }

  public boolean containsValue(TDataNodeLocation info) {
    boolean result = false;
    dataNodeInfoReadWriteLock.readLock().lock();

    try {
      for (Map.Entry<Integer, TDataNodeLocation> entry : onlineDataNodes.entrySet()) {
        info.setDataNodeId(entry.getKey());
        if (entry.getValue().equals(info)) {
          result = true;
          break;
        }
      }
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }

    return result;
  }

  public void put(int dataNodeID, TDataNodeLocation info) {
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
    TDataNodeLocation info = plan.getLocation();
    dataNodeInfoReadWriteLock.writeLock().lock();
    try {
      nextDataNodeId = Math.max(nextDataNodeId, info.getDataNodeId());
      onlineDataNodes.put(info.getDataNodeId(), info);
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
  public DataNodeLocationsDataSet getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    DataNodeLocationsDataSet result = new DataNodeLocationsDataSet();
    result.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    int dataNodeId = plan.getDataNodeID();
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      if (dataNodeId == -1) {
        result.setDataNodeLocations(new HashMap<>(onlineDataNodes));
      } else {

        result.setDataNodeLocations(
            Collections.singletonMap(dataNodeId, onlineDataNodes.get(dataNodeId)));
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

  public List<TDataNodeLocation> getOnlineDataNodes() {
    List<TDataNodeLocation> result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = new ArrayList<>(onlineDataNodes.values());
    } finally {
      dataNodeInfoReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public TDataNodeLocation getOnlineDataNode(int dataNodeId) {
    TDataNodeLocation result;
    dataNodeInfoReadWriteLock.readLock().lock();
    try {
      result = onlineDataNodes.get(dataNodeId);
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
