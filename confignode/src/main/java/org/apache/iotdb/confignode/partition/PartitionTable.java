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
package org.apache.iotdb.confignode.partition;

import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PartitionTable stores schema partition table, data partition table, DataNode information,
 * StorageGroup schema and real-time write load allocation rules. The PartitionTable is thread-safe.
 */
public class PartitionTable {

  private final ReentrantReadWriteLock storageGroupLock;
  private final Map<String, StorageGroupSchema> storageGroupsMap;

  private final ReentrantReadWriteLock dataNodeLock;
  private final Map<Integer, DataNodeInfo> dataNodesMap; // Map<DataNodeID, DataNodeInfo>

  private final ReentrantReadWriteLock schemaLock;
  private final SchemaPartitionInfo schemaPartition;

  private final ReentrantReadWriteLock dataLock;
  private final DataPartitionInfo dataPartition;

  private PartitionTable() {
    this.storageGroupLock = new ReentrantReadWriteLock();
    this.storageGroupsMap = new HashMap<>();

    this.dataNodeLock = new ReentrantReadWriteLock();
    this.dataNodesMap = new HashMap<>();

    this.schemaLock = new ReentrantReadWriteLock();
    this.schemaPartition = new SchemaPartitionInfo();

    this.dataLock = new ReentrantReadWriteLock();
    this.dataPartition = new DataPartitionInfo();
  }

  public TSStatus registerDataNode(RegisterDataNodePlan plan) {
    dataNodeLock.writeLock().lock();
    if (dataNodesMap.containsValue(plan.getInfo())) {
      dataNodeLock.writeLock().unlock();
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    dataNodesMap.put(plan.getInfo().getDataNodeID(), plan.getInfo());
    dataNodeLock.writeLock().unlock();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public Map<Integer, DataNodeInfo> getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    Map<Integer, DataNodeInfo> result = new HashMap<>();
    dataNodeLock.readLock().lock();
    switch (plan.getDataNodeID()) {
      case Integer.MIN_VALUE:
        int minKey = Integer.MAX_VALUE;
        for (Integer key : dataNodesMap.keySet()) {
          minKey = Math.min(key, minKey);
        }
        if (minKey < Integer.MAX_VALUE) {
          result.put(minKey, dataNodesMap.get(minKey));
        } else {
          result = null;
        }
        break;
      case Integer.MAX_VALUE:
        int maxKey = Integer.MIN_VALUE;
        for (Integer key : dataNodesMap.keySet()) {
          maxKey = Math.max(key, maxKey);
        }
        if (maxKey > Integer.MIN_VALUE) {
          result.put(maxKey, dataNodesMap.get(maxKey));
        } else {
          result = null;
        }
        break;
      case -1:
        result.putAll(dataNodesMap);
        break;
      default:
        if (dataNodesMap.containsKey(plan.getDataNodeID())) {
          result.put(plan.getDataNodeID(), dataNodesMap.get(plan.getDataNodeID()));
        } else {
          result = null;
        }
    }
    dataNodeLock.readLock().unlock();
    return result;
  }

  private static class PartitionTableHolder {

    private static final PartitionTable INSTANCE = new PartitionTable();

    private PartitionTableHolder() {
      // empty constructor
    }
  }

  public static PartitionTable getInstance() {
    return PartitionTable.PartitionTableHolder.INSTANCE;
  }
}
