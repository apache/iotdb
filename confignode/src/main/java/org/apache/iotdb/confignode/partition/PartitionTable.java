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

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PartitionTable stores schema partition table, data partition table, DataNode information,
 * StorageGroup schema and real-time write load allocation rules. The PartitionTable is thread-safe.
 */
public class PartitionTable {

  private static final int regionReplicaCount =
      ConfigNodeDescriptor.getInstance().getConf().getRegionReplicaCount();
  private static final int schemaRegionCount =
      ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionCount();
  private static final int dataRegionCount =
      ConfigNodeDescriptor.getInstance().getConf().getDataRegionCount();

  private final ReentrantReadWriteLock storageGroupLock;
  // TODO: Serialize and Deserialize
  private final Map<String, StorageGroupSchema> storageGroupsMap;

  private final ReentrantReadWriteLock dataNodeLock;
  // TODO: Serialize and Deserialize
  private int nextDataNode = 0;
  // TODO: Serialize and Deserialize
  private int nextSchemaRegionGroup = 0;
  // TODO: Serialize and Deserialize
  private int nextDataRegionGroup = 0;
  // TODO: Serialize and Deserialize
  private final Map<Integer, DataNodeInfo> dataNodesMap; // Map<DataNodeID, DataNodeInfo>

  private final ReentrantReadWriteLock schemaLock;
  // TODO: Serialize and Deserialize
  private final SchemaPartitionInfo schemaPartition;

  private final ReentrantReadWriteLock dataLock;
  // TODO: Serialize and Deserialize
  private final DataPartitionInfo dataPartition;

  public PartitionTable() {
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
    TSStatus result;
    DataNodeInfo info = plan.getInfo();
    dataNodeLock.writeLock().lock();

    if (dataNodesMap.containsValue(info)) {
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      result.setMessage(
          String.format(
              "DataNode %s is already registered.", plan.getInfo().getEndPoint().toString()));
    } else {
      info.setDataNodeID(nextDataNode);
      dataNodesMap.put(info.getDataNodeID(), info);
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      result.setMessage(String.valueOf(nextDataNode));
      nextDataNode += 1;
    }

    dataNodeLock.writeLock().unlock();
    return result;
  }

  public Map<Integer, DataNodeInfo> getDataNodeInfo(QueryDataNodeInfoPlan plan) {
    Map<Integer, DataNodeInfo> result = new HashMap<>();
    dataNodeLock.readLock().lock();

    if (plan.getDataNodeID() == -1) {
      result.putAll(dataNodesMap);
    } else {
      if (dataNodesMap.containsKey(plan.getDataNodeID())) {
        result.put(plan.getDataNodeID(), dataNodesMap.get(plan.getDataNodeID()));
      } else {
        result = null;
      }
    }

    dataNodeLock.readLock().unlock();
    return result;
  }

  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    storageGroupLock.writeLock().lock();

    if (dataNodesMap.size() < regionReplicaCount) {
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      result.setMessage("DataNode is not enough, please register more.");
    } else {
      if (storageGroupsMap.containsKey(plan.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        result.setMessage(
            String.format("StorageGroup %s is already set.", plan.getSchema().getName()));
      } else {
        StorageGroupSchema schema = new StorageGroupSchema(plan.getSchema().getName());
        regionAllocation(schema);
        storageGroupsMap.put(schema.getName(), schema);
        result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
    }

    storageGroupLock.writeLock().unlock();
    return result;
  }

  private void regionAllocation(StorageGroupSchema schema) {
    // TODO: 2PL may cause deadlock, remember to optimize
    dataNodeLock.writeLock().lock();
    schemaLock.writeLock().lock();
    dataLock.writeLock().lock();

    // TODO: Use CopySet algorithm to optimize region allocation policy
    for (int i = 0; i < schemaRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(dataNodesMap.keySet());
      Collections.shuffle(dataNodeList);
      for (int j = 0; j < regionReplicaCount; j++) {
        dataNodesMap.get(dataNodeList.get(j)).addSchemaRegionGroup(nextSchemaRegionGroup);
      }
      schemaPartition.createSchemaRegion(
          nextSchemaRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      schema.addSchemaRegionGroup(nextSchemaRegionGroup);
      nextSchemaRegionGroup += 1;
    }
    for (int i = 0; i < dataRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(dataNodesMap.keySet());
      Collections.shuffle(dataNodeList);
      for (int j = 0; j < regionReplicaCount; j++) {
        dataNodesMap.get(dataNodeList.get(j)).addDataRegionGroup(nextDataRegionGroup);
      }
      dataPartition.createDataRegion(
          nextDataRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      schema.addDataRegionGroup(nextDataRegionGroup);
      nextDataRegionGroup += 1;
    }

    dataLock.writeLock().unlock();
    schemaLock.writeLock().unlock();
    dataNodeLock.writeLock().lock();
  }

  public List<StorageGroupSchema> getStorageGroupSchema() {
    return new ArrayList<>(storageGroupsMap.values());
  }
}
