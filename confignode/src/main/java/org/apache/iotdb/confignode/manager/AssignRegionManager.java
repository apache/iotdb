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

import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.partition.DataRegionInfo;
import org.apache.iotdb.confignode.partition.SchemaRegionInfo;
import org.apache.iotdb.confignode.partition.SchemaRegionReplicaSet;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class AssignRegionManager {
  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
  private static final int regionReplicaCount = conf.getRegionReplicaCount();
  private static final int schemaRegionCount = conf.getSchemaRegionCount();
  private static final int dataRegionCount = conf.getDataRegionCount();

  /** partition read write lock */
  private final ReentrantReadWriteLock partitionReadWriteLock;

  // TODO: Serialize and Deserialize
  private final Map<String, StorageGroupSchema> storageGroupsMap;

  // TODO: Serialize and Deserialize
  private int nextSchemaRegionGroup = 0;
  // TODO: Serialize and Deserialize
  private int nextDataRegionGroup = 0;

  // TODO: Serialize and Deserialize
  private final SchemaRegionInfo schemaRegion;

  // TODO: Serialize and Deserialize
  private final DataRegionInfo dataRegion;

  private final Manager configNodeManager;

  public AssignRegionManager(Manager configNodeManager) {
    this.partitionReadWriteLock = new ReentrantReadWriteLock();
    this.storageGroupsMap = new HashMap<>();
    this.schemaRegion = new SchemaRegionInfo();
    this.dataRegion = new DataRegionInfo();
    this.configNodeManager = configNodeManager;
  }

  /**
   * 1. region allocation 2. add to storage group map
   *
   * @param plan SetStorageGroupPlan
   * @return TSStatusCode.SUCCESS_STATUS if region allocate
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    partitionReadWriteLock.writeLock().lock();
    try {
      if (configNodeManager.getDataNodeInfoManager().getDataNodeId().size() < regionReplicaCount) {
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
    } finally {
      partitionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  private DataNodeInfoManager getDataNodeInfoManager() {
    return configNodeManager.getDataNodeInfoManager();
  }

  /**
   * TODO: Only perform in leader node, @rongzhao
   *
   * @param schema
   */
  private void regionAllocation(StorageGroupSchema schema) {
    // TODO: Use CopySet algorithm to optimize region allocation policy
    for (int i = 0; i < schemaRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(getDataNodeInfoManager().getDataNodeId());
      Collections.shuffle(dataNodeList);
      for (int j = 0; j < regionReplicaCount; j++) {
        configNodeManager
            .getDataNodeInfoManager()
            .addSchemaRegionGroup(dataNodeList.get(j), nextSchemaRegionGroup);
      }
      schemaRegion.createSchemaRegion(
          nextSchemaRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      schema.addSchemaRegionGroup(nextSchemaRegionGroup);
      nextSchemaRegionGroup += 1;
    }
    for (int i = 0; i < dataRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(getDataNodeInfoManager().getDataNodeId());
      Collections.shuffle(dataNodeList);
      for (int j = 0; j < regionReplicaCount; j++) {
        configNodeManager
            .getDataNodeInfoManager()
            .addDataRegionGroup(dataNodeList.get(j), nextDataRegionGroup);
      }
      dataRegion.createDataRegion(nextDataRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      schema.addDataRegionGroup(nextDataRegionGroup);
      nextDataRegionGroup += 1;
    }
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    StorageGroupSchemaDataSet result = new StorageGroupSchemaDataSet();
    partitionReadWriteLock.readLock().lock();
    try {
      result.setSchemaList(new ArrayList<>(storageGroupsMap.values()));
    } finally {
      partitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** @return key is schema region id, value is endpoint list */
  public List<SchemaRegionReplicaSet> getSchemaRegionEndPoint() {
    List<SchemaRegionReplicaSet> schemaRegionEndPoints = new ArrayList<>();

    schemaRegion
        .getSchemaRegionDataNodesMap()
        .entrySet()
        .forEach(
            entity -> {
              SchemaRegionReplicaSet schemaRegionReplicaSet = new SchemaRegionReplicaSet();
              List<Endpoint> endPoints = new ArrayList<>();
              entity
                  .getValue()
                  .forEach(
                      dataNodeId -> {
                        if (getDataNodeInfoManager().getOnlineDataNodes().containsKey(dataNodeId)) {
                          endPoints.add(
                              getDataNodeInfoManager()
                                  .getOnlineDataNodes()
                                  .get(dataNodeId)
                                  .getEndPoint());
                        }
                      });
              schemaRegionReplicaSet.setSchemaRegionId(entity.getKey());
              schemaRegionReplicaSet.setEndPointList(endPoints);
              schemaRegionEndPoints.add(schemaRegionReplicaSet);
            });
    return schemaRegionEndPoints;
  }
}
