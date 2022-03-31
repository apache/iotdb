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
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.sys.QueryStorageGroupSchemaPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  private int nextSchemaRegionGroup = 0;
  // TODO: Serialize and Deserialize
  private int nextDataRegionGroup = 0;

  private RegionInfoPersistence regionInfoPersistence = RegionInfoPersistence.getInstance();

  private final Manager configNodeManager;

  public AssignRegionManager(Manager configNodeManager) {
    this.partitionReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeManager = configNodeManager;
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
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
      if (configNodeManager.getDataNodeManager().getDataNodeId().size() < regionReplicaCount) {
        result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        result.setMessage("DataNode is not enough, please register more.");
      } else {
        if (regionInfoPersistence.containsKey(plan.getSchema().getName())) {
          result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
          result.setMessage(
              String.format("StorageGroup %s is already set.", plan.getSchema().getName()));
        } else {
          String storageGroupName = plan.getSchema().getName();
          StorageGroupSchema storageGroupSchema = new StorageGroupSchema(storageGroupName);

          // allocate schema region
          SchemaRegionInfo schemaRegionInfo = schemaRegionAllocation(storageGroupSchema);
          plan.setSchemaRegionInfo(schemaRegionInfo);

          // allocate data region
          DataRegionInfo dataRegionInfo = dataRegionAllocation(storageGroupSchema);
          plan.setDataRegionInfo(dataRegionInfo);

          // write consensus
          result = getConsensusManager().write(plan).getStatus();
        }
      }
    } finally {
      partitionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  private DataNodeManager getDataNodeInfoManager() {
    return configNodeManager.getDataNodeManager();
  }

  private SchemaRegionInfo schemaRegionAllocation(StorageGroupSchema storageGroupSchema) {

    SchemaRegionInfo schemaRegionInfo = new SchemaRegionInfo();
    // TODO: Use CopySet algorithm to optimize region allocation policy
    for (int i = 0; i < schemaRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(getDataNodeInfoManager().getDataNodeId());
      Collections.shuffle(dataNodeList);
      schemaRegionInfo.createSchemaRegion(
          nextSchemaRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      storageGroupSchema.addSchemaRegionGroup(nextSchemaRegionGroup);
      nextSchemaRegionGroup += 1;
    }
    return schemaRegionInfo;
  }

  /**
   * TODO: Only perform in leader node, @rongzhao
   *
   * @param storageGroupSchema
   */
  private DataRegionInfo dataRegionAllocation(StorageGroupSchema storageGroupSchema) {
    // TODO: Use CopySet algorithm to optimize region allocation policy
    DataRegionInfo dataRegionInfo = new DataRegionInfo();
    for (int i = 0; i < dataRegionCount; i++) {
      List<Integer> dataNodeList = new ArrayList<>(getDataNodeInfoManager().getDataNodeId());
      Collections.shuffle(dataNodeList);
      dataRegionInfo.createDataRegion(
          nextDataRegionGroup, dataNodeList.subList(0, regionReplicaCount));
      storageGroupSchema.addDataRegionGroup(nextDataRegionGroup);
      nextDataRegionGroup += 1;
    }
    return dataRegionInfo;
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {

    ConsensusReadResponse readResponse =
        getConsensusManager().read(new QueryStorageGroupSchemaPlan());
    return (StorageGroupSchemaDataSet) readResponse.getDataset();
  }
}
