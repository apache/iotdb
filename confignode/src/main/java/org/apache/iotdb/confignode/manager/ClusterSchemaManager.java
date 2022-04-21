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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.cli.TemporaryClient;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.confignode.persistence.StorageGroupInfo;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.sys.QueryStorageGroupSchemaPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

public class ClusterSchemaManager {

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
  private static final int schemaReplicationFactor = conf.getSchemaReplicationFactor();
  private static final int dataReplicationFactor = conf.getDataReplicationFactor();
  private static final int initialSchemaRegionCount = conf.getInitialSchemaRegionCount();
  private static final int initialDataRegionCount = conf.getInitialDataRegionCount();

  private static final StorageGroupInfo storageGroupInfo = StorageGroupInfo.getInstance();
  private static final PartitionInfo partitionInfo = PartitionInfo.getInstance();

  private final Manager configManager;

  public ClusterSchemaManager(Manager configManager) {
    this.configManager = configManager;
  }

  /**
   * Set StorageGroup and allocate the default amount Regions
   *
   * @param setPlan SetStorageGroupPlan
   * @return SUCCESS_STATUS if the StorageGroup is set and region allocation successful.
   *     NOT_ENOUGH_DATA_NODE if there are not enough DataNode for Region allocation.
   *     STORAGE_GROUP_ALREADY_EXISTS if the StorageGroup is already set.
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan setPlan) {
    TSStatus result;
    if (configManager.getDataNodeManager().getOnlineDataNodeCount()
        < Math.max(initialSchemaRegionCount, initialDataRegionCount)) {
      result = new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode());
      result.setMessage("DataNode is not enough, please register more.");
    } else {
      if (storageGroupInfo.containsStorageGroup(setPlan.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode());
        result.setMessage(
            String.format("StorageGroup %s is already set.", setPlan.getSchema().getName()));
      } else {
        CreateRegionsPlan createPlan = new CreateRegionsPlan();
        createPlan.setStorageGroup(setPlan.getSchema().getName());

        // Allocate default Regions
        allocateRegions(TConsensusGroupType.SchemaRegion, createPlan, setPlan);
        allocateRegions(TConsensusGroupType.DataRegion, createPlan, setPlan);

        // Persist StorageGroup and Regions
        getConsensusManager().write(setPlan);
        result = getConsensusManager().write(createPlan).getStatus();

        // Create Regions in DataNode
        // TODO: use client pool
        for (TRegionReplicaSet regionReplicaSet : createPlan.getRegionReplicaSets()) {
          for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
            switch (regionReplicaSet.getRegionId().getType()) {
              case SchemaRegion:
                TemporaryClient.getInstance()
                    .createSchemaRegion(
                        dataNodeLocation.getDataNodeId(),
                        createPlan.getStorageGroup(),
                        regionReplicaSet);
                break;
              case DataRegion:
                TemporaryClient.getInstance()
                    .createDataRegion(
                        dataNodeLocation.getDataNodeId(),
                        createPlan.getStorageGroup(),
                        regionReplicaSet,
                        setPlan.getSchema().getTTL());
            }
          }
        }
      }
    }
    return result;
  }

  /** TODO: Allocate by LoadManager */
  private void allocateRegions(
      TConsensusGroupType type, CreateRegionsPlan createPlan, SetStorageGroupPlan setPlan) {

    // TODO: Use CopySet algorithm to optimize region allocation policy

    int replicaCount =
        type.equals(TConsensusGroupType.SchemaRegion)
            ? schemaReplicationFactor
            : dataReplicationFactor;
    int regionCount =
        type.equals(TConsensusGroupType.SchemaRegion)
            ? initialSchemaRegionCount
            : initialDataRegionCount;
    List<TDataNodeLocation> onlineDataNodes = getDataNodeInfoManager().getOnlineDataNodes();
    for (int i = 0; i < regionCount; i++) {
      Collections.shuffle(onlineDataNodes);

      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      TConsensusGroupId consensusGroupId =
          new TConsensusGroupId(type, partitionInfo.generateNextRegionGroupId());
      regionReplicaSet.setRegionId(consensusGroupId);
      regionReplicaSet.setDataNodeLocations(onlineDataNodes.subList(0, replicaCount));
      createPlan.addRegion(regionReplicaSet);

      switch (type) {
        case SchemaRegion:
          setPlan.getSchema().addToSchemaRegionGroupIds(consensusGroupId);
          break;
        case DataRegion:
          setPlan.getSchema().addToDataRegionGroupIds(consensusGroupId);
      }
    }
  }

  /**
   * Get the SchemaRegionGroupIds or DataRegionGroupIds from the specific StorageGroup
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return All SchemaRegionGroupIds when type is SchemaRegion, and all DataRegionGroupIds when
   *     type is DataRegion
   */
  public List<TConsensusGroupId> getRegionGroupIds(String storageGroup, TConsensusGroupType type) {
    return storageGroupInfo.getRegionGroupIds(storageGroup, type);
  }

  /**
   * Get all the StorageGroupSchema
   *
   * @return StorageGroupSchemaDataSet
   */
  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    ConsensusReadResponse readResponse =
        getConsensusManager().read(new QueryStorageGroupSchemaPlan());
    return (StorageGroupSchemaDataSet) readResponse.getDataset();
  }

  public List<String> getStorageGroupNames() {
    return storageGroupInfo.getStorageGroupNames();
  }

  private DataNodeManager getDataNodeInfoManager() {
    return configManager.getDataNodeManager();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
