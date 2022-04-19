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
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.sys.QueryStorageGroupSchemaPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

/** manage data partition and schema partition */
public class RegionManager {

  private static final ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
  private static final int schemaReplicationFactor = conf.getDefaultSchemaReplicationFactor();
  private static final int dataReplicationFactor = conf.getDefaultDataReplicationFactor();
  private static final int initialSchemaRegionCount = conf.getInitialSchemaRegionCount();
  private static final int initialDataRegionCount = conf.getInitialDataRegionCount();

  private static final RegionInfoPersistence regionInfoPersistence =
      RegionInfoPersistence.getInstance();

  private final Manager configNodeManager;

  public RegionManager(Manager configNodeManager) {
    this.configNodeManager = configNodeManager;
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
  }

  /**
   * Set StorageGroup and allocate the default amount Regions
   *
   * @param plan SetStorageGroupPlan
   * @return SUCCESS_STATUS if the StorageGroup is set and region allocation successful.
   *     NOT_ENOUGH_DATA_NODE if there are not enough DataNode for Region allocation.
   *     STORAGE_GROUP_ALREADY_EXISTS if the StorageGroup is already set.
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    TSStatus result;
    if (configNodeManager.getDataNodeManager().getOnlineDataNodeCount() < Math.max(initialSchemaRegionCount, initialDataRegionCount)) {
      result = new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode());
      result.setMessage("DataNode is not enough, please register more.");
    } else {
      if (regionInfoPersistence.containsStorageGroup(plan.getSchema().getName())) {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode());
        result.setMessage(
            String.format("StorageGroup %s is already set.", plan.getSchema().getName()));
      } else {
        CreateRegionsPlan createPlan = new CreateRegionsPlan();
        createPlan.setStorageGroup(plan.getSchema().getName());

        // Allocate default Regions
        allocateRegions(TConsensusGroupType.SchemaRegion, createPlan);
        allocateRegions(TConsensusGroupType.DataRegion, createPlan);

        // Persist StorageGroup and Regions
        getConsensusManager().write(plan);
        result = getConsensusManager().write(createPlan).getStatus();

        // Create Regions in DataNode
        for (TRegionReplicaSet regionReplicaSet : createPlan.getRegionReplicaSets()) {
          for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
            switch (regionReplicaSet.getRegionId().getType()) {
              case SchemaRegion:
                TemporaryClient.getInstance().createSchemaRegion(dataNodeLocation.getDataNodeId(), createPlan.getStorageGroup(), regionReplicaSet);
                break;
              case DataRegion:
                TemporaryClient.getInstance().createDataRegion(dataNodeLocation.getDataNodeId(), createPlan.getStorageGroup(), regionReplicaSet, plan.getSchema().getTTL());
            }
          }
        }
      }
    }
    return result;
  }

  private DataNodeManager getDataNodeInfoManager() {
    return configNodeManager.getDataNodeManager();
  }

  private void allocateRegions(TConsensusGroupType type, CreateRegionsPlan plan) {

    // TODO: Use CopySet algorithm to optimize region allocation policy

    int replicaCount = type.equals(TConsensusGroupType.SchemaRegion) ? schemaReplicationFactor : dataReplicationFactor;
    int regionCount = type.equals(TConsensusGroupType.SchemaRegion) ? initialSchemaRegionCount : initialDataRegionCount;
    List<TDataNodeLocation> onlineDataNodes = getDataNodeInfoManager().getOnlineDataNodes();
    for (int i = 0; i < regionCount; i++) {
      Collections.shuffle(onlineDataNodes);

      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      TConsensusGroupId consensusGroupId = new TConsensusGroupId(type, regionInfoPersistence.generateNextRegionGroupId());
      regionReplicaSet.setRegionId(consensusGroupId);
      regionReplicaSet.setDataNodeLocations(onlineDataNodes.subList(0, replicaCount));
      plan.addRegion(regionReplicaSet);
    }
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    ConsensusReadResponse readResponse =
        getConsensusManager().read(new QueryStorageGroupSchemaPlan());
    return (StorageGroupSchemaDataSet) readResponse.getDataset();
  }

  public List<String> getStorageGroupNames() {
    return regionInfoPersistence.getStorageGroupNames();
  }
}
