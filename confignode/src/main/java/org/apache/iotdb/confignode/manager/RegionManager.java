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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
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
  private static final int regionReplicaCount = conf.getRegionReplicaCount();
  private static final int schemaRegionCount = conf.getSchemaRegionCount();
  private static final int dataRegionCount = conf.getDataRegionCount();

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
    if (configNodeManager.getDataNodeManager().getOnlineDataNodeCount() < regionReplicaCount) {
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

        // allocate schema region
        allocateRegions(GroupType.SchemaRegion, createPlan);
        // allocate data region
        allocateRegions(GroupType.DataRegion, createPlan);

        // set StorageGroup
        getConsensusManager().write(plan);

        // create Region
        // TODO: Send create Region to DataNode
        result = getConsensusManager().write(createPlan).getStatus();
      }
    }
    return result;
  }

  private DataNodeManager getDataNodeInfoManager() {
    return configNodeManager.getDataNodeManager();
  }

  private void allocateRegions(GroupType type, CreateRegionsPlan plan) {

    // TODO: Use CopySet algorithm to optimize region allocation policy

    int regionCount = type.equals(GroupType.SchemaRegion) ? schemaRegionCount : dataRegionCount;
    List<DataNodeLocation> onlineDataNodes = getDataNodeInfoManager().getOnlineDataNodes();
    for (int i = 0; i < regionCount; i++) {
      Collections.shuffle(onlineDataNodes);

      RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
      ConsensusGroupId consensusGroupId = null;
      switch (type) {
        case SchemaRegion:
          consensusGroupId = new SchemaRegionId(regionInfoPersistence.generateNextRegionGroupId());
          break;
        case DataRegion:
          consensusGroupId = new DataRegionId(regionInfoPersistence.generateNextRegionGroupId());
      }
      regionReplicaSet.setConsensusGroupId(consensusGroupId);
      regionReplicaSet.setDataNodeList(onlineDataNodes.subList(0, regionReplicaCount));
      plan.addRegion(regionReplicaSet);
    }
  }

  public StorageGroupSchemaDataSet getStorageGroupSchema() {
    ConsensusReadResponse readResponse =
        getConsensusManager().read(new QueryStorageGroupSchemaPlan());
    return (StorageGroupSchemaDataSet) readResponse.getDataset();
  }
}
