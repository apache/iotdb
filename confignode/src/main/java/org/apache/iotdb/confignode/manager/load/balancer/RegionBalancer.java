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
package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsReq;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.Manager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.CopySetRegionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionAllocator;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import java.util.List;

/**
 * The RegionBalancer provides interfaces to generate optimal Region allocation and migration plans
 */
public class RegionBalancer {

  private final Manager configManager;

  public RegionBalancer(Manager configManager) {
    this.configManager = configManager;
  }

  /**
   * Generate a Regions allocation plan(CreateRegionsReq)
   *
   * @param storageGroups List<StorageGroup>
   * @param consensusGroupType TConsensusGroupType of the new Regions
   * @param regionNum Number of Regions to be allocated per StorageGroup
   * @return CreateRegionsReq
   * @throws NotEnoughDataNodeException When the number of DataNodes is not enough for allocation
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   */
  public CreateRegionGroupsReq genRegionsAllocationPlan(
      List<String> storageGroups, TConsensusGroupType consensusGroupType, int regionNum)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException {
    CreateRegionGroupsReq createRegionGroupsReq = new CreateRegionGroupsReq();
    IRegionAllocator regionAllocator = genRegionAllocator();

    List<TDataNodeInfo> onlineDataNodes = getNodeManager().getOnlineDataNodes(-1);
    List<TRegionReplicaSet> allocatedRegions = getPartitionManager().getAllReplicaSets();

    for (String storageGroup : storageGroups) {
      // Get schema
      TStorageGroupSchema storageGroupSchema =
          getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup);
      int replicationFactor =
          consensusGroupType == TConsensusGroupType.SchemaRegion
              ? storageGroupSchema.getSchemaReplicationFactor()
              : storageGroupSchema.getDataReplicationFactor();

      // Check validity
      if (onlineDataNodes.size() < replicationFactor) {
        throw new NotEnoughDataNodeException();
      }

      for (int i = 0; i < regionNum; i++) {
        // Generate allocation plan
        TRegionReplicaSet newRegion =
            regionAllocator.allocateRegion(
                onlineDataNodes,
                allocatedRegions,
                replicationFactor,
                new TConsensusGroupId(
                    consensusGroupType, getPartitionManager().generateNextRegionGroupId()));
        createRegionGroupsReq.addRegion(storageGroup, newRegion);

        allocatedRegions.add(newRegion);
      }
    }

    return createRegionGroupsReq;
  }

  private IRegionAllocator genRegionAllocator() {
    // TODO: The RegionAllocator should be configurable
    return new CopySetRegionAllocator();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
