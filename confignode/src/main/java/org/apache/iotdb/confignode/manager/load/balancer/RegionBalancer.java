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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.CopySetRegionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyRegionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionAllocator;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import java.util.List;
import java.util.Map;

/**
 * The RegionBalancer provides interfaces to generate optimal Region allocation and migration plans
 */
public class RegionBalancer {

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();

  private final IManager configManager;

  public RegionBalancer(IManager configManager) {
    this.configManager = configManager;
  }

  /**
   * Generate a Regions allocation plan(CreateRegionsPlan)
   *
   * @param allotmentMap Map<StorageGroupName, Region allotment>
   * @param consensusGroupType TConsensusGroupType of the new Regions
   * @return CreateRegionsPlan
   * @throws NotEnoughDataNodeException When the number of DataNodes is not enough for allocation
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   */
  public CreateRegionGroupsPlan genRegionsAllocationPlan(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException {
    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    IRegionAllocator regionAllocator = genRegionAllocator();

    List<TDataNodeConfiguration> onlineDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    List<TRegionReplicaSet> allocatedRegions = getPartitionManager().getAllReplicaSets();
    allocatedRegions.removeIf(
        allocateRegion -> allocateRegion.getRegionId().getType() != consensusGroupType);

    for (Map.Entry<String, Integer> entry : allotmentMap.entrySet()) {
      String storageGroup = entry.getKey();
      int allotment = entry.getValue();

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

      for (int i = 0; i < allotment; i++) {
        // Generate allocation plan
        TRegionReplicaSet newRegion =
            regionAllocator.allocateRegion(
                onlineDataNodes,
                allocatedRegions,
                replicationFactor,
                new TConsensusGroupId(
                    consensusGroupType, getPartitionManager().generateNextRegionGroupId()));
        createRegionGroupsPlan.addRegionGroup(storageGroup, newRegion);

        allocatedRegions.add(newRegion);
      }
    }

    return createRegionGroupsPlan;
  }

  private IRegionAllocator genRegionAllocator() {
    RegionBalancer.RegionAllocateStrategy regionAllocateStrategy =
        CONFIG_NODE_CONFIG.getRegionAllocateStrategy();
    if (regionAllocateStrategy == null) {
      return new GreedyRegionAllocator();
    }
    switch (regionAllocateStrategy) {
      case COPY_SET:
        return new CopySetRegionAllocator();
      default:
        return new GreedyRegionAllocator();
    }
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

  /** region allocate strategy */
  public enum RegionAllocateStrategy {
    COPY_SET,
    GREEDY
  }
}
