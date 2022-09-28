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
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.CopySetRegionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyRegionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionAllocator;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;

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
   * Generate a Regions' allocation plan(CreateRegionsPlan)
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

    // The new Regions will occupy online DataNodes firstly
    List<TDataNodeConfiguration> onlineDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    // Some new Regions will have to occupy unknown DataNodes
    // if the number of online DataNodes is insufficient
    List<TDataNodeConfiguration> availableDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown);

    // Make sure the number of available DataNodes is enough for allocating new Regions
    for (String storageGroup : allotmentMap.keySet()) {
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(storageGroup, consensusGroupType);
      if (availableDataNodes.size() < replicationFactor) {
        throw new NotEnoughDataNodeException();
      }
    }

    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    IRegionAllocator regionAllocator = genRegionAllocator();
    // Only considering the specified ConsensusGroupType when doing allocation
    List<TRegionReplicaSet> allocatedRegions = getPartitionManager().getAllReplicaSets();
    allocatedRegions.removeIf(
        allocateRegion -> allocateRegion.getRegionId().getType() != consensusGroupType);

    for (Map.Entry<String, Integer> entry : allotmentMap.entrySet()) {
      String storageGroup = entry.getKey();
      int allotment = entry.getValue();
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(storageGroup, consensusGroupType);
      List<TDataNodeConfiguration> targetDataNodes =
          onlineDataNodes.size() >= replicationFactor ? onlineDataNodes : availableDataNodes;

      for (int i = 0; i < allotment; i++) {
        // Generate allocation plan
        TRegionReplicaSet newRegion =
            regionAllocator.allocateRegion(
                targetDataNodes,
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
