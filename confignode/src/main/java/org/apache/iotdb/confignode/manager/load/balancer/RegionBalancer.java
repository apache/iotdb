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
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.CopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The RegionBalancer provides interfaces to generate optimal Region allocation and migration plans
 */
public class RegionBalancer {

  private final IManager configManager;
  private final IRegionGroupAllocator regionGroupAllocator;

  public RegionBalancer(IManager configManager) {
    this.configManager = configManager;

    switch (ConfigNodeDescriptor.getInstance().getConf().getRegionGroupAllocatePolicy()) {
      case COPY_SET:
        this.regionGroupAllocator = new CopySetRegionGroupAllocator();
        break;
      case GREEDY:
      default:
        this.regionGroupAllocator = new GreedyRegionGroupAllocator();
    }
  }

  /**
   * Generate a RegionGroups' allocation plan(CreateRegionGroupsPlan)
   *
   * @param allotmentMap Map<StorageGroupName, RegionGroup allotment>
   * @param consensusGroupType TConsensusGroupType of the new RegionGroups
   * @return CreateRegionGroupsPlan
   * @throws NotEnoughDataNodeException When the number of DataNodes is not enough for allocation
   * @throws DatabaseNotExistsException When some StorageGroups don't exist
   */
  public CreateRegionGroupsPlan genRegionGroupsAllocationPlan(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, DatabaseNotExistsException {

    // The new RegionGroups will occupy online DataNodes firstly
    List<TDataNodeConfiguration> onlineDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    // Some new RegionGroups will have to occupy unknown DataNodes
    // if the number of online DataNodes is insufficient
    List<TDataNodeConfiguration> availableDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown);

    // Make sure the number of available DataNodes is enough for allocating new RegionGroups
    for (String storageGroup : allotmentMap.keySet()) {
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(storageGroup, consensusGroupType);
      if (availableDataNodes.size() < replicationFactor) {
        throw new NotEnoughDataNodeException();
      }
    }

    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    // Only considering the specified ConsensusGroupType when doing allocation
    List<TRegionReplicaSet> allocatedRegionGroups =
        getPartitionManager().getAllReplicaSets(consensusGroupType);

    for (Map.Entry<String, Integer> entry : allotmentMap.entrySet()) {
      String storageGroup = entry.getKey();
      int allotment = entry.getValue();
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(storageGroup, consensusGroupType);
      List<TDataNodeConfiguration> targetDataNodes =
          onlineDataNodes.size() >= replicationFactor ? onlineDataNodes : availableDataNodes;

      for (int i = 0; i < allotment; i++) {
        // Prepare input data
        Map<Integer, TDataNodeConfiguration> availableDataNodeMap = new ConcurrentHashMap<>();
        Map<Integer, Double> freeDiskSpaceMap = new ConcurrentHashMap<>();
        targetDataNodes.forEach(
            dataNodeConfiguration -> {
              int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
              availableDataNodeMap.put(dataNodeId, dataNodeConfiguration);
              freeDiskSpaceMap.put(dataNodeId, getNodeManager().getFreeDiskSpace(dataNodeId));
            });

        // Generate allocation plan
        TRegionReplicaSet newRegionGroup =
            regionGroupAllocator.generateOptimalRegionReplicasDistribution(
                availableDataNodeMap,
                freeDiskSpaceMap,
                allocatedRegionGroups,
                replicationFactor,
                new TConsensusGroupId(
                    consensusGroupType, getPartitionManager().generateNextRegionGroupId()));
        createRegionGroupsPlan.addRegionGroup(storageGroup, newRegionGroup);

        // Mark the new RegionGroup as allocated
        allocatedRegionGroups.add(newRegionGroup);
      }
    }

    return createRegionGroupsPlan;
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

  public enum RegionGroupAllocatePolicy {
    COPY_SET,
    GREEDY
  }
}
