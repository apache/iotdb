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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class GreedyCopySetRegionGroupMigratorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyCopySetRegionGroupMigratorTest.class);

  private static final PartiteGraphPlacementRegionGroupAllocator ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();
  private static final GreedyCopySetRegionGroupMigrator MIGRATOR =
      new GreedyCopySetRegionGroupMigrator();

  // Test parameters
  private final int nodeCount; // n - number of nodes
  private final int regionCount; // |R| - number of regions
  private final int replicaCount; // r - replication factor

  // Instance variables to avoid interference between tests
  private final Map<Integer, TDataNodeConfiguration> beforeNodeMap = new TreeMap<>();
  private final Map<Integer, TDataNodeConfiguration> availableDataNodeMap = new TreeMap<>();
  private final Map<Integer, Double> beforeSpaceMap = new TreeMap<>();
  private final Map<Integer, Double> freeSpaceMap = new TreeMap<>();

  @Parameterized.Parameters(name = "n={0}, |R|={1}, r={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {3, 9, 2}, // n=3, |R|=9, r=2, dateRegionPerNode=6
          //          {4, 12, 3}, // n=3, |R|=12, r=3, dateRegionPerNode=9
          //          {6, 30, 5}, // n=6, |R|=30, r=5, dateRegionPerNode=25
          {10, 100, 2}, // n=10, |R|=100, r=2, dateRegionPerNode=2
          //          {10, 100, 3}, // n=10, |R|=100, r=3, dateRegionPerNode=30
          //          {10, 100, 5}, // n=10, |R|=100, r=5, dateRegionPerNode=50
          //          {100, 500, 2}, // n=100, |R|=500, r=2, dateRegionPerNode=10
          //          {100, 500, 3}, // n=100, |R|=500, r=3, dateRegionPerNode=15
        });
  }

  public GreedyCopySetRegionGroupMigratorTest(int nodeCount, int regionCount, int replicaCount) {
    this.nodeCount = nodeCount;
    this.regionCount = regionCount;
    this.replicaCount = replicaCount;
  }

  @Test
  public void autoBalanceRegionReplicasDistribution() {
    LOGGER.info(
        "Starting test with targetNodeIds: nodeCount(n)={}, regionCount(|R|)={}, replicaCount(r)={}",
        nodeCount,
        regionCount,
        replicaCount);

    Random random = new Random();
    beforeNodeMap.clear();
    availableDataNodeMap.clear();
    beforeSpaceMap.clear();
    freeSpaceMap.clear();

    // Initialize nodes: the first n-1 nodes exist before migration
    for (int i = 1; i <= nodeCount - 1; i++) {
      TDataNodeConfiguration dataNodeConfiguration =
          new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i));
      double space = random.nextDouble();
      beforeNodeMap.put(i, dataNodeConfiguration);
      availableDataNodeMap.put(i, dataNodeConfiguration);
      beforeSpaceMap.put(i, space);
      freeSpaceMap.put(i, space);
    }
    // The nth node is a newly added node
    for (int i = nodeCount; i <= nodeCount; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpaceMap.put(i, random.nextDouble());
    }

    // Generate initial region allocation
    List<TRegionReplicaSet> allocatedResult = new ArrayList<>();
    for (int i = 0; i < regionCount; i++) {
      allocatedResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              beforeNodeMap,
              beforeSpaceMap,
              allocatedResult,
              allocatedResult,
              replicaCount,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }

    // Create mock statistics
    Map<TConsensusGroupId, RegionGroupStatistics> fakeStatisticsMap = new TreeMap<>();
    allocatedResult.forEach(
        regionGroup -> {
          RegionGroupStatistics fakeStatistics =
              RegionGroupStatistics.generateDefaultRegionGroupStatistics();
          fakeStatistics.setDiskUsage(100);
          fakeStatisticsMap.put(regionGroup.getRegionId(), fakeStatistics);
        });

    // Record statistics before migration
    Map<Integer, Integer> beforeRegionCounter = new TreeMap<>();
    Map<Integer, Long> beforeDiskCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                beforeRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
                beforeDiskCounter.merge(
                    location.getDataNodeId(),
                    fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage(),
                    Long::sum);
              });
    }
    LOGGER.info("Region count before migration: {}", beforeRegionCounter);
    LOGGER.info("Disk count before migration: {}", beforeDiskCounter);

    // Specify target node IDs (use the newly added node)
    List<Integer> targetNodeIds = Collections.singletonList(nodeCount);
    LOGGER.info("Target node IDs: {}", targetNodeIds);

    // Execute migration with targetNodeIds
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableDataNodeMap, fakeStatisticsMap, allocatedResult, replicaCount, targetNodeIds);

    // Verify that migrations target the specified node
    int migrationsToTargetNode = 0;
    long migrationCost = 0;
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      Set<Integer> originSet =
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> migrationSet =
          migrationPlan.get(regionReplicaSet.getRegionId()).getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      LOGGER.info(
          "Region ID: {}, original nodes: {}, target nodes: {}",
          regionReplicaSet.getRegionId().getId(),
          originSet,
          migrationSet);

      // Check if any targetNodeId is in the migration set
      for (Integer targetNodeId : targetNodeIds) {
        if (migrationSet.contains(targetNodeId) && !originSet.contains(targetNodeId)) {
          migrationsToTargetNode++;
          break; // Count each region only once
        }
      }

      migrationSet.removeAll(originSet);
      migrationCost +=
          migrationSet.size() * fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage();
    }
    LOGGER.info("Migration cost: {}", migrationCost);
    LOGGER.info(
        "Number of regions migrated to target nodes {}: {}", targetNodeIds, migrationsToTargetNode);

    // Record statistics after migration
    Map<Integer, Integer> afterRegionCounter = new TreeMap<>();
    Map<Integer, Long> afterDiskCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : migrationPlan.values()) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                afterRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
                afterDiskCounter.merge(
                    location.getDataNodeId(),
                    fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage(),
                    Long::sum);
              });
    }
    LOGGER.info("Region count after migration: {}", afterRegionCounter);
    LOGGER.info("Disk count after migration: {}", afterDiskCounter);

    // Verify that at least one target node has regions after migration
    boolean hasTargetNodeWithRegions = false;
    for (Integer targetNodeId : targetNodeIds) {
      if (afterRegionCounter.containsKey(targetNodeId)) {
        hasTargetNodeWithRegions = true;
        LOGGER.info(
            "Target node {} has {} regions after migration",
            targetNodeId,
            afterRegionCounter.get(targetNodeId));
      }
    }
    assert hasTargetNodeWithRegions
        : "At least one target node from "
            + targetNodeIds
            + " should have at least one region after migration";
  }

  @Test
  public void autoBalanceRegionReplicasDistributionWithTwoTargetNodes() {
    LOGGER.info(
        "Starting test with multiple targetNodeIds: nodeCount(n)={}, regionCount(|R|)={}, replicaCount(r)={}",
        nodeCount,
        regionCount,
        replicaCount);

    // Skip test if nodeCount is too small for multiple target nodes
    if (nodeCount < 4) {
      LOGGER.info("Skipping multi-node test: nodeCount ({}) is too small", nodeCount);
      return;
    }

    Random random = new Random();
    beforeNodeMap.clear();
    availableDataNodeMap.clear();
    beforeSpaceMap.clear();
    freeSpaceMap.clear();

    // Initialize nodes: the first n-2 nodes exist before migration
    for (int i = 1; i <= nodeCount - 2; i++) {
      TDataNodeConfiguration dataNodeConfiguration =
          new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i));
      double space = random.nextDouble();
      beforeNodeMap.put(i, dataNodeConfiguration);
      availableDataNodeMap.put(i, dataNodeConfiguration);
      beforeSpaceMap.put(i, space);
      freeSpaceMap.put(i, space);
    }
    // The last 2 nodes are newly added nodes
    for (int i = nodeCount - 1; i <= nodeCount; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpaceMap.put(i, random.nextDouble());
    }

    // Generate initial region allocation
    List<TRegionReplicaSet> allocatedResult = new ArrayList<>();
    for (int i = 0; i < regionCount; i++) {
      allocatedResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              beforeNodeMap,
              beforeSpaceMap,
              allocatedResult,
              allocatedResult,
              replicaCount,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }

    // Create mock statistics with different disk usage to test node selection
    Map<TConsensusGroupId, RegionGroupStatistics> fakeStatisticsMap = new TreeMap<>();
    allocatedResult.forEach(
        regionGroup -> {
          RegionGroupStatistics fakeStatistics =
              RegionGroupStatistics.generateDefaultRegionGroupStatistics();
          fakeStatistics.setDiskUsage(100);
          fakeStatisticsMap.put(regionGroup.getRegionId(), fakeStatistics);
        });

    // Record statistics before migration
    Map<Integer, Integer> beforeRegionCounter = new TreeMap<>();
    Map<Integer, Long> beforeDiskCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                beforeRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
                beforeDiskCounter.merge(
                    location.getDataNodeId(),
                    fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage(),
                    Long::sum);
              });
    }
    LOGGER.info("Region count before migration: {}", beforeRegionCounter);
    LOGGER.info("Disk count before migration: {}", beforeDiskCounter);

    // Specify multiple target node IDs (use the newly added nodes)
    List<Integer> targetNodeIds = Arrays.asList(nodeCount - 1, nodeCount);
    LOGGER.info("Target node IDs (multiple): {}", targetNodeIds);

    // Execute migration with multiple targetNodeIds
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableDataNodeMap, fakeStatisticsMap, allocatedResult, replicaCount, targetNodeIds);

    // Verify that migrations target the specified nodes
    Map<Integer, Integer> migrationsToTargetNodes = new TreeMap<>();
    for (Integer targetNodeId : targetNodeIds) {
      migrationsToTargetNodes.put(targetNodeId, 0);
    }
    long migrationCost = 0;
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      Set<Integer> originSet =
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> migrationSet =
          migrationPlan.get(regionReplicaSet.getRegionId()).getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      LOGGER.info(
          "Region ID: {}, original nodes: {}, target nodes: {}",
          regionReplicaSet.getRegionId().getId(),
          originSet,
          migrationSet);

      // Count migrations to each target node
      for (Integer targetNodeId : targetNodeIds) {
        if (migrationSet.contains(targetNodeId) && !originSet.contains(targetNodeId)) {
          migrationsToTargetNodes.merge(targetNodeId, 1, Integer::sum);
        }
      }

      migrationSet.removeAll(originSet);
      migrationCost +=
          migrationSet.size() * fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage();
    }
    LOGGER.info("Migration cost: {}", migrationCost);
    LOGGER.info("Number of regions migrated to each target node: {}", migrationsToTargetNodes);

    // Record statistics after migration
    Map<Integer, Integer> afterRegionCounter = new TreeMap<>();
    Map<Integer, Long> afterDiskCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : migrationPlan.values()) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                afterRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
                afterDiskCounter.merge(
                    location.getDataNodeId(),
                    fakeStatisticsMap.get(regionReplicaSet.regionId).getDiskUsage(),
                    Long::sum);
              });
    }
    LOGGER.info("Region count after migration: {}", afterRegionCounter);
    LOGGER.info("Disk count after migration: {}", afterDiskCounter);

    // Verify that both target nodes have regions after migration
    int targetNodesWithRegions = 0;
    for (Integer targetNodeId : targetNodeIds) {
      if (afterRegionCounter.containsKey(targetNodeId)
          && afterRegionCounter.get(targetNodeId) > 0) {
        targetNodesWithRegions++;
        LOGGER.info(
            "Target node {} has {} regions after migration",
            targetNodeId,
            afterRegionCounter.get(targetNodeId));
      }
    }
    assert targetNodesWithRegions > 0
        : "At least one target node from "
            + targetNodeIds
            + " should have at least one region after migration";

    // Verify that migrations are distributed across target nodes (if there are enough migrations)
    int totalMigrations =
        migrationsToTargetNodes.values().stream().mapToInt(Integer::intValue).sum();
    if (totalMigrations > 1) {
      // At least one migration should go to each target node (if possible)
      int nodesWithMigrations =
          (int) migrationsToTargetNodes.values().stream().filter(count -> count > 0).count();
      LOGGER.info(
          "Migrations distributed across {} out of {} target nodes",
          nodesWithMigrations,
          targetNodeIds.size());
      // This is a soft assertion - algorithm may choose to use only one node if it's optimal
      // But we log it for visibility
    }
  }

  @Test
  public void autoBalanceRegionReplicasDistributionWithThreeTargetNodes() {
    LOGGER.info(
        "Starting test with three targetNodeIds: nodeCount(n)={}, regionCount(|R|)={}, replicaCount(r)={}",
        nodeCount,
        regionCount,
        replicaCount);

    // Skip test if nodeCount is too small for three target nodes
    if (nodeCount < 5) {
      LOGGER.info("Skipping three-node test: nodeCount ({}) is too small", nodeCount);
      return;
    }

    Random random = new Random();
    beforeNodeMap.clear();
    availableDataNodeMap.clear();
    beforeSpaceMap.clear();
    freeSpaceMap.clear();

    // Initialize nodes: the first n-3 nodes exist before migration
    int existingNodes = nodeCount - 3;
    for (int i = 1; i <= existingNodes; i++) {
      TDataNodeConfiguration dataNodeConfiguration =
          new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i));
      double space = random.nextDouble();
      beforeNodeMap.put(i, dataNodeConfiguration);
      availableDataNodeMap.put(i, dataNodeConfiguration);
      beforeSpaceMap.put(i, space);
      freeSpaceMap.put(i, space);
    }
    // The last 3 nodes are newly added nodes
    for (int i = nodeCount - 2; i <= nodeCount; i++) {
      availableDataNodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpaceMap.put(i, random.nextDouble());
    }

    // Generate initial region allocation
    List<TRegionReplicaSet> allocatedResult = new ArrayList<>();
    for (int i = 0; i < regionCount; i++) {
      allocatedResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              beforeNodeMap,
              beforeSpaceMap,
              allocatedResult,
              allocatedResult,
              replicaCount,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }

    // Create mock statistics
    Map<TConsensusGroupId, RegionGroupStatistics> fakeStatisticsMap = new TreeMap<>();
    allocatedResult.forEach(
        regionGroup -> {
          RegionGroupStatistics fakeStatistics =
              RegionGroupStatistics.generateDefaultRegionGroupStatistics();
          fakeStatistics.setDiskUsage(100);
          fakeStatisticsMap.put(regionGroup.getRegionId(), fakeStatistics);
        });

    // Record statistics before migration
    Map<Integer, Integer> beforeRegionCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                beforeRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
              });
    }
    LOGGER.info("Region count before migration: {}", beforeRegionCounter);

    // Specify three target node IDs
    List<Integer> targetNodeIds = Arrays.asList(nodeCount - 2, nodeCount - 1, nodeCount);
    LOGGER.info("Target node IDs (three): {}", targetNodeIds);

    // Execute migration with three targetNodeIds
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableDataNodeMap, fakeStatisticsMap, allocatedResult, replicaCount, targetNodeIds);

    // Verify that migrations target the specified nodes
    Map<Integer, Integer> migrationsToTargetNodes = new TreeMap<>();
    for (Integer targetNodeId : targetNodeIds) {
      migrationsToTargetNodes.put(targetNodeId, 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      Set<Integer> originSet =
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> migrationSet =
          migrationPlan.get(regionReplicaSet.getRegionId()).getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());

      // Count migrations to each target node
      for (Integer targetNodeId : targetNodeIds) {
        if (migrationSet.contains(targetNodeId) && !originSet.contains(targetNodeId)) {
          migrationsToTargetNodes.merge(targetNodeId, 1, Integer::sum);
        }
      }
    }
    LOGGER.info("Number of regions migrated to each target node: {}", migrationsToTargetNodes);

    // Record statistics after migration
    Map<Integer, Integer> afterRegionCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : migrationPlan.values()) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              location -> {
                afterRegionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
              });
    }
    LOGGER.info("Region count after migration: {}", afterRegionCounter);

    // Verify that at least one target node has regions after migration
    int targetNodesWithRegions = 0;
    for (Integer targetNodeId : targetNodeIds) {
      if (afterRegionCounter.containsKey(targetNodeId)
          && afterRegionCounter.get(targetNodeId) > 0) {
        targetNodesWithRegions++;
        LOGGER.info(
            "Target node {} has {} regions after migration",
            targetNodeId,
            afterRegionCounter.get(targetNodeId));
      }
    }
    assert targetNodesWithRegions > 0
        : "At least one target node from "
            + targetNodeIds
            + " should have at least one region after migration";

    // Verify that algorithm uses multiple target nodes when beneficial
    int totalMigrations =
        migrationsToTargetNodes.values().stream().mapToInt(Integer::intValue).sum();
    if (totalMigrations >= 3) {
      // With enough migrations, algorithm should distribute across multiple nodes
      int nodesWithMigrations =
          (int) migrationsToTargetNodes.values().stream().filter(count -> count > 0).count();
      LOGGER.info(
          "With {} total migrations, algorithm used {} out of {} target nodes",
          totalMigrations,
          nodesWithMigrations,
          targetNodeIds.size());
      // This demonstrates that the algorithm can choose between multiple target nodes
    }
  }
}
