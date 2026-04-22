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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic scenario tests for {@link GreedyCopySetRegionGroupMigrator}.
 *
 * <p>Each test constructs a specific cluster scenario with known data, executes migration, and
 * verifies correctness with quantitative assertions.
 */
public class GCRMigratorScenarioTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCRMigratorScenarioTest.class);
  private static final GreedyCopySetRegionGroupMigrator MIGRATOR =
      new GreedyCopySetRegionGroupMigrator();

  /**
   * Scenario A: Standard scale-out with uniform diskUsage.
   *
   * <p>3 existing nodes with 9 RegionGroups (r=2), all diskUsage=100MB. Add 1 new node N4.
   *
   * <p>Expected: Region count and disk usage should become more balanced. N4 should receive
   * regions.
   */
  @Test
  public void testStandardScaleOut() {
    int replicaCount = 2;

    // Build cluster: N1~N3 existing, N4 new
    int[] existingNodes = {1, 2, 3};
    int newNode = 4;
    Map<Integer, TDataNodeConfiguration> beforeNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);

    // Allocate 9 RegionGroups on N1~N3 using PGP
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(beforeNodeMap, spaceMap, 9, replicaCount);

    // Uniform diskUsage = 100MB for all RegionGroups
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildUniformStatisticsMap(allocatedResult, 100_000_000L);

    // All nodes (including new) available after scale-out
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before migration (N4 has 0 regions)
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute migration
    List<Integer> targetNodeIds = Collections.singletonList(newNode);
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, targetNodeIds);

    // Metrics after migration
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    Assert.assertTrue(
        "New node " + newNode + " should have at least 1 region after migration",
        afterRegionCounter.getOrDefault(newNode, 0) > 0);
  }

  /**
   * Scenario B: Scale-out with skewed diskUsage (large and small regions).
   *
   * <p>3 existing nodes with 9 RegionGroups (r=2). RegionGroups whose replicas include N1 are
   * assigned high diskUsage (500MB), others get low diskUsage (50MB). This makes N1 a disk-heavy
   * node. Add 1 new node N4.
   *
   * <p>Expected: Algorithm should prioritize migrating large regions away from N1. Both Var(region)
   * and Var(disk) should not worsen.
   */
  @Test
  public void testSkewedDiskScaleOut() {
    int replicaCount = 2;

    // Build cluster: N1~N3 existing, N4 new
    int[] existingNodes = {1, 2, 3};
    int newNode = 4;
    Map<Integer, TDataNodeConfiguration> beforeNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);

    // Allocate 9 RegionGroups on N1~N3 using PGP
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(beforeNodeMap, spaceMap, 9, replicaCount);

    // Skewed diskUsage: RegionGroups with a replica on N1 get 500MB, others get 50MB
    long[] diskUsages = new long[allocatedResult.size()];
    for (int i = 0; i < allocatedResult.size(); i++) {
      boolean containsN1 =
          allocatedResult.get(i).getDataNodeLocations().stream()
              .anyMatch(loc -> loc.getDataNodeId() == 1);
      diskUsages[i] = containsN1 ? 500_000_000L : 50_000_000L;
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All nodes available after scale-out
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before migration
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute migration
    List<Integer> targetNodeIds = Collections.singletonList(newNode);
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, targetNodeIds);

    // Metrics after migration
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    Assert.assertTrue(
        "New node " + newNode + " should have at least 1 region after migration",
        afterRegionCounter.getOrDefault(newNode, 0) > 0);
  }

  /**
   * Scenario C: Already balanced cluster — should not over-migrate.
   *
   * <p>4 nodes with 12 RegionGroups (r=2) allocated across all 4 nodes. diskUsage uniform (100MB).
   * targetNodeIds = [4] but N4 already has regions from the initial allocation.
   *
   * <p>Expected: Cluster is already balanced. Migration count should be 0 (no improvement
   * possible).
   */
  @Test
  public void testAlreadyBalancedNoMigration() {
    int replicaCount = 2;

    // Build cluster: all 4 nodes exist from the beginning
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> nodeMap = GCRMigratorTestHelper.buildNodeMap(allNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(allNodes);

    // Allocate 12 RegionGroups on all 4 nodes using PGP (already balanced)
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(nodeMap, spaceMap, 12, replicaCount);

    // Uniform diskUsage = 100MB
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildUniformStatisticsMap(allocatedResult, 100_000_000L);

    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before migration
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));

    // Execute migration with N4 as target (but N4 already has regions)
    List<Integer> targetNodeIds = Collections.singletonList(4);
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            nodeMap, statsMap, allocatedResult, replicaCount, targetNodeIds);

    // Metrics after migration
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info("Migrations: {}", migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    Assert.assertEquals("Already balanced cluster should have 0 migrations", 0, migrations);
  }

  /**
   * Scenario D: Scale-out with multiple target nodes.
   *
   * <p>5 existing nodes with 30 RegionGroups (r=2), differentiated diskUsage. Add 2 new nodes N6,
   * N7.
   *
   * <p>Expected: Both new nodes should receive regions. Variance metrics should not worsen.
   */
  @Test
  public void testMultiTargetNodeScaleOut() {
    int replicaCount = 2;

    // Build cluster: N1~N5 existing, N6/N7 new
    int[] existingNodes = {1, 2, 3, 4, 5};
    int[] newNodes = {6, 7};
    Map<Integer, TDataNodeConfiguration> beforeNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);

    // Allocate 30 RegionGroups on N1~N5 using PGP
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(beforeNodeMap, spaceMap, 30, replicaCount);

    // Differentiated diskUsage: deterministic based on regionId, range [50MB, 500MB)
    long[] diskUsages = new long[allocatedResult.size()];
    for (int i = 0; i < diskUsages.length; i++) {
      diskUsages[i] = 50_000_000L + ((long) i * 37_000_000L) % 450_000_000L; // range [50MB, 500MB)
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All nodes available after scale-out
    int[] allNodes = {1, 2, 3, 4, 5, 6, 7};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>();
    for (int id : allNodes) {
      allNodeIds.add(id);
    }

    // Metrics before migration (N6, N7 have 0 regions)
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute migration with two target nodes
    List<Integer> targetNodeIds = Arrays.asList(newNodes[0], newNodes[1]);
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, targetNodeIds);

    // Metrics after migration
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    for (int newNode : newNodes) {
      Assert.assertTrue(
          "New node " + newNode + " should have at least 1 region after migration",
          afterRegionCounter.getOrDefault(newNode, 0) > 0);
    }
  }

  /**
   * Scenario F3: Two-database — GCR vs Greedy vs PGP comparison (bidirectional mode).
   *
   * <p>Models a realistic IoT cluster with 2 databases whose write pressure ratio is 5:1, producing
   * Region sizes of 5GB vs 1GB. Initial allocation is on 4 nodes; 2 new nodes join (6 total). All
   * algorithms run in bidirectional (LOAD BALANCE ALL) mode.
   *
   * <p>Setup: 4 existing nodes, replica factor = 2
   *
   * <ul>
   *   <li>db_fast: 12 RegionGroups (IDs 0~11), each 5 GB — higher write pressure
   *   <li>db_slow: 12 RegionGroups (IDs 12~23), each 1 GB — lower write pressure
   *   <li>Total: 24 RegionGroups, size ratio 5:1
   * </ul>
   *
   * <p>Region count reflects write pressure: both databases have the same number of Regions, but
   * db_fast Regions are 5x larger due to higher write throughput. Balancing Region count ensures
   * future write load is evenly distributed, which is critical for Phase 2 (online performance
   * test).
   *
   * <p>Expected: All algorithms improve Var(disk). GCR migration cost <= PGP migration cost.
   */
  @Test
  public void testMultiDatabaseMigratorComparison() {
    int replicaCount = 2;
    GreedyRegionGroupMigrator greedyMigrator = new GreedyRegionGroupMigrator();
    PGPRebalanceRegionGroupMigrator pgpMigrator = new PGPRebalanceRegionGroupMigrator();

    // Build initial cluster: N1~N4
    int[] existingNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> existingNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);

    // Allocate RegionGroups for 2 databases on N1~N4 using PGP:
    //   db_fast: 12 RegionGroups (IDs 0~11),  each 5 GB  — write pressure 5x
    //   db_slow: 12 RegionGroups (IDs 12~23), each 1 GB  — write pressure 1x
    // Total: 24 RegionGroups, size ratio 5:1
    int dbFastCount = 12;
    int dbSlowCount = 12;

    List<TRegionReplicaSet> dbFastRegions =
        GCRMigratorTestHelper.allocateWithPGP(existingNodeMap, spaceMap, dbFastCount, replicaCount);
    List<TRegionReplicaSet> dbSlowRegions =
        GCRMigratorTestHelper.allocateWithPGP(existingNodeMap, spaceMap, dbSlowCount, replicaCount);
    for (int i = 0; i < dbSlowRegions.size(); i++) {
      dbSlowRegions
          .get(i)
          .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, dbFastCount + i));
    }

    List<TRegionReplicaSet> allRegions = new ArrayList<>();
    allRegions.addAll(dbFastRegions);
    allRegions.addAll(dbSlowRegions);

    // DiskUsage: 5GB / 1GB (ratio 5:1)
    long[] diskUsages = new long[allRegions.size()];
    for (int i = 0; i < dbFastCount; i++) {
      diskUsages[i] = 5_000_000_000L; // 5 GB
    }
    for (int i = dbFastCount; i < allRegions.size(); i++) {
      diskUsages[i] = 1_000_000_000L; // 1 GB
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allRegions, diskUsages);

    // Scale-out: N5, N6 join (0 regions, 0 disk)
    int[] allNodes = {1, 2, 3, 4, 5, 6};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6));

    // Before metrics (same for all algorithms)
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allRegions, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allRegions, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "F3 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("F3 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Run all 3 algorithms in bidirectional mode (empty targetNodeIds)
    Map<TConsensusGroupId, TRegionReplicaSet> gcrPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allRegions, replicaCount, Collections.emptyList());
    Map<TConsensusGroupId, TRegionReplicaSet> greedyPlan =
        greedyMigrator.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allRegions, replicaCount, Collections.emptyList());
    Map<TConsensusGroupId, TRegionReplicaSet> pgpPlan =
        pgpMigrator.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allRegions, replicaCount, Collections.emptyList());

    // Compute metrics for each algorithm
    Map<Integer, Integer> gcrRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(gcrPlan.values(), allNodeIds);
    Map<Integer, Long> gcrDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(gcrPlan.values(), statsMap, allNodeIds);
    long gcrVarRegion = GCRMigratorTestHelper.computeVariance(gcrRegionCounter);
    long gcrVarDisk =
        GCRMigratorTestHelper.computeVariance(
            gcrDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int gcrMigrations = GCRMigratorTestHelper.countMigrations(allRegions, gcrPlan);
    long gcrCost = GCRMigratorTestHelper.computeMigrationCost(allRegions, gcrPlan, statsMap);

    Map<Integer, Integer> greedyRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(greedyPlan.values(), allNodeIds);
    Map<Integer, Long> greedyDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(greedyPlan.values(), statsMap, allNodeIds);
    long greedyVarRegion = GCRMigratorTestHelper.computeVariance(greedyRegionCounter);
    long greedyVarDisk =
        GCRMigratorTestHelper.computeVariance(
            greedyDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int greedyMigrations = GCRMigratorTestHelper.countMigrations(allRegions, greedyPlan);
    long greedyCost = GCRMigratorTestHelper.computeMigrationCost(allRegions, greedyPlan, statsMap);

    Map<Integer, Integer> pgpRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(pgpPlan.values(), allNodeIds);
    Map<Integer, Long> pgpDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(pgpPlan.values(), statsMap, allNodeIds);
    long pgpVarRegion = GCRMigratorTestHelper.computeVariance(pgpRegionCounter);
    long pgpVarDisk =
        GCRMigratorTestHelper.computeVariance(
            pgpDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int pgpMigrations = GCRMigratorTestHelper.countMigrations(allRegions, pgpPlan);
    long pgpCost = GCRMigratorTestHelper.computeMigrationCost(allRegions, pgpPlan, statsMap);

    LOGGER.info(
        "F3 GCR:    regionCounter={}, diskCounter={}, Var(region)={}, Var(disk)={}, migrations={}, cost={}MB",
        gcrRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(gcrDiskCounter),
        gcrVarRegion,
        gcrVarDisk,
        gcrMigrations,
        gcrCost / GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "F3 Greedy: regionCounter={}, diskCounter={}, Var(region)={}, Var(disk)={}, migrations={}, cost={}MB",
        greedyRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(greedyDiskCounter),
        greedyVarRegion,
        greedyVarDisk,
        greedyMigrations,
        greedyCost / GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "F3 PGP:    regionCounter={}, diskCounter={}, Var(region)={}, Var(disk)={}, migrations={}, cost={}MB",
        pgpRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(pgpDiskCounter),
        pgpVarRegion,
        pgpVarDisk,
        pgpMigrations,
        pgpCost / GCRMigratorTestHelper.DISK_SCALE_FACTOR);

    // All algorithms should improve disk variance
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, gcrVarDisk, "GCR Var(disk)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, greedyVarDisk, "Greedy Var(disk)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, pgpVarDisk, "PGP Var(disk)");

    // All algorithms should satisfy replica constraint
    GCRMigratorTestHelper.assertReplicaConstraint(gcrPlan, replicaCount);
    GCRMigratorTestHelper.assertReplicaConstraint(greedyPlan, replicaCount);
    GCRMigratorTestHelper.assertReplicaConstraint(pgpPlan, replicaCount);

    // GCR should have lower migration cost than PGP (cost-aware vs placement-unaware)
    Assert.assertTrue(
        "GCR migration cost ("
            + gcrCost / GCRMigratorTestHelper.DISK_SCALE_FACTOR
            + "MB) should be <= PGP ("
            + pgpCost / GCRMigratorTestHelper.DISK_SCALE_FACTOR
            + "MB)",
        gcrCost <= pgpCost);
  }

  // ==================== Bidirectional Mode (LOAD BALANCE ALL) ====================

  /**
   * Scenario E1: Bidirectional — Region imbalanced, disk uniform (Phase 1 only).
   *
   * <p>4 nodes, 12 RegionGroups (r=2), but all regions placed only on N1~N3 (N4 has 0). All
   * diskUsage uniform (100MB). targetNodeIds = empty → bidirectional mode.
   *
   * <p>Expected: Phase 1 rebalances region count so N4 gets regions. Phase 2 skipped since uniform
   * disk means deviations are within δ after Phase 1 rebalance.
   */
  @Test
  public void testBidirectionalRegionImbalancedDiskUniform() {
    int replicaCount = 2;

    // Allocate all 12 RegionGroups on N1~N3 only
    int[] existingNodes = {1, 2, 3};
    Map<Integer, TDataNodeConfiguration> existingNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(existingNodeMap, spaceMap, 12, replicaCount);

    // Uniform diskUsage = 100MB
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildUniformStatisticsMap(allocatedResult, 100_000_000L);

    // All 4 nodes available
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E1 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E1 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration (empty targetNodeIds)
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E1 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E1 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    // N4 should now have regions
    Assert.assertTrue(
        "N4 should have regions after bidirectional balance",
        afterRegionCounter.getOrDefault(4, 0) > 0);
    // Region count should be balanced: ideal = 12*2/4 = 6 per node
    int idealFloor = 12 * replicaCount / 4;
    int idealCeil = idealFloor + (12 * replicaCount % 4 == 0 ? 0 : 1);
    for (int nodeId : allNodes) {
      int count = afterRegionCounter.getOrDefault(nodeId, 0);
      Assert.assertTrue(
          "Node "
              + nodeId
              + " region count "
              + count
              + " not in ["
              + idealFloor
              + ","
              + idealCeil
              + "]",
          count >= idealFloor && count <= idealCeil);
    }
  }

  /**
   * Scenario E2: Bidirectional — Region and disk both imbalanced (Phase 1 → Phase 2).
   *
   * <p>4 nodes, 12 RegionGroups (r=2), all on N1~N3. RegionGroups with replicas including N1 get
   * 500MB, others get 50MB. targetNodeIds = empty.
   *
   * <p>Expected: Phase 1 redistributes region count. Phase 2 further optimizes disk balance. Both
   * variances should improve.
   */
  @Test
  public void testBidirectionalBothImbalanced() {
    int replicaCount = 2;

    // Allocate all 12 RegionGroups on N1~N3 only
    int[] existingNodes = {1, 2, 3};
    Map<Integer, TDataNodeConfiguration> existingNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(existingNodeMap, spaceMap, 12, replicaCount);

    // Skewed diskUsage: regions with N1 replica → 500MB, others → 50MB
    long[] diskUsages = new long[allocatedResult.size()];
    for (int i = 0; i < allocatedResult.size(); i++) {
      boolean containsN1 =
          allocatedResult.get(i).getDataNodeLocations().stream()
              .anyMatch(loc -> loc.getDataNodeId() == 1);
      diskUsages[i] = containsN1 ? 500_000_000L : 50_000_000L;
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All 4 nodes available
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E2 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E2 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E2 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E2 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    Assert.assertTrue(
        "N4 should have regions after bidirectional balance",
        afterRegionCounter.getOrDefault(4, 0) > 0);
    Assert.assertTrue("Should have at least 1 migration", migrations > 0);
  }

  /**
   * Scenario E3: Bidirectional — Region uniform, disk severely skewed (Phase 2 only).
   *
   * <p>3 nodes, 9 RegionGroups (r=2), evenly distributed (6 replicas per node). N1 has large
   * regions (500MB), N2/N3 have small (50MB). targetNodeIds = empty.
   *
   * <p>Expected: Phase 1 skipped (regions already balanced). Phase 2 migrates large regions away
   * from N1 and small regions to N1, reducing disk variance without breaking region balance.
   */
  @Test
  public void testBidirectionalDiskSkewedOnly() {
    int replicaCount = 2;

    // Build a balanced cluster: 3 nodes, 9 RegionGroups
    int[] allNodes = {1, 2, 3};
    Map<Integer, TDataNodeConfiguration> nodeMap = GCRMigratorTestHelper.buildNodeMap(allNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(allNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(nodeMap, spaceMap, 9, replicaCount);

    // Skewed diskUsage: regions with N1 replica → 500MB, others → 50MB
    long[] diskUsages = new long[allocatedResult.size()];
    for (int i = 0; i < allocatedResult.size(); i++) {
      boolean containsN1 =
          allocatedResult.get(i).getDataNodeLocations().stream()
              .anyMatch(loc -> loc.getDataNodeId() == 1);
      diskUsages[i] = containsN1 ? 500_000_000L : 50_000_000L;
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3));

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E3 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E3 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            nodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E3 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E3 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    // Region count should remain balanced (Phase 1 skipped)
    int idealFloor = 9 * replicaCount / 3;
    int idealCeil = idealFloor + (9 * replicaCount % 3 == 0 ? 0 : 1);
    for (int nodeId : allNodes) {
      int count = afterRegionCounter.getOrDefault(nodeId, 0);
      Assert.assertTrue(
          "Node "
              + nodeId
              + " region count "
              + count
              + " should still be in ["
              + idealFloor
              + ","
              + idealCeil
              + "]",
          count >= idealFloor && count <= idealCeil);
    }
  }

  /**
   * Scenario E4: Bidirectional — Region uniform, disk within δ threshold (no migration).
   *
   * <p>3 nodes, 9 RegionGroups (r=2), evenly distributed. Disk slightly different but all within
   * 20% deviation from mean. targetNodeIds = empty.
   *
   * <p>Expected: Phase 1 skipped (regions balanced). Phase 2 skipped (disk within δ). 0 migrations.
   */
  @Test
  public void testBidirectionalDiskWithinThreshold() {
    int replicaCount = 2;

    // Build balanced cluster
    int[] allNodes = {1, 2, 3};
    Map<Integer, TDataNodeConfiguration> nodeMap = GCRMigratorTestHelper.buildNodeMap(allNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(allNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(nodeMap, spaceMap, 9, replicaCount);

    // Slightly different diskUsage: 90MB, 100MB, 110MB (cycle)
    // Per node, total disk will be within ~10% of mean
    long[] diskUsages = new long[allocatedResult.size()];
    long[] options = {90_000_000L, 100_000_000L, 110_000_000L};
    for (int i = 0; i < allocatedResult.size(); i++) {
      diskUsages[i] = options[i % 3];
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3));

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E4 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E4 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            nodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E4 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E4 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
  }

  /**
   * Scenario E5: Bidirectional — Completely balanced (0 migrations).
   *
   * <p>4 nodes, 12 RegionGroups (r=2), PGP-allocated across all 4 nodes. Uniform diskUsage.
   * targetNodeIds = empty.
   *
   * <p>Expected: Both phases skipped. 0 migrations.
   */
  @Test
  public void testBidirectionalCompletelyBalanced() {
    int replicaCount = 2;

    // All 4 nodes from the start
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> nodeMap = GCRMigratorTestHelper.buildNodeMap(allNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(allNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(nodeMap, spaceMap, 12, replicaCount);

    // Uniform diskUsage = 100MB
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildUniformStatisticsMap(allocatedResult, 100_000_000L);

    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E5 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            nodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    long afterVarRegion =
        GCRMigratorTestHelper.computeVariance(
            GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds));
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds),
            GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E5 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    Assert.assertEquals("Completely balanced cluster should have 0 migrations", 0, migrations);
  }

  /**
   * Scenario E6: Bidirectional — Extreme imbalance (all regions on one node pair).
   *
   * <p>4 nodes, 8 RegionGroups (r=2), all manually placed on [N1, N2]. N3 and N4 have 0 regions.
   * diskUsage differentiated. targetNodeIds = empty.
   *
   * <p>Expected: Phase 1 aggressively rebalances. All nodes should get regions. Variances should
   * improve dramatically.
   */
  @Test
  public void testBidirectionalExtremeImbalance() {
    int replicaCount = 2;

    // All 8 RegionGroups placed on [N1, N2]
    int[][] placements = new int[8][];
    for (int i = 0; i < 8; i++) {
      placements[i] = new int[] {1, 2};
    }
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.buildManualReplicaSets(placements);

    // Differentiated diskUsage
    long[] diskUsages = new long[8];
    for (int i = 0; i < 8; i++) {
      diskUsages[i] = 100_000_000L + (long) i * 50_000_000L; // 100MB ~ 450MB
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All 4 nodes available
    int[] allNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));

    // Metrics before: N1=8, N2=8, N3=0, N4=0
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E6 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E6 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E6 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E6 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    // All nodes should have regions: ideal = 8*2/4 = 4 per node
    for (int nodeId : allNodes) {
      Assert.assertTrue(
          "Node " + nodeId + " should have at least 1 region after extreme rebalance",
          afterRegionCounter.getOrDefault(nodeId, 0) > 0);
    }
    Assert.assertTrue("Should have multiple migrations for extreme case", migrations >= 4);
  }

  /**
   * Scenario E7: Bidirectional — Phase 1→Phase 2 handoff verification.
   *
   * <p>5 nodes, 20 RegionGroups (r=2), all on N1~N4 (N5 has 0). RegionGroups with N1 replica get
   * 400MB, others get 50MB. targetNodeIds = empty.
   *
   * <p>Expected: Phase 1 redistributes regions to include N5. Phase 2 further optimizes disk by
   * moving large regions away from N1. Region count remains balanced after both phases.
   */
  @Test
  public void testBidirectionalPhaseHandoff() {
    int replicaCount = 2;

    // Allocate 20 RegionGroups on N1~N4 only
    int[] existingNodes = {1, 2, 3, 4};
    Map<Integer, TDataNodeConfiguration> existingNodeMap =
        GCRMigratorTestHelper.buildNodeMap(existingNodes);
    Map<Integer, Double> spaceMap = GCRMigratorTestHelper.buildUniformSpaceMap(existingNodes);
    List<TRegionReplicaSet> allocatedResult =
        GCRMigratorTestHelper.allocateWithPGP(existingNodeMap, spaceMap, 20, replicaCount);

    // Skewed diskUsage: regions with N1 replica → 400MB, others → 50MB
    long[] diskUsages = new long[allocatedResult.size()];
    for (int i = 0; i < allocatedResult.size(); i++) {
      boolean containsN1 =
          allocatedResult.get(i).getDataNodeLocations().stream()
              .anyMatch(loc -> loc.getDataNodeId() == 1);
      diskUsages[i] = containsN1 ? 400_000_000L : 50_000_000L;
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        GCRMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All 5 nodes available
    int[] allNodes = {1, 2, 3, 4, 5};
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        GCRMigratorTestHelper.buildNodeMap(allNodes);
    Set<Integer> allNodeIds = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));

    // Metrics before (N5 has 0 regions)
    Map<Integer, Integer> beforeRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIds);
    Map<Integer, Long> beforeDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIds);
    long beforeVarRegion = GCRMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        GCRMigratorTestHelper.computeVariance(
            beforeDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info(
        "E7 Before: regionCounter={}, diskCounter={}",
        beforeRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info("E7 Before: Var(region)={}, Var(disk)={}", beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        GCRMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIds);
    Map<Integer, Long> afterDiskCounter =
        GCRMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIds);
    long afterVarRegion = GCRMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        GCRMigratorTestHelper.computeVariance(
            afterDiskCounter, GCRMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = GCRMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    LOGGER.info(
        "E7 After: regionCounter={}, diskCounter={}",
        afterRegionCounter,
        GCRMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "E7 After: Var(region)={}, Var(disk)={}, migrations={}",
        afterVarRegion,
        afterVarDisk,
        migrations);

    // Assertions
    GCRMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    GCRMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
    GCRMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");
    // N5 should have regions after Phase 1
    Assert.assertTrue(
        "N5 should have regions after bidirectional balance",
        afterRegionCounter.getOrDefault(5, 0) > 0);
    // Region count should be balanced: ideal = 20*2/5 = 8 per node
    int idealFloor = 20 * replicaCount / 5;
    int idealCeil = idealFloor + (20 * replicaCount % 5 == 0 ? 0 : 1);
    for (int nodeId : allNodes) {
      int count = afterRegionCounter.getOrDefault(nodeId, 0);
      Assert.assertTrue(
          "Node "
              + nodeId
              + " region count "
              + count
              + " not in ["
              + idealFloor
              + ","
              + idealCeil
              + "]",
          count >= idealFloor && count <= idealCeil);
    }
    Assert.assertTrue("Should have multiple migrations", migrations > 0);
  }
}
