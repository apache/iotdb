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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Parameterized scale tests for bidirectional mode of {@link CostAwareRegionGroupMigrator}, {@link
 * GreedyRegionGroupMigrator}, and {@link PGPRebalanceRegionGroupMigrator}.
 *
 * <p>Tests the algorithm's bidirectional migration (LOAD BALANCE ALL) across different cluster
 * sizes, region counts, and replication factors. Initial placement is done on a subset of nodes,
 * then bidirectional mode rebalances across all nodes.
 *
 * <p>For CAR, strict assertions apply: region counts must be within [idealFloor, idealCeil]. For
 * Greedy and PGPRebalance, relaxed assertions apply: only check variance non-worsening, replica
 * constraints, and new nodes having regions.
 */
@RunWith(Parameterized.class)
public class CARMigratorBidirectionalScaleTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CARMigratorBidirectionalScaleTest.class);
  private static final CostAwareRegionGroupMigrator CAR_MIGRATOR =
      new CostAwareRegionGroupMigrator();
  private static final GreedyRegionGroupMigrator GREEDY_MIGRATOR = new GreedyRegionGroupMigrator();
  private static final PGPRebalanceRegionGroupMigrator PGP_MIGRATOR =
      new PGPRebalanceRegionGroupMigrator();

  private final String migratorName;
  private final IRegionGroupMigrator migrator;
  private final int totalNodeCount;
  private final int initialNodeCount;
  private final int regionCount;
  private final int replicaCount;
  private final boolean skewedDisk;

  @Parameterized.Parameters(
      name = "migrator={0}, total={1}, initial={2}, |R|={3}, r={4}, skewedDisk={5}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    Object[][] scenarios =
        new Object[][] {
          // {totalNodes, initialNodes, regionCount, replicaCount, skewedDisk}
          //          {4, 3, 12, 2, false}, // small: region imbalance, uniform disk
          //          {4, 3, 12, 2, true}, // small: region imbalance, skewed disk
          //          {5, 4, 20, 2, false}, // medium: region imbalance, uniform disk
          //          {5, 4, 20, 2, true}, // medium: region imbalance, skewed disk
          {100, 90, 900, 2, false}, // medium: 3 replicas, uniform disk
          {100, 90, 900, 2, true}, // medium: 3 replicas, skewed disk
          //          {8, 6, 48, 2, false}, // large: region imbalance, uniform disk
          //          {8, 6, 48, 2, true}, // large: region imbalance, skewed disk
          //          {10, 8, 80, 2, true}, // large: 10 nodes, skewed disk
        };
    for (Object[] scenario : scenarios) {
      // CAR migrator
      params.add(
          new Object[] {"CAR", scenario[0], scenario[1], scenario[2], scenario[3], scenario[4]});
      // Greedy migrator
      params.add(
          new Object[] {"Greedy", scenario[0], scenario[1], scenario[2], scenario[3], scenario[4]});
      // PGPRebalance migrator
      params.add(
          new Object[] {
            "PGPRebalance", scenario[0], scenario[1], scenario[2], scenario[3], scenario[4]
          });
    }
    return params;
  }

  public CARMigratorBidirectionalScaleTest(
      String migratorName,
      int totalNodeCount,
      int initialNodeCount,
      int regionCount,
      int replicaCount,
      boolean skewedDisk) {
    this.migratorName = migratorName;
    if ("CAR".equals(migratorName)) {
      this.migrator = CAR_MIGRATOR;
    } else if ("Greedy".equals(migratorName)) {
      this.migrator = GREEDY_MIGRATOR;
    } else {
      this.migrator = PGP_MIGRATOR;
    }
    this.totalNodeCount = totalNodeCount;
    this.initialNodeCount = initialNodeCount;
    this.regionCount = regionCount;
    this.replicaCount = replicaCount;
    this.skewedDisk = skewedDisk;
  }

  @Test
  public void testBidirectionalBalance() {
    LOGGER.info(
        "[{}] Bidirectional scale test: totalNodes={}, initialNodes={}, regionCount={}, replicaCount={}, skewedDisk={}",
        migratorName,
        totalNodeCount,
        initialNodeCount,
        regionCount,
        replicaCount,
        skewedDisk);

    // Build initial nodes: 1 ~ initialNodeCount
    int[] initialNodeIds = IntStream.rangeClosed(1, initialNodeCount).toArray();
    Map<Integer, TDataNodeConfiguration> initialNodeMap =
        CARMigratorTestHelper.buildNodeMap(initialNodeIds);
    Map<Integer, Double> spaceMap = CARMigratorTestHelper.buildUniformSpaceMap(initialNodeIds);

    // Allocate RegionGroups on initial nodes using PGP
    List<TRegionReplicaSet> allocatedResult =
        CARMigratorTestHelper.allocateWithPGP(initialNodeMap, spaceMap, regionCount, replicaCount);

    // DiskUsage: skewed or uniform
    long[] diskUsages = new long[allocatedResult.size()];
    if (skewedDisk) {
      // Skewed: regions with N1 replica get 500MB, others get 50MB
      for (int i = 0; i < allocatedResult.size(); i++) {
        boolean containsN1 =
            allocatedResult.get(i).getDataNodeLocations().stream()
                .anyMatch(loc -> loc.getDataNodeId() == 1);
        diskUsages[i] = containsN1 ? 500_000_000L : 50_000_000L;
      }
    } else {
      // Uniform: 100MB
      for (int i = 0; i < diskUsages.length; i++) {
        diskUsages[i] = 100_000_000L;
      }
    }
    Map<TConsensusGroupId, RegionGroupStatistics> statsMap =
        CARMigratorTestHelper.buildStatisticsMap(allocatedResult, diskUsages);

    // All nodes available
    int[] allNodeIds = IntStream.rangeClosed(1, totalNodeCount).toArray();
    Map<Integer, TDataNodeConfiguration> availableNodeMap =
        CARMigratorTestHelper.buildNodeMap(allNodeIds);
    Set<Integer> allNodeIdSet = new HashSet<>();
    for (int id : allNodeIds) {
      allNodeIdSet.add(id);
    }

    // Metrics before
    Map<Integer, Integer> beforeRegionCounter =
        CARMigratorTestHelper.computeRegionCounter(allocatedResult, allNodeIdSet);
    Map<Integer, Long> beforeDiskCounter =
        CARMigratorTestHelper.computeDiskCounter(allocatedResult, statsMap, allNodeIdSet);
    long beforeVarRegion = CARMigratorTestHelper.computeVariance(beforeRegionCounter);
    long beforeVarDisk =
        CARMigratorTestHelper.computeVariance(
            beforeDiskCounter, CARMigratorTestHelper.DISK_SCALE_FACTOR);
    LOGGER.info("[{}] Before: regionCounter={}", migratorName, beforeRegionCounter);
    LOGGER.info(
        "[{}] Before: diskCounter={}",
        migratorName,
        CARMigratorTestHelper.diskCounterToMB(beforeDiskCounter));
    LOGGER.info(
        "[{}] Before: Var(region)={}, Var(disk)={}", migratorName, beforeVarRegion, beforeVarDisk);

    // Execute bidirectional migration (empty targetNodeIds)
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        migrator.autoBalanceRegionReplicasDistribution(
            availableNodeMap, statsMap, allocatedResult, replicaCount, Collections.emptyList());

    // Metrics after
    Map<Integer, Integer> afterRegionCounter =
        CARMigratorTestHelper.computeRegionCounter(migrationPlan.values(), allNodeIdSet);
    Map<Integer, Long> afterDiskCounter =
        CARMigratorTestHelper.computeDiskCounter(migrationPlan.values(), statsMap, allNodeIdSet);
    long afterVarRegion = CARMigratorTestHelper.computeVariance(afterRegionCounter);
    long afterVarDisk =
        CARMigratorTestHelper.computeVariance(
            afterDiskCounter, CARMigratorTestHelper.DISK_SCALE_FACTOR);
    int migrations = CARMigratorTestHelper.countMigrations(allocatedResult, migrationPlan);
    long migrationCost =
        CARMigratorTestHelper.computeMigrationCost(allocatedResult, migrationPlan, statsMap);
    LOGGER.info("[{}] After: regionCounter={}", migratorName, afterRegionCounter);
    LOGGER.info(
        "[{}] After: diskCounter={}",
        migratorName,
        CARMigratorTestHelper.diskCounterToMB(afterDiskCounter));
    LOGGER.info(
        "[{}] After: Var(region)={}, Var(disk)={}, migrations={}, migrationCost={}MB",
        migratorName,
        afterVarRegion,
        afterVarDisk,
        migrations,
        migrationCost / CARMigratorTestHelper.DISK_SCALE_FACTOR);

    // Common assertions (both CAR and Greedy)
    CARMigratorTestHelper.assertReplicaConstraint(migrationPlan, replicaCount);
    CARMigratorTestHelper.assertNotWorse(beforeVarDisk, afterVarDisk, "Var(disk)");

    // New nodes (initialNodeCount+1 ~ totalNodeCount) should have regions
    for (int nodeId = initialNodeCount + 1; nodeId <= totalNodeCount; nodeId++) {
      Assert.assertTrue(
          "["
              + migratorName
              + "] New node "
              + nodeId
              + " should have regions after bidirectional balance",
          afterRegionCounter.getOrDefault(nodeId, 0) > 0);
    }

    // CAR-only strict assertion: region count must be within [idealFloor, idealCeil]
    if ("CAR".equals(migratorName)) {
      CARMigratorTestHelper.assertNotWorse(beforeVarRegion, afterVarRegion, "Var(region)");
      int totalReplicas = regionCount * replicaCount;
      int idealFloor = totalReplicas / totalNodeCount;
      int idealCeil = idealFloor + (totalReplicas % totalNodeCount == 0 ? 0 : 1);
      for (int nodeId : allNodeIds) {
        int count = afterRegionCounter.getOrDefault(nodeId, 0);
        Assert.assertTrue(
            "[CAR] Node "
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
  }
}
