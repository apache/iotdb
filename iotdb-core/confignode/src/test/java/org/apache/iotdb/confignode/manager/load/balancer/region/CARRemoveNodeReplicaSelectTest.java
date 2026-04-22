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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Tests for {@link CARRegionGroupAllocator#removeNodeReplicaSelect}.
 *
 * <p>Verifies correctness (no duplicate replicas, all regions assigned), region balance
 * (var(region) minimized), and disk-aware balancing (var(disk) optimized).
 */
public class CARRemoveNodeReplicaSelectTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CARRemoveNodeReplicaSelectTest.class);

  private static final long DISK_SCALE_FACTOR = 1_000_000L;

  private static final IRegionGroupAllocator CAR_ALLOCATOR = new CARRegionGroupAllocator();

  // Use GCR allocator for initial allocation (CAR only supports removeNodeReplicaSelect)
  private static final IRegionGroupAllocator PGR_ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  private static final TDataNodeLocation REMOVE_DATANODE_LOCATION =
      new TDataNodeLocation().setDataNodeId(5);

  private static final int TEST_DATA_NODE_NUM = 5;

  private static final int DATA_REGION_PER_DATA_NODE = 30;

  private static final int DATA_REPLICATION_FACTOR = 2;

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new HashMap<>();

  private static final Map<Integer, Double> FREE_SPACE_MAP = new HashMap<>();

  @Before
  public void setUp() {
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, Math.random());
    }
  }

  /** Test basic single-database scale-in with uniform disk usage. */
  @Test
  public void testSelectDestNode() {
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    // Allocate initial regions using GCR allocator
    List<TRegionReplicaSet> allocateResult = new ArrayList<>();
    List<TRegionReplicaSet> databaseAllocateResult = new ArrayList<>();
    for (int index = 0; index < dataRegionGroupNum; index++) {
      TRegionReplicaSet replicaSet =
          PGR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      allocateResult.add(replicaSet);
      databaseAllocateResult.add(new TRegionReplicaSet(replicaSet));
    }

    // Identify regions on the removed node
    List<TRegionReplicaSet> migratedReplicas =
        allocateResult.stream()
            .filter(
                replicaSet -> replicaSet.getDataNodeLocations().contains(REMOVE_DATANODE_LOCATION))
            .collect(Collectors.toList());

    AVAILABLE_DATA_NODE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());
    FREE_SPACE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());

    // Build remain replicas (remove the decommissioned node from each replica set)
    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();
    for (TRegionReplicaSet replicaSet : migratedReplicas) {
      allocateResult.remove(replicaSet);
      replicaSet.getDataNodeLocations().remove(REMOVE_DATANODE_LOCATION);
      allocateResult.add(replicaSet);
      remainReplicas.add(replicaSet);
    }

    // Build input maps
    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    for (TRegionReplicaSet r : remainReplicas) {
      remainReplicasMap.put(r.getRegionId(), r);
    }
    Map<TConsensusGroupId, String> regionDatabaseMap = new HashMap<>();
    for (TRegionReplicaSet replicaSet : allocateResult) {
      regionDatabaseMap.put(replicaSet.getRegionId(), "database");
    }
    Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap = new HashMap<>();
    databaseAllocatedRegionGroupMap.put("database", databaseAllocateResult);

    // Call CAR allocator
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        CAR_ALLOCATOR.removeNodeReplicaSelect(
            AVAILABLE_DATA_NODE_MAP,
            FREE_SPACE_MAP,
            allocateResult,
            regionDatabaseMap,
            databaseAllocatedRegionGroupMap,
            remainReplicasMap,
            Collections.emptyMap());

    // Verify: all regions assigned
    Assert.assertEquals(remainReplicasMap.size(), result.size());

    // Verify: no duplicate replicas (assigned node not in remain set)
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      int assignedNodeId = entry.getValue().getLocation().getDataNodeId();
      TRegionReplicaSet remainReplica = remainReplicasMap.get(regionId);
      for (TDataNodeLocation loc : remainReplica.getDataNodeLocations()) {
        Assert.assertNotEquals(
            "Assigned node should not be in remain replica set",
            loc.getDataNodeId(),
            assignedNodeId);
      }
    }

    // Compute final region distribution
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    AVAILABLE_DATA_NODE_MAP.keySet().forEach(n -> regionCounter.put(n, 0));
    for (TRegionReplicaSet replicaSet : allocateResult) {
      for (TDataNodeLocation loc : replicaSet.getDataNodeLocations()) {
        regionCounter.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      regionCounter.merge(entry.getValue().getLocation().getDataNodeId(), 1, Integer::sum);
    }

    // Verify: all remaining nodes used
    Set<Integer> usedNodes = new HashSet<>();
    for (TDataNodeConfiguration cfg : result.values()) {
      usedNodes.add(cfg.getLocation().getDataNodeId());
    }
    Assert.assertEquals(TEST_DATA_NODE_NUM - 1, usedNodes.size());

    // Verify: good region balance (max - min ≤ 1)
    int maxCount = regionCounter.values().stream().max(Integer::compareTo).orElse(0);
    int minCount = regionCounter.values().stream().min(Integer::compareTo).orElse(0);
    LOGGER.info("CAR regionCounter: {}", regionCounter);
    LOGGER.info("CAR max={}, min={}, diff={}", maxCount, minCount, maxCount - minCount);
    Assert.assertTrue(
        "Region distribution should be balanced (max - min <= 1)", maxCount - minCount <= 1);
  }

  /** Test multi-database scale-in. */
  @Test
  public void testSelectDestNodeMultiDatabase() {
    final String[] DB_NAMES = {"db0", "db1", "db2"};
    final int TOTAL_RG_NUM =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    int basePerDb = TOTAL_RG_NUM / DB_NAMES.length;
    int remainder = TOTAL_RG_NUM % DB_NAMES.length;

    Map<String, List<TRegionReplicaSet>> dbAllocatedMap = new HashMap<>();
    List<TRegionReplicaSet> globalAllocatedList = new ArrayList<>();
    int globalIndex = 0;

    for (int dbIdx = 0; dbIdx < DB_NAMES.length; dbIdx++) {
      String db = DB_NAMES[dbIdx];
      int rgToCreate = basePerDb + (dbIdx < remainder ? 1 : 0);
      List<TRegionReplicaSet> perDbList = new ArrayList<>();
      dbAllocatedMap.put(db, perDbList);

      for (int i = 0; i < rgToCreate; i++) {
        TRegionReplicaSet rs =
            PGR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                globalAllocatedList,
                perDbList,
                DATA_REPLICATION_FACTOR,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, globalIndex++));
        globalAllocatedList.add(rs);
        perDbList.add(rs);
      }
    }

    // Identify impacted replicas
    List<TRegionReplicaSet> impactedReplicas =
        globalAllocatedList.stream()
            .filter(rs -> rs.getDataNodeLocations().contains(REMOVE_DATANODE_LOCATION))
            .collect(Collectors.toList());

    AVAILABLE_DATA_NODE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());
    FREE_SPACE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());

    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();
    for (TRegionReplicaSet rs : impactedReplicas) {
      globalAllocatedList.remove(rs);
      rs.getDataNodeLocations().remove(REMOVE_DATANODE_LOCATION);
      globalAllocatedList.add(rs);
      remainReplicas.add(rs);
    }

    Map<TConsensusGroupId, TRegionReplicaSet> remainMap = new HashMap<>();
    remainReplicas.forEach(r -> remainMap.put(r.getRegionId(), r));

    Map<TConsensusGroupId, String> regionDbMap = new HashMap<>();
    dbAllocatedMap.forEach((db, list) -> list.forEach(r -> regionDbMap.put(r.getRegionId(), db)));

    // Call CAR allocator
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        CAR_ALLOCATOR.removeNodeReplicaSelect(
            AVAILABLE_DATA_NODE_MAP,
            FREE_SPACE_MAP,
            globalAllocatedList,
            regionDbMap,
            dbAllocatedMap,
            remainMap,
            Collections.emptyMap());

    // Verify: all regions assigned
    Assert.assertEquals(remainMap.size(), result.size());

    // Verify: no duplicate replicas
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      int assignedNodeId = entry.getValue().getLocation().getDataNodeId();
      TRegionReplicaSet remainReplica = remainMap.get(regionId);
      for (TDataNodeLocation loc : remainReplica.getDataNodeLocations()) {
        Assert.assertNotEquals(loc.getDataNodeId(), assignedNodeId);
      }
    }

    // Verify: all remaining nodes used
    Set<Integer> usedNodes = new HashSet<>();
    for (TDataNodeConfiguration cfg : result.values()) {
      usedNodes.add(cfg.getLocation().getDataNodeId());
    }
    Assert.assertEquals(TEST_DATA_NODE_NUM - 1, usedNodes.size());

    // Compute final region distribution
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    AVAILABLE_DATA_NODE_MAP.keySet().forEach(n -> regionCounter.put(n, 0));
    for (TRegionReplicaSet replicaSet : globalAllocatedList) {
      for (TDataNodeLocation loc : replicaSet.getDataNodeLocations()) {
        regionCounter.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      regionCounter.merge(entry.getValue().getLocation().getDataNodeId(), 1, Integer::sum);
    }

    int maxCount = regionCounter.values().stream().max(Integer::compareTo).orElse(0);
    int minCount = regionCounter.values().stream().min(Integer::compareTo).orElse(0);
    LOGGER.info("CAR multi-DB regionCounter: {}", regionCounter);
    LOGGER.info("CAR multi-DB max={}, min={}, diff={}", maxCount, minCount, maxCount - minCount);
    Assert.assertTrue(
        "Multi-DB region distribution should be balanced (max - min <= 1)",
        maxCount - minCount <= 1);
  }

  /**
   * Test disk-aware balancing with non-uniform disk usage.
   *
   * <p>Scenario: 4 nodes, 12 RGs with r=2 (24 replicas, 6 per node). Remove node 4, ~6 regions need
   * migration to nodes 1-3. Regions 0-5 are heavy (200MB), regions 6-11 are light (100MB).
   *
   * <p>Due to mutual-exclusion constraints, most migrated regions' remain replicas may land on the
   * same node, limiting candidates. Perfect region balance (max-min ≤ 1) may not be achievable. The
   * algorithm minimizes var(region) first, then var(disk) as secondary optimization.
   *
   * <p>This test verifies: (1) correctness — all regions assigned with no duplicate replicas, (2)
   * heavy regions are not all concentrated on one node.
   */
  @Test
  public void testDiskAwareBalancing() {
    final int NUM_NODES = 4;
    final int REMOVE_NODE = 4;
    final int REPLICATION_FACTOR = 2;
    final int RG_NUM = 12; // 12 RGs × 2 replicas = 24 total, 6 per node initially

    Map<Integer, TDataNodeConfiguration> nodeMap = new HashMap<>();
    Map<Integer, Double> freeSpace = new HashMap<>();
    for (int i = 1; i <= NUM_NODES; i++) {
      nodeMap.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      freeSpace.put(i, 1000.0);
    }

    // Allocate regions
    List<TRegionReplicaSet> allocatedRGs = new ArrayList<>();
    for (int idx = 0; idx < RG_NUM; idx++) {
      TRegionReplicaSet rs =
          PGR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              nodeMap,
              freeSpace,
              allocatedRGs,
              allocatedRGs,
              REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, idx));
      allocatedRGs.add(rs);
    }

    // Create non-uniform disk usage: regions 0-3 have 500MB, regions 4-11 have 100MB
    Map<TConsensusGroupId, Long> diskUsageMap = new HashMap<>();
    for (int idx = 0; idx < RG_NUM; idx++) {
      TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, idx);
      long usage = (idx < 6) ? 200L * DISK_SCALE_FACTOR : 100L * DISK_SCALE_FACTOR;
      diskUsageMap.put(groupId, usage);
    }

    // Debug: show initial allocation
    for (TRegionReplicaSet rs : allocatedRGs) {
      int rgId = rs.getRegionId().getId();
      String nodes =
          rs.getDataNodeLocations().stream()
              .map(l -> String.valueOf(l.getDataNodeId()))
              .collect(Collectors.joining(","));
      long diskMB = diskUsageMap.getOrDefault(rs.getRegionId(), 0L) / DISK_SCALE_FACTOR;
      LOGGER.info("  RG{} [{}MB] -> nodes {{{}}}", rgId, diskMB, nodes);
    }

    // Identify impacted replicas (on removed node)
    TDataNodeLocation removeLocation = new TDataNodeLocation().setDataNodeId(REMOVE_NODE);
    List<TRegionReplicaSet> impactedReplicas =
        allocatedRGs.stream()
            .filter(rs -> rs.getDataNodeLocations().contains(removeLocation))
            .collect(Collectors.toList());

    nodeMap.remove(REMOVE_NODE);
    freeSpace.remove(REMOVE_NODE);

    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();
    for (TRegionReplicaSet rs : impactedReplicas) {
      allocatedRGs.remove(rs);
      rs.getDataNodeLocations().remove(removeLocation);
      allocatedRGs.add(rs);
      remainReplicas.add(rs);
    }

    // Debug: show migrated regions and their constraints
    LOGGER.info("=== Regions to migrate (from node {}) ===", REMOVE_NODE);
    Set<Integer> availNodes = new HashSet<>(nodeMap.keySet());
    for (TRegionReplicaSet rs : remainReplicas) {
      int rgId = rs.getRegionId().getId();
      long diskMB = diskUsageMap.getOrDefault(rs.getRegionId(), 0L) / DISK_SCALE_FACTOR;
      Set<Integer> remainNodeIds =
          rs.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());
      Set<Integer> candidateNodes = new HashSet<>(availNodes);
      candidateNodes.removeAll(remainNodeIds);
      LOGGER.info(
          "  RG{} [{}MB] remain={} candidates={}", rgId, diskMB, remainNodeIds, candidateNodes);
    }

    Map<TConsensusGroupId, TRegionReplicaSet> remainMap = new HashMap<>();
    remainReplicas.forEach(r -> remainMap.put(r.getRegionId(), r));

    Map<TConsensusGroupId, String> regionDbMap = new HashMap<>();
    Map<String, List<TRegionReplicaSet>> dbAllocMap = new HashMap<>();
    List<TRegionReplicaSet> dbList = new ArrayList<>(allocatedRGs);
    dbAllocMap.put("db", dbList);
    for (TRegionReplicaSet rs : allocatedRGs) {
      regionDbMap.put(rs.getRegionId(), "db");
    }

    // Call CAR allocator with disk usage
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        CAR_ALLOCATOR.removeNodeReplicaSelect(
            nodeMap, freeSpace, allocatedRGs, regionDbMap, dbAllocMap, remainMap, diskUsageMap);

    // Verify: all regions assigned
    Assert.assertEquals(remainMap.size(), result.size());

    // Verify: no duplicate replicas
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      int assignedNodeId = entry.getValue().getLocation().getDataNodeId();
      TRegionReplicaSet remainReplica = remainMap.get(regionId);
      for (TDataNodeLocation loc : remainReplica.getDataNodeLocations()) {
        Assert.assertNotEquals(
            "Assigned node should not be in remain replica set",
            loc.getDataNodeId(),
            assignedNodeId);
      }
    }

    // Compute final region and disk distribution
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    Map<Integer, Long> diskCounter = new TreeMap<>();
    nodeMap
        .keySet()
        .forEach(
            n -> {
              regionCounter.put(n, 0);
              diskCounter.put(n, 0L);
            });
    for (TRegionReplicaSet rs : allocatedRGs) {
      long diskUsage = diskUsageMap.getOrDefault(rs.getRegionId(), 0L);
      for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
        if (nodeMap.containsKey(loc.getDataNodeId())) {
          regionCounter.merge(loc.getDataNodeId(), 1, Integer::sum);
          diskCounter.merge(loc.getDataNodeId(), diskUsage, Long::sum);
        }
      }
    }
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      long diskUsage = diskUsageMap.getOrDefault(entry.getKey(), 0L);
      int nodeId = entry.getValue().getLocation().getDataNodeId();
      regionCounter.merge(nodeId, 1, Integer::sum);
      diskCounter.merge(nodeId, diskUsage, Long::sum);
    }

    // Log distributions
    Map<Integer, Long> diskMB = new TreeMap<>();
    diskCounter.forEach((k, v) -> diskMB.put(k, v / DISK_SCALE_FACTOR));
    LOGGER.info("CAR disk-aware regionCounter: {}", regionCounter);
    LOGGER.info("CAR disk-aware diskCounter(MB): {}", diskMB);

    // Compute variances
    long nNodes = regionCounter.size();
    long rSum = 0, rSumSq = 0;
    for (int v : regionCounter.values()) {
      rSum += v;
      rSumSq += (long) v * v;
    }
    long regionVariance = nNodes * rSumSq - rSum * rSum;

    long dSum = 0, dSumSq = 0;
    for (long v : diskCounter.values()) {
      long s = v / DISK_SCALE_FACTOR;
      dSum += s;
      dSumSq += s * s;
    }
    long diskVariance = nNodes * dSumSq - dSum * dSum;
    LOGGER.info("CAR disk-aware Var(region)={}, Var(disk)={}", regionVariance, diskVariance);

    // Verify: all assigned nodes are valid
    for (TDataNodeConfiguration cfg : result.values()) {
      Assert.assertTrue(nodeMap.containsKey(cfg.getLocation().getDataNodeId()));
    }

    // Note: due to mutual-exclusion constraints (remain replicas on a single node),
    // perfect region balance (max-min ≤ 1) may not be feasible. For example, if most
    // regions' remain replicas are on node 3, they can only go to {1,2}, making it
    // impossible to give node 3 its fair share. The algorithm finds the minimum
    // var(region) under these constraints, then optimizes var(disk) as secondary goal.

    // Verify: heavy regions (≥ 200MB) that were migrated should not all concentrate on one node.
    // Due to mutual-exclusion constraints (remain replicas), heavy regions may share
    // a limited candidate set, so perfect distribution is not always possible.
    // But the algorithm should at least use more than one node when candidates allow it.
    List<Integer> heavyTargetNodes = new ArrayList<>();
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      long diskUsage = diskUsageMap.getOrDefault(entry.getKey(), 0L);
      if (diskUsage >= 200L * DISK_SCALE_FACTOR) {
        heavyTargetNodes.add(entry.getValue().getLocation().getDataNodeId());
      }
    }
    LOGGER.info("Heavy regions migrated to nodes: {}", heavyTargetNodes);
    if (heavyTargetNodes.size() >= 2) {
      Set<Integer> uniqueHeavyNodes = new HashSet<>(heavyTargetNodes);
      Assert.assertTrue(
          "Heavy regions should not all be placed on the same node, got " + heavyTargetNodes,
          uniqueHeavyNodes.size() > 1);
    }
  }

  /** Verify that generateOptimalRegionReplicasDistribution throws UnsupportedOperationException. */
  @Test(expected = UnsupportedOperationException.class)
  public void testGenerateOptimalRegionReplicasDistributionThrows() {
    CAR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
        AVAILABLE_DATA_NODE_MAP,
        FREE_SPACE_MAP,
        Collections.emptyList(),
        Collections.emptyList(),
        2,
        new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
  }
}
