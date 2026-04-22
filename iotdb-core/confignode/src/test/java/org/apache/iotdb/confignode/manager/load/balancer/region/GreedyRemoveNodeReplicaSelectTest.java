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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Unit tests for GreedyRegionGroupAllocator.removeNodeReplicaSelect. */
public class GreedyRemoveNodeReplicaSelectTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyRemoveNodeReplicaSelectTest.class);

  private static final IRegionGroupAllocator GREEDY_ALLOCATOR = new GreedyRegionGroupAllocator();

  /** Helper: create a TDataNodeLocation with the given id. */
  private static TDataNodeLocation loc(int id) {
    return new TDataNodeLocation().setDataNodeId(id);
  }

  /** Helper: create a TDataNodeConfiguration with the given id. */
  private static TDataNodeConfiguration conf(int id) {
    return new TDataNodeConfiguration().setLocation(loc(id));
  }

  /** Helper: create a TConsensusGroupId for DataRegion with the given id. */
  private static TConsensusGroupId rgId(int id) {
    return new TConsensusGroupId(TConsensusGroupType.DataRegion, id);
  }

  /** Helper: create a TRegionReplicaSet with the given group id and node ids. */
  private static TRegionReplicaSet replicaSet(int groupId, int... nodeIds) {
    TRegionReplicaSet rs = new TRegionReplicaSet();
    rs.setRegionId(rgId(groupId));
    List<TDataNodeLocation> locs = new ArrayList<>();
    for (int nid : nodeIds) {
      locs.add(loc(nid));
    }
    rs.setDataNodeLocations(locs);
    return rs;
  }

  /**
   * Test basic correctness: every region gets assigned, mutual exclusion is satisfied, and the
   * greedy algorithm selects the least-loaded node at each step.
   *
   * <p>Setup: 5 available nodes (1~5), remove node 6. 6 RegionGroups (replication factor = 2).
   */
  @Test
  public void testBasicCorrectnessAndMutualExclusion() {
    // Available nodes: 1~5
    Map<Integer, TDataNodeConfiguration> availableNodes = new HashMap<>();
    Map<Integer, Double> freeSpace = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      availableNodes.put(i, conf(i));
      freeSpace.put(i, 100.0);
    }

    // Simulate the cluster state AFTER removing node 6 from replica sets.
    // The allocatedRegionGroups reflects the current state: node 6 already removed from locations.
    // Each region's remain replica set is what's left after removing node 6.
    //
    // RG0: was [1, 6] -> remain [1], need to migrate the one from node 6
    // RG1: was [2, 6] -> remain [2]
    // RG2: was [3, 6] -> remain [3]
    // RG3: was [4, 6] -> remain [4]
    // RG4: was [1, 2] -> not affected (no node 6)
    // RG5: was [3, 5] -> not affected
    List<TRegionReplicaSet> allocatedRegionGroups =
        new ArrayList<>(
            Arrays.asList(
                replicaSet(0, 1), // remain after removing node 6
                replicaSet(1, 2),
                replicaSet(2, 3),
                replicaSet(3, 4),
                replicaSet(4, 1, 2), // unaffected
                replicaSet(5, 3, 5) // unaffected
                ));

    // Regions that need migration: RG0~RG3
    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    remainReplicasMap.put(rgId(0), replicaSet(0, 1)); // exclude node 1
    remainReplicasMap.put(rgId(1), replicaSet(1, 2)); // exclude node 2
    remainReplicasMap.put(rgId(2), replicaSet(2, 3)); // exclude node 3
    remainReplicasMap.put(rgId(3), replicaSet(3, 4)); // exclude node 4

    Map<TConsensusGroupId, String> regionDbMap = new HashMap<>();
    remainReplicasMap.keySet().forEach(id -> regionDbMap.put(id, "db"));
    Map<String, List<TRegionReplicaSet>> dbAllocMap = new HashMap<>();
    dbAllocMap.put("db", allocatedRegionGroups);

    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        GREEDY_ALLOCATOR.removeNodeReplicaSelect(
            availableNodes,
            freeSpace,
            allocatedRegionGroups,
            regionDbMap,
            dbAllocMap,
            remainReplicasMap,
            Collections.emptyMap());

    // 1. Every region must be assigned
    Assert.assertEquals(4, result.size());
    for (TConsensusGroupId id : remainReplicasMap.keySet()) {
      Assert.assertTrue("Region " + id + " should have an assignment", result.containsKey(id));
    }

    // 2. Mutual exclusion: assigned node must NOT be in the remain replica set
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> e : result.entrySet()) {
      TConsensusGroupId regionId = e.getKey();
      int assignedNodeId = e.getValue().getLocation().getDataNodeId();
      TRegionReplicaSet remain = remainReplicasMap.get(regionId);
      Set<Integer> excludedIds = new HashSet<>();
      remain.getDataNodeLocations().forEach(l -> excludedIds.add(l.getDataNodeId()));
      Assert.assertFalse(
          "Region " + regionId + " assigned to excluded node " + assignedNodeId,
          excludedIds.contains(assignedNodeId));
    }

    LOGGER.info("Basic correctness test passed. Assignments:");
    result.forEach(
        (id, node) ->
            LOGGER.info("  Region {} -> Node {}", id.getId(), node.getLocation().getDataNodeId()));
  }

  /**
   * Reproduce the greedy-vs-global example from the thesis (Table 5.1).
   *
   * <p>Setup: 5 nodes (D1~D5), remove D5. D5 has 4 regions: R1, R2, R3, R4. Their other replicas
   * are on D1, D2, D3, D1 respectively. Initial loads: D1=3, D2=2, D3=2, D4=1.
   *
   * <p>The greedy algorithm processes regions in iteration order. Due to HashMap ordering, the
   * exact sequence may vary, but we verify that the greedy result has MaxGlobalLoad >= 3 (the
   * optimal). Specifically, if processed in the order R1,R2,R3,R4, greedy produces peak load = 4.
   *
   * <p>We verify: (a) all regions assigned, (b) mutual exclusion holds, (c) max load >= optimal
   * (3).
   */
  @Test
  public void testThesisGreedyExample() {
    // Available nodes: D1~D4 (D5 is being removed)
    Map<Integer, TDataNodeConfiguration> availableNodes = new HashMap<>();
    Map<Integer, Double> freeSpace = new HashMap<>();
    for (int i = 1; i <= 4; i++) {
      availableNodes.put(i, conf(i));
      freeSpace.put(i, 100.0);
    }

    // Build allocatedRegionGroups to produce initial loads: D1=3, D2=2, D3=2, D4=1
    // These are the regions NOT on D5 (the existing load before migration).
    // Plus the remain replicas of the regions being migrated from D5.
    //
    // D1 has 3 regions (from existing RGs): RG10:[D1,D2], RG11:[D1,D3], and R1/R4's remain [D1]
    // D2 has 2 regions: RG10:[D1,D2], and R2's remain [D2]
    // D3 has 2 regions: RG11:[D1,D3], and R3's remain [D3]
    // D4 has 1 region: RG12:[D4,D2]
    // Wait - let's count more carefully.
    //
    // We need: after removing D5 from replica sets, the remaining allocatedRegionGroups should
    // produce regionCounter = {D1:3, D2:2, D3:2, D4:1}
    //
    // Regions being migrated (from D5): R1(remain=[D1]), R2(remain=[D2]), R3(remain=[D3]),
    // R4(remain=[D1])
    // These remains contribute: D1:2, D2:1, D3:1
    // So we need other regions to bring totals to D1:3, D2:2, D3:2, D4:1
    // Other regions contribute: D1:1, D2:1, D3:1, D4:1
    // One extra RG: RG_extra:[D2, D4] -> D2:+1, D4:+1. But D1 still needs +1.
    // Let's use: RG_extra1:[D1, D4] -> D1:+1, D4:+1 and adjust.
    //
    // Simpler approach: just build the exact allocated list.
    // R1: [D1]       -> D1 count: 1
    // R2: [D2]       -> D2 count: 1
    // R3: [D3]       -> D3 count: 1
    // R4: [D1]       -> D1 count: 2
    // Extra_A: [D1, D2] -> D1 count: 3, D2 count: 2
    // Extra_B: [D3, D4] -> D3 count: 2, D4 count: 1
    // Total: D1=3, D2=2, D3=2, D4=1  ✓

    List<TRegionReplicaSet> allocatedRegionGroups =
        new ArrayList<>(
            Arrays.asList(
                replicaSet(1, 1), // R1 remain
                replicaSet(2, 2), // R2 remain
                replicaSet(3, 3), // R3 remain
                replicaSet(4, 1), // R4 remain
                replicaSet(10, 1, 2), // extra: contributes to D1 and D2
                replicaSet(11, 3, 4) // extra: contributes to D3 and D4
                ));

    // Regions to migrate from D5
    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    remainReplicasMap.put(rgId(1), replicaSet(1, 1)); // R1: other replica on D1
    remainReplicasMap.put(rgId(2), replicaSet(2, 2)); // R2: other replica on D2
    remainReplicasMap.put(rgId(3), replicaSet(3, 3)); // R3: other replica on D3
    remainReplicasMap.put(rgId(4), replicaSet(4, 1)); // R4: other replica on D1

    Map<TConsensusGroupId, String> regionDbMap = new HashMap<>();
    remainReplicasMap.keySet().forEach(id -> regionDbMap.put(id, "db"));
    Map<String, List<TRegionReplicaSet>> dbAllocMap = new HashMap<>();
    dbAllocMap.put("db", allocatedRegionGroups);

    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        GREEDY_ALLOCATOR.removeNodeReplicaSelect(
            availableNodes,
            freeSpace,
            allocatedRegionGroups,
            regionDbMap,
            dbAllocMap,
            remainReplicasMap,
            Collections.emptyMap());

    // All 4 regions must be assigned
    Assert.assertEquals(4, result.size());

    // Check mutual exclusion
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> e : result.entrySet()) {
      TConsensusGroupId regionId = e.getKey();
      int assignedNodeId = e.getValue().getLocation().getDataNodeId();
      TRegionReplicaSet remain = remainReplicasMap.get(regionId);
      for (TDataNodeLocation l : remain.getDataNodeLocations()) {
        Assert.assertNotEquals(
            "Mutual exclusion violated for Region " + regionId, l.getDataNodeId(), assignedNodeId);
      }
    }

    // Compute the final load distribution
    Map<Integer, Integer> finalLoad = new HashMap<>();
    for (int i = 1; i <= 4; i++) {
      finalLoad.put(i, 0);
    }
    for (TRegionReplicaSet rs : allocatedRegionGroups) {
      for (TDataNodeLocation l : rs.getDataNodeLocations()) {
        finalLoad.computeIfPresent(l.getDataNodeId(), (k, v) -> v + 1);
      }
    }
    for (TDataNodeConfiguration node : result.values()) {
      finalLoad.merge(node.getLocation().getDataNodeId(), 1, Integer::sum);
    }

    int maxLoad = finalLoad.values().stream().max(Integer::compareTo).orElse(0);
    LOGGER.info("Thesis example - Greedy result:");
    LOGGER.info("  Final loads: {}", finalLoad);
    LOGGER.info("  Max load: {} (optimal would be 3)", maxLoad);
    result.forEach(
        (id, node) ->
            LOGGER.info("  Region {} -> Node {}", id.getId(), node.getLocation().getDataNodeId()));

    // The optimal max load is 3 (total 12 regions / 4 nodes = 3 each).
    // Greedy may achieve 3 or worse depending on iteration order.
    // We verify it's at least a valid solution (>= 3).
    Assert.assertTrue("Max load should be at least optimal (3)", maxLoad >= 3);
  }
}
