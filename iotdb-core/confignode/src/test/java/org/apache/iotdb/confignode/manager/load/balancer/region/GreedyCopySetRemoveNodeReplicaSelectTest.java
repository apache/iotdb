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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GreedyCopySetRemoveNodeReplicaSelectTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyCopySetRemoveNodeReplicaSelectTest.class);

  private static final IRegionGroupAllocator GCR_ALLOCATOR =
      new GreedyCopySetRegionGroupAllocator();

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
    // Construct TEST_DATA_NODE_NUM DataNodes
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, Math.random());
    }
  }

  @Test
  public void testSelectDestNode() {
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    List<TRegionReplicaSet> allocateResult = new ArrayList<>();
    List<TRegionReplicaSet> databaseAllocateResult = new ArrayList<>();
    for (int index = 0; index < dataRegionGroupNum; index++) {
      TRegionReplicaSet replicaSet =
          GCR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      TRegionReplicaSet replicaSetCopy = new TRegionReplicaSet(replicaSet);

      allocateResult.add(replicaSet);
      databaseAllocateResult.add(replicaSetCopy);
    }

    List<TRegionReplicaSet> migratedReplicas =
        allocateResult.stream()
            .filter(
                replicaSet -> replicaSet.getDataNodeLocations().contains(REMOVE_DATANODE_LOCATION))
            .collect(Collectors.toList());

    AVAILABLE_DATA_NODE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());
    FREE_SPACE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());

    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();
    for (TRegionReplicaSet replicaSet : migratedReplicas) {
      List<TDataNodeLocation> dataNodeLocations = replicaSet.getDataNodeLocations();
      allocateResult.remove(replicaSet);
      dataNodeLocations.remove(REMOVE_DATANODE_LOCATION);
      allocateResult.add(replicaSet);
      remainReplicas.add(replicaSet);
    }

    Map<Integer, Integer> randomRegionCounter = new HashMap<>();
    Map<Integer, Integer> PGPRegionCounter = new HashMap<>();
    Set<Integer> randomSelectedNodeIds = new HashSet<>();
    Set<Integer> PGPSelectedNodeIds = new HashSet<>();

    int randomMaxRegionCount = 0;
    int randomMinRegionCount = Integer.MAX_VALUE;
    int PGPMaxRegionCount = 0;
    int PGPMinRegionCount = Integer.MAX_VALUE;

    AVAILABLE_DATA_NODE_MAP
        .keySet()
        .forEach(
            nodeId -> {
              randomRegionCounter.put(nodeId, 0);
              PGPRegionCounter.put(nodeId, 0);
            });

    for (TRegionReplicaSet replicaSet : allocateResult) {
      for (TDataNodeLocation loc : replicaSet.getDataNodeLocations()) {
        randomRegionCounter.put(
            loc.getDataNodeId(), randomRegionCounter.get(loc.getDataNodeId()) + 1);
        PGPRegionCounter.put(loc.getDataNodeId(), PGPRegionCounter.get(loc.getDataNodeId()) + 1);
      }
    }

    for (TRegionReplicaSet remainReplicaSet : remainReplicas) {
      TDataNodeLocation selectedNode =
          randomSelectNodeForRegion(remainReplicaSet.getDataNodeLocations()).get();
      LOGGER.info(
          "Random Selected DataNode {} for Region {}",
          selectedNode.getDataNodeId(),
          remainReplicaSet.regionId);
      randomSelectedNodeIds.add(selectedNode.getDataNodeId());
      randomRegionCounter.put(
          selectedNode.getDataNodeId(), randomRegionCounter.get(selectedNode.getDataNodeId()) + 1);
    }

    LOGGER.info("Remain Replicas... :");
    for (TRegionReplicaSet remainReplicaSet : remainReplicas) {
      LOGGER.info("Region Group Id: {}", remainReplicaSet.regionId.id);
      List<TDataNodeLocation> dataNodeLocations = remainReplicaSet.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        LOGGER.info("DataNode: {}", dataNodeLocation.getDataNodeId());
      }
    }
    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap = new HashMap<>();
    databaseAllocatedRegionGroupMap.put("database", databaseAllocateResult);

    for (TRegionReplicaSet remainReplicaSet : remainReplicas) {
      remainReplicasMap.put(remainReplicaSet.getRegionId(), remainReplicaSet);
    }
    Map<TConsensusGroupId, String> regionDatabaseMap = new HashMap<>();
    for (TRegionReplicaSet replicaSet : allocateResult) {
      regionDatabaseMap.put(replicaSet.getRegionId(), "database");
    }
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        GCR_ALLOCATOR.removeNodeReplicaSelect(
            AVAILABLE_DATA_NODE_MAP,
            FREE_SPACE_MAP,
            allocateResult,
            regionDatabaseMap,
            databaseAllocatedRegionGroupMap,
            remainReplicasMap);

    for (TConsensusGroupId regionId : result.keySet()) {
      TDataNodeConfiguration selectedNode = result.get(regionId);

      LOGGER.info(
          "GCR Selected DataNode {} for Region {}",
          selectedNode.getLocation().getDataNodeId(),
          regionId);
      PGPSelectedNodeIds.add(selectedNode.getLocation().getDataNodeId());
      PGPRegionCounter.put(
          selectedNode.getLocation().getDataNodeId(),
          PGPRegionCounter.get(selectedNode.getLocation().getDataNodeId()) + 1);
    }

    LOGGER.info("randomRegionCount:");

    for (Integer i : randomRegionCounter.keySet()) {
      Integer value = randomRegionCounter.get(i);
      randomMaxRegionCount = Math.max(randomMaxRegionCount, value);
      randomMinRegionCount = Math.min(randomMinRegionCount, value);
      LOGGER.info("{} : {}", i, value);
    }

    LOGGER.info("PGPRegionCount:");

    for (Integer i : PGPRegionCounter.keySet()) {
      Integer value = PGPRegionCounter.get(i);
      PGPMaxRegionCount = Math.max(PGPMaxRegionCount, value);
      PGPMinRegionCount = Math.min(PGPMinRegionCount, value);
      LOGGER.info("{} : {}", i, value);
    }

    LOGGER.info("PGPSelectedNodeIds size: {}", PGPSelectedNodeIds.size());
    Assert.assertEquals(TEST_DATA_NODE_NUM - 1, PGPSelectedNodeIds.size());
    LOGGER.info("randomSelectedNodeIds size: {}", randomSelectedNodeIds.size());
    Assert.assertTrue(PGPSelectedNodeIds.size() >= randomSelectedNodeIds.size());
    LOGGER.info(
        "randomMaxRegionCount: {}, PGPMaxRegionCount: {}", randomMaxRegionCount, PGPMaxRegionCount);
    Assert.assertTrue(randomMaxRegionCount >= PGPMaxRegionCount);
  }

  @Test
  public void testSelectDestNodeMultiDatabase() {
    // Pre‑allocate RegionReplicaSets for multiple databases
    final String[] DB_NAMES = {"db0", "db1", "db2"};
    final int TOTAL_RG_NUM =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    int basePerDb = TOTAL_RG_NUM / DB_NAMES.length;
    int remainder = TOTAL_RG_NUM % DB_NAMES.length; // first <remainder> DBs get one extra

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
            GCR_ALLOCATOR.generateOptimalRegionReplicasDistribution(
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

    // Identify the replica‑sets that contain the node to be removed
    List<TRegionReplicaSet> impactedReplicas =
        globalAllocatedList.stream()
            .filter(rs -> rs.getDataNodeLocations().contains(REMOVE_DATANODE_LOCATION))
            .collect(Collectors.toList());

    // Simulate removing the faulty/offline node
    AVAILABLE_DATA_NODE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());
    FREE_SPACE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());

    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();
    for (TRegionReplicaSet rs : impactedReplicas) {
      globalAllocatedList.remove(rs);
      rs.getDataNodeLocations().remove(REMOVE_DATANODE_LOCATION);
      globalAllocatedList.add(rs);
      remainReplicas.add(rs);
    }

    // Build helper maps for removeNodeReplicaSelect
    Map<TConsensusGroupId, TRegionReplicaSet> remainMap = new HashMap<>();
    remainReplicas.forEach(r -> remainMap.put(r.getRegionId(), r));

    Map<TConsensusGroupId, String> regionDbMap = new HashMap<>();
    dbAllocatedMap.forEach((db, list) -> list.forEach(r -> regionDbMap.put(r.getRegionId(), db)));

    // Baseline: random selection for comparison
    Map<Integer, Integer> rndCount = new HashMap<>();
    Map<Integer, Integer> planCount = new HashMap<>();
    Set<Integer> rndNodes = new HashSet<>();
    Set<Integer> planNodes = new HashSet<>();
    int rndMax = 0, rndMin = Integer.MAX_VALUE;
    int planMax = 0, planMin = Integer.MAX_VALUE;

    AVAILABLE_DATA_NODE_MAP
        .keySet()
        .forEach(
            n -> {
              rndCount.put(n, 0);
              planCount.put(n, 0);
            });

    for (TRegionReplicaSet replicaSet : globalAllocatedList) {
      for (TDataNodeLocation loc : replicaSet.getDataNodeLocations()) {
        rndCount.merge(loc.getDataNodeId(), 1, Integer::sum);
        planCount.merge(loc.getDataNodeId(), 1, Integer::sum);
      }
    }

    for (TRegionReplicaSet r : remainReplicas) {
      TDataNodeLocation pick = randomSelectNodeForRegion(r.getDataNodeLocations()).get();
      LOGGER.info("Random Selected DataNode {} for Region {}", pick.getDataNodeId(), r.regionId);
      rndNodes.add(pick.getDataNodeId());
      rndCount.merge(pick.getDataNodeId(), 1, Integer::sum);
    }

    LOGGER.info("Remain Replicas... :");
    for (TRegionReplicaSet remainReplicaSet : remainReplicas) {
      LOGGER.info("Region Group Id: {}", remainReplicaSet.regionId.id);
      List<TDataNodeLocation> dataNodeLocations = remainReplicaSet.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        LOGGER.info("DataNode: {}", dataNodeLocation.getDataNodeId());
      }
    }

    // Call the method under test
    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        GCR_ALLOCATOR.removeNodeReplicaSelect(
            AVAILABLE_DATA_NODE_MAP,
            FREE_SPACE_MAP,
            globalAllocatedList,
            regionDbMap,
            dbAllocatedMap,
            remainMap);

    for (TConsensusGroupId regionId : result.keySet()) {
      TDataNodeConfiguration selectedNode = result.get(regionId);

      LOGGER.info(
          "GCR Selected DataNode {} for Region {}",
          selectedNode.getLocation().getDataNodeId(),
          regionId);
      planNodes.add(selectedNode.getLocation().getDataNodeId());
      planCount.merge(selectedNode.getLocation().getDataNodeId(), 1, Integer::sum);
    }

    // Calculate load distribution
    for (int c : rndCount.values()) {
      rndMax = Math.max(rndMax, c);
      rndMin = Math.min(rndMin, c);
    }
    for (int c : planCount.values()) {
      planMax = Math.max(planMax, c);
      planMin = Math.min(planMin, c);
    }

    // Assertions
    Assert.assertEquals(TEST_DATA_NODE_NUM - 1, planNodes.size());
    Assert.assertTrue(planNodes.size() >= rndNodes.size());
    Assert.assertTrue(rndMax >= planMax);
  }

  private Optional<TDataNodeLocation> randomSelectNodeForRegion(
      List<TDataNodeLocation> regionReplicaNodes) {
    List<TDataNodeConfiguration> dataNodeConfigurations =
        new ArrayList<>(AVAILABLE_DATA_NODE_MAP.values());
    // Randomly selected to ensure a basic load balancing
    Collections.shuffle(dataNodeConfigurations);
    return dataNodeConfigurations.stream()
        .map(TDataNodeConfiguration::getLocation)
        .filter(e -> !regionReplicaNodes.contains(e))
        .findAny();
  }
}
