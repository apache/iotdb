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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Random allocator for scale-in experimental comparison.
 *
 * <p>For each region that needs migration, randomly selects an available node (excluding nodes
 * already holding a replica of that region). This serves as a baseline to demonstrate the
 * effectiveness of the GCR algorithm's multi-objective optimization.
 *
 * <p>Expected result: poor load balance quality (high variance in region distribution) compared to
 * GCR, since no load-awareness is applied.
 */
public class RandomRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(RandomRegionGroupAllocator.class);

  private static final SecureRandom RANDOM = new SecureRandom();

  /** Number of rounds to run and pick the worst-variance result for experimental comparison. */
  private static final int MAX_RETRY = 1000;

  private static final GreedyRegionGroupAllocator GREEDY_ALLOCATOR =
      new GreedyRegionGroupAllocator();

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Random allocator is only used for scale-in; delegate initial allocation to Greedy
    return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
        availableDataNodeMap,
        freeDiskSpaceMap,
        allocatedRegionGroups,
        databaseAllocatedRegionGroups,
        replicationFactor,
        consensusGroupId);
  }

  @Override
  public Map<TConsensusGroupId, TDataNodeConfiguration> removeNodeReplicaSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap,
      Map<TConsensusGroupId, Long> regionDiskUsageMap) {

    ScaleInAllocatorLogger.logSummary(
        LOGGER,
        "Random",
        "Initial",
        availableDataNodeMap,
        allocatedRegionGroups,
        null,
        regionDatabaseMap,
        remainReplicasMap,
        regionDiskUsageMap,
        ScaleInAllocatorLogger.inferRemovedNodeMap(
            availableDataNodeMap,
            regionDatabaseMap,
            databaseAllocatedRegionGroupMap,
            remainReplicasMap));

    // Run multiple rounds and pick the result with the highest variance, so that the log
    // can demonstrate the random algorithm's load imbalance.
    Map<TConsensusGroupId, TDataNodeConfiguration> bestResult = null;
    long bestVariance = -1;

    for (int round = 0; round < MAX_RETRY; round++) {
      Map<TConsensusGroupId, TDataNodeConfiguration> candidateResult =
          doRandomSelect(availableDataNodeMap, remainReplicasMap);

      long variance =
          ScaleInAllocatorLogger.computeResultVariance(
              candidateResult, allocatedRegionGroups, availableDataNodeMap);
      if (variance > bestVariance) {
        bestVariance = variance;
        bestResult = candidateResult;
      }
    }

    ScaleInAllocatorLogger.logSummary(
        LOGGER,
        "Random",
        "Final",
        availableDataNodeMap,
        allocatedRegionGroups,
        bestResult,
        regionDatabaseMap,
        remainReplicasMap,
        regionDiskUsageMap,
        null);

    return bestResult;
  }

  /**
   * Single round of random selection: for each region, randomly pick one available node (excluding
   * nodes already holding a replica).
   */
  private Map<TConsensusGroupId, TDataNodeConfiguration> doRandomSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {

    Map<TConsensusGroupId, TDataNodeConfiguration> result = new HashMap<>();

    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> entry : remainReplicasMap.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      TRegionReplicaSet remainReplicaSet = entry.getValue();

      // Build the exclusion set: nodes already holding a replica of this region group
      Set<Integer> excludedNodes =
          remainReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toSet());

      // Build candidate list: available nodes not in the exclusion set
      List<Integer> candidates =
          availableDataNodeMap.keySet().stream()
              .filter(nodeId -> !excludedNodes.contains(nodeId))
              .collect(Collectors.toList());

      if (!candidates.isEmpty()) {
        // Randomly pick one candidate
        int selectedNodeId = candidates.get(RANDOM.nextInt(candidates.size()));
        result.put(regionId, availableDataNodeMap.get(selectedNodeId));
      }
    }

    return result;
  }
}
