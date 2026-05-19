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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Allocate Region Greedily */
public class GreedyRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(GreedyRegionGroupAllocator.class);

  private static final SecureRandom RANDOM = new SecureRandom();

  /** Number of rounds to run and pick the worst-variance result for experimental comparison. */
  private static final int MAX_RETRY = 1000;

  public GreedyRegionGroupAllocator() {
    // Empty constructor
  }

  static class DataNodeEntry implements Comparable<DataNodeEntry> {

    public int dataNodeId;
    public int regionCount;
    public int databaseRegionCount;
    public double freeDiskSpace;
    public int randomWeight;

    public DataNodeEntry(
        int dataNodeId, int regionCount, int databaseRegionCount, double freeDiskSpace) {
      this.dataNodeId = dataNodeId;
      this.regionCount = regionCount;
      this.databaseRegionCount = databaseRegionCount;
      this.freeDiskSpace = freeDiskSpace;
      this.randomWeight = RANDOM.nextInt();
    }

    @Override
    public int compareTo(DataNodeEntry other) {
      if (this.regionCount != other.regionCount) {
        return Integer.compare(this.regionCount, other.regionCount);
      }
      if (this.databaseRegionCount != other.databaseRegionCount) {
        return Integer.compare(this.databaseRegionCount, other.databaseRegionCount);
      }
      if (this.freeDiskSpace != other.freeDiskSpace) {
        return Double.compare(other.freeDiskSpace, this.freeDiskSpace);
      }
      return Integer.compare(this.randomWeight, other.randomWeight);
    }
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Build weightList ordered by (regionCount asc, databaseRegionCount asc, freeDiskSpace desc)
    List<TDataNodeLocation> weightList =
        buildWeightList(
            availableDataNodeMap,
            freeDiskSpaceMap,
            allocatedRegionGroups,
            databaseAllocatedRegionGroups);
    return new TRegionReplicaSet(
        consensusGroupId,
        weightList.stream().limit(replicationFactor).collect(Collectors.toList()));
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
        "Greedy",
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
    // can demonstrate the greedy algorithm's load imbalance. Greedy has internal randomness
    // (shuffle for tie-breaking), so different runs may produce different results.
    Map<TConsensusGroupId, TDataNodeConfiguration> bestResult = null;
    long bestVariance = -1;

    for (int round = 0; round < MAX_RETRY; round++) {
      Map<TConsensusGroupId, TDataNodeConfiguration> candidateResult =
          doGreedySelect(availableDataNodeMap, allocatedRegionGroups, remainReplicasMap);

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
        "Greedy",
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
   * Single round of greedy selection: for each region, pick the least-loaded valid node with random
   * tie-breaking.
   */
  private Map<TConsensusGroupId, TDataNodeConfiguration> doGreedySelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {

    // Compute the current region count for each available node
    Map<Integer, Integer> regionCounter = new HashMap<>();
    for (Integer nodeId : availableDataNodeMap.keySet()) {
      regionCounter.put(nodeId, 0);
    }
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      for (TDataNodeLocation loc : replicaSet.getDataNodeLocations()) {
        regionCounter.computeIfPresent(loc.getDataNodeId(), (k, v) -> v + 1);
      }
    }

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

      // Shuffle first to randomize tie-breaking
      Collections.shuffle(candidates, RANDOM);

      // Pick the candidate with the minimum current region count
      int bestNodeId = -1;
      int bestLoad = Integer.MAX_VALUE;
      for (int candidate : candidates) {
        int load = regionCounter.getOrDefault(candidate, 0);
        if (load < bestLoad) {
          bestLoad = load;
          bestNodeId = candidate;
        }
      }

      if (bestNodeId >= 0) {
        result.put(regionId, availableDataNodeMap.get(bestNodeId));
        // Update the region counter so subsequent iterations see the new load
        regionCounter.merge(bestNodeId, 1, Integer::sum);
      }
    }

    return result;
  }

  private List<TDataNodeLocation> buildWeightList(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups) {

    // Map<DataNodeId, Region count>
    Map<Integer, Integer> regionCounter = new HashMap<>(availableDataNodeMap.size());
    allocatedRegionGroups.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));

    // Map<DataNodeId, Region count within the same Database>
    Map<Integer, Integer> databaseRegionCounter = new HashMap<>(availableDataNodeMap.size());
    if (databaseAllocatedRegionGroups != null) {
      databaseAllocatedRegionGroups.forEach(
          regionReplicaSet ->
              regionReplicaSet
                  .getDataNodeLocations()
                  .forEach(
                      dataNodeLocation ->
                          databaseRegionCounter.merge(
                              dataNodeLocation.getDataNodeId(), 1, Integer::sum)));
    }

    /* Construct priority map */
    List<DataNodeEntry> entryList = new ArrayList<>();
    availableDataNodeMap.forEach(
        (datanodeId, dataNodeConfiguration) ->
            entryList.add(
                new DataNodeEntry(
                    datanodeId,
                    regionCounter.getOrDefault(datanodeId, 0),
                    databaseRegionCounter.getOrDefault(datanodeId, 0),
                    freeDiskSpaceMap.getOrDefault(datanodeId, 0d))));

    // Sort weightList
    return entryList.stream()
        .sorted()
        .map(entry -> availableDataNodeMap.get(entry.dataNodeId).getLocation())
        .collect(Collectors.toList());
  }
}
