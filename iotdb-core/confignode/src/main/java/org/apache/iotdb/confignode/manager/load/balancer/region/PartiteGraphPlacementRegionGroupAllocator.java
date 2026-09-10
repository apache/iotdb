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
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphPlacementRegionGroupAllocator implements IRegionGroupAllocator {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final GreedyRegionGroupAllocator GREEDY_ALLOCATOR =
      new GreedyRegionGroupAllocator();

  private int subGraphCount;
  private int replicationFactor;
  private int regionPerDataNode;

  private int dataNodeNum;
  // The number of allocated Regions in each DataNode (fake-id indexed)
  private int[] regionCounter;
  // The number of allocated Regions in each DataNode within the same Database (fake-id indexed)
  private int[] databaseRegionCounter;
  // Whether there exists a region with both i and j as replicas (fake-id indexed, binary)
  private int[][] combinationCounter;
  private Map<Integer, Integer> fakeToRealIdMap;

  private int alphaDataNodeNum;
  // The best valuation found so far
  private Value bestValue;
  private int[] bestAlphaNodes;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    this.regionPerDataNode =
        consensusGroupId.getType().equals(TConsensusGroupType.DataRegion)
            ? ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode()
            : ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode();
    prepare(
        replicationFactor,
        availableDataNodeMap,
        allocatedRegionGroups,
        databaseAllocatedRegionGroups);

    // Select alpha nodes set
    for (int i = 0; i < subGraphCount; i++) {
      subGraphSearch(i, freeDiskSpaceMap);
    }
    if (bestValue.regionSum == Integer.MAX_VALUE) {
      // Use greedy allocator as alternative if no alpha nodes set is found
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    // Select the beta nodes sets
    List<Integer> betaDataNodes = partiteGraphSearch(bestAlphaNodes[0] % subGraphCount);
    if (betaDataNodes.size() < replicationFactor - alphaDataNodeNum) {
      // Use greedy allocator as alternative if no beta nodes set is found
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(
          availableDataNodeMap,
          freeDiskSpaceMap,
          allocatedRegionGroups,
          databaseAllocatedRegionGroups,
          replicationFactor,
          consensusGroupId);
    }

    // The next placement scheme is alpha \cup beta
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(bestAlphaNodes[i])).getLocation());
    }
    for (int i = 0; i < replicationFactor - alphaDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(betaDataNodes.get(i))).getLocation());
    }
    return result;
  }

  @Override
  public Map<TConsensusGroupId, TDataNodeConfiguration> removeNodeReplicaSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {
    // TODO: Implement this method
    throw new UnsupportedOperationException(
        ManagerMessages
            .THE_REMOVENODEREPLICASELECT_METHOD_OF_PARTITEGRAPHPLACEMENTREGIONGROUPALLOCATOR);
  }

  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups) {
    this.subGraphCount = replicationFactor / 2 + (replicationFactor % 2 == 0 ? 0 : 1);
    this.replicationFactor = replicationFactor;

    // Initialize the fake index for each DataNode,
    // since the index of DataNode might not be continuous
    this.fakeToRealIdMap = new TreeMap<>();
    Map<Integer, Integer> realToFakeIdMap = new TreeMap<>();
    this.dataNodeNum = availableDataNodeMap.size();
    List<Integer> dataNodeIdList =
        availableDataNodeMap.values().stream()
            .map(c -> c.getLocation().getDataNodeId())
            .collect(Collectors.toList());
    for (int i = 0; i < dataNodeNum; i++) {
      fakeToRealIdMap.put(i, dataNodeIdList.get(i));
      realToFakeIdMap.put(dataNodeIdList.get(i), i);
    }

    // Compute regionCounter, databaseRegionCounter and combinationCounter
    this.regionCounter = new int[dataNodeNum];
    Arrays.fill(regionCounter, 0);
    this.databaseRegionCounter = new int[dataNodeNum];
    Arrays.fill(databaseRegionCounter, 0);
    this.combinationCounter = new int[dataNodeNum][dataNodeNum];
    for (int i = 0; i < dataNodeNum; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        Integer fakeIId = realToFakeIdMap.get(dataNodeLocations.get(i).getDataNodeId());
        if (fakeIId == null) {
          // Skip nodes that are no longer available
          continue;
        }
        regionCounter[fakeIId]++;
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          Integer fakeJId = realToFakeIdMap.get(dataNodeLocations.get(j).getDataNodeId());
          if (fakeJId == null) {
            continue;
          }
          combinationCounter[fakeIId][fakeJId] = 1;
          combinationCounter[fakeJId][fakeIId] = 1;
        }
      }
    }
    if (databaseAllocatedRegionGroups != null) {
      for (TRegionReplicaSet regionReplicaSet : databaseAllocatedRegionGroups) {
        for (TDataNodeLocation location : regionReplicaSet.getDataNodeLocations()) {
          Integer fakeId = realToFakeIdMap.get(location.getDataNodeId());
          if (fakeId != null) {
            databaseRegionCounter[fakeId]++;
          }
        }
      }
    }

    // Reset the optimal result
    this.alphaDataNodeNum = replicationFactor / 2 + 1;
    this.bestValue = Value.worst();
    this.bestAlphaNodes = new int[alphaDataNodeNum];
  }

  private Value valuation(int[] nodes) {
    int regionSum = 0;
    int databaseRegionSum = 0;
    int edgeSum = 0;
    for (int iota : nodes) {
      regionSum += regionCounter[iota];
      databaseRegionSum += databaseRegionCounter[iota];
      for (int kappa : nodes) {
        edgeSum += combinationCounter[iota][kappa];
      }
    }
    return new Value(regionSum, databaseRegionSum, edgeSum);
  }

  private void subGraphSearch(int firstIndex, Map<Integer, Double> freeDiskSpaceMap) {
    List<PgpDataNodeEntry> entryList = new ArrayList<>();
    for (int index = firstIndex; index < dataNodeNum; index += subGraphCount) {
      // Prune: skip filled DataNodes
      if (regionCounter[index] < regionPerDataNode) {
        entryList.add(
            new PgpDataNodeEntry(
                index,
                regionCounter[index],
                databaseRegionCounter[index],
                freeDiskSpaceMap.get(fakeToRealIdMap.get(index))));
      }
    }
    if (entryList.size() < alphaDataNodeNum) {
      // Skip: not enough DataNodes
      return;
    }
    Collections.sort(entryList);
    int[] alphaNodes = new int[alphaDataNodeNum];
    for (int i = 0; i < alphaDataNodeNum - 1; i++) {
      alphaNodes[i] = entryList.get(i).dataNodeId;
    }
    for (int i = alphaDataNodeNum - 1; i < entryList.size(); i++) {
      alphaNodes[alphaDataNodeNum - 1] = entryList.get(i).dataNodeId;
      Value currentValue = valuation(alphaNodes);
      if (currentValue.compareTo(bestValue) < 0) {
        bestValue = currentValue;
        System.arraycopy(alphaNodes, 0, bestAlphaNodes, 0, alphaDataNodeNum);
      }
    }
  }

  private List<Integer> partiteGraphSearch(int alphaIndex) {
    List<Integer> betaNodes = new ArrayList<>();
    int[] tmpNodes = new int[alphaDataNodeNum + 1];
    System.arraycopy(bestAlphaNodes, 0, tmpNodes, 0, alphaDataNodeNum);
    for (int partiteIndex = 0; partiteIndex < subGraphCount; partiteIndex++) {
      if (partiteIndex == alphaIndex) {
        // Skip the alphaIndex subgraph
        continue;
      }
      int selectedDataNode = -1;
      Value tmpValue = Value.worst();
      for (int i = partiteIndex; i < dataNodeNum; i += subGraphCount) {
        if (regionCounter[i] >= regionPerDataNode) {
          // Pruning: skip filled DataNodes
          continue;
        }
        tmpNodes[alphaDataNodeNum] = i;
        Value currentValue = valuation(tmpNodes);
        if (currentValue.compareTo(tmpValue) < 0) {
          tmpValue = currentValue;
          selectedDataNode = i;
        }
      }
      if (selectedDataNode == -1) {
        return new ArrayList<>();
      }
      betaNodes.add(selectedDataNode);
    }
    return betaNodes;
  }

  /**
   * Valuation for a candidate alpha (or alpha ∪ beta-candidate) set. Smaller is better. Comparison
   * priority:
   *
   * <ol>
   *   <li>{@code regionSum} — global per-DN region count (balance the whole cluster)
   *   <li>{@code databaseRegionSum} — per-(database, DN) region count (balance each database)
   *   <li>{@code edgeSum} — 2-Region combination scatter (refines PGP's scatter property)
   * </ol>
   */
  private static class Value implements Comparable<Value> {

    private final int regionSum;
    private final int databaseRegionSum;
    private final int edgeSum;

    private Value(int regionSum, int databaseRegionSum, int edgeSum) {
      this.regionSum = regionSum;
      this.databaseRegionSum = databaseRegionSum;
      this.edgeSum = edgeSum;
    }

    private static Value worst() {
      return new Value(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Override
    public int compareTo(Value other) {
      if (regionSum != other.regionSum) {
        return Integer.compare(regionSum, other.regionSum);
      }
      if (databaseRegionSum != other.databaseRegionSum) {
        return Integer.compare(databaseRegionSum, other.databaseRegionSum);
      }
      return Integer.compare(edgeSum, other.edgeSum);
    }
  }

  /**
   * Pre-sort entry for selecting alpha nodes inside a sub-graph. Sort priority matches {@link
   * Value}: regionCount first, databaseRegionCount second, then freeDiskSpace (descending) and a
   * random tie-breaker.
   */
  private static class PgpDataNodeEntry implements Comparable<PgpDataNodeEntry> {

    private final int dataNodeId;
    private final int regionCount;
    private final int databaseRegionCount;
    private final double freeDiskSpace;
    private final int randomWeight;

    private PgpDataNodeEntry(
        int dataNodeId, int regionCount, int databaseRegionCount, double freeDiskSpace) {
      this.dataNodeId = dataNodeId;
      this.regionCount = regionCount;
      this.databaseRegionCount = databaseRegionCount;
      this.freeDiskSpace = freeDiskSpace;
      this.randomWeight = RANDOM.nextInt();
    }

    @Override
    public int compareTo(PgpDataNodeEntry other) {
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
}
