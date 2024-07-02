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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TieredReplicationAllocator implements IRegionGroupAllocator {

  private final Random RANDOM = new Random();
  private final Map<TConsensusGroupType, Integer> dataNodeNumMap = new TreeMap<>();
  private final Map<TConsensusGroupType, Map<Integer, List<List<Integer>>>> COPY_SETS =
      new TreeMap<>();

  private static class DataNodeEntry {

    private final int dataNodeId;
    private final int scatterWidth;

    public DataNodeEntry(int dataNodeId, int scatterWidth) {
      this.dataNodeId = dataNodeId;
      this.scatterWidth = scatterWidth;
    }

    public int getDataNodeId() {
      return dataNodeId;
    }

    public int compare(DataNodeEntry other) {
      return Integer.compare(scatterWidth, other.scatterWidth);
    }
  }

  private void init(
      int dataNodeNum,
      int replicationFactor,
      int loadFactor,
      TConsensusGroupType consensusGroupType) {
    this.dataNodeNumMap.put(consensusGroupType, dataNodeNum);
    Map<Integer, List<List<Integer>>> copy_sets =
        COPY_SETS.computeIfAbsent(consensusGroupType, k -> new TreeMap<>());
    Map<Integer, BitSet> scatterWidthMap = new TreeMap<>();
    for (int i = 1; i <= dataNodeNum; i++) {
      scatterWidthMap.put(i, new BitSet(dataNodeNum + 1));
    }
    int targetScatterWidth = Math.min(loadFactor * (replicationFactor - 1), dataNodeNum - 1);
    while (existScatterWidthUnsatisfied(scatterWidthMap, targetScatterWidth, consensusGroupType)) {
      for (int firstRegion = 1; firstRegion <= dataNodeNum; firstRegion++) {
        if (!copy_sets.containsKey(firstRegion)
            || scatterWidthMap.get(firstRegion).cardinality() < targetScatterWidth) {
          List<Integer> copySet = new ArrayList<>();
          copySet.add(firstRegion);
          List<DataNodeEntry> otherDataNodes = new ArrayList<>();
          for (int i = 1; i <= dataNodeNum; i++) {
            if (i != firstRegion) {
              otherDataNodes.add(new DataNodeEntry(i, scatterWidthMap.get(i).cardinality()));
            }
          }
          otherDataNodes.sort(DataNodeEntry::compare);
          for (DataNodeEntry entry : otherDataNodes) {
            boolean accepted = true;
            int secondRegion = entry.getDataNodeId();
            for (int e : copySet) {
              if (scatterWidthMap.get(e).get(secondRegion)) {
                accepted = false;
                break;
              }
            }
            if (accepted) {
              copySet.add(secondRegion);
            }
            if (copySet.size() == replicationFactor) {
              break;
            }
          }

          while (copySet.size() < replicationFactor) {
            int secondRegion = RANDOM.nextInt(dataNodeNum) + 1;
            while (copySet.contains(secondRegion)) {
              secondRegion = RANDOM.nextInt(dataNodeNum) + 1;
            }
            copySet.add(secondRegion);
          }

          for (int i = 0; i < copySet.size(); i++) {
            for (int j = i + 1; j < copySet.size(); j++) {
              scatterWidthMap.get(copySet.get(i)).set(copySet.get(j));
              scatterWidthMap.get(copySet.get(j)).set(copySet.get(i));
            }
          }
          for (int e : copySet) {
            copy_sets.computeIfAbsent(e, k -> new ArrayList<>()).add(copySet);
          }
          break;
        }
      }
    }
  }

  private boolean existScatterWidthUnsatisfied(
      Map<Integer, BitSet> scatterWidthMap,
      int targetScatterWidth,
      TConsensusGroupType consensusGroupType) {
    for (int i = 1; i <= dataNodeNumMap.get(consensusGroupType); i++) {
      if (!COPY_SETS.get(consensusGroupType).containsKey(i)) {
        return true;
      }
    }
    AtomicBoolean result = new AtomicBoolean(false);
    scatterWidthMap.forEach(
        (k, v) -> {
          if (v.cardinality() < targetScatterWidth) {
            result.set(true);
          }
        });
    return result.get();
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    if (this.dataNodeNumMap.getOrDefault(consensusGroupId.getType(), -1)
        != availableDataNodeMap.size()) {
      init(
          availableDataNodeMap.size(),
          replicationFactor,
          (int) ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode(),
          consensusGroupId.getType());
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    for (int i = 1; i <= dataNodeNumMap.get(consensusGroupId.getType()); i++) {
      regionCounter.put(i, 0);
    }
    allocatedRegionGroups.forEach(
        regionGroup ->
            regionGroup
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));
    int firstRegion = -1, minCount = Integer.MAX_VALUE;
    for (Map.Entry<Integer, Integer> counterEntry : regionCounter.entrySet()) {
      int dataNodeId = counterEntry.getKey();
      int regionCount = counterEntry.getValue();
      if (regionCount < minCount) {
        minCount = regionCount;
        firstRegion = dataNodeId;
      } else if (regionCount == minCount && RANDOM.nextBoolean()) {
        firstRegion = dataNodeId;
      }
    }
    List<Integer> copySet =
        COPY_SETS
            .get(consensusGroupId.getType())
            .get(firstRegion)
            .get(RANDOM.nextInt(COPY_SETS.get(consensusGroupId.getType()).get(firstRegion).size()));
    for (int dataNodeId : copySet) {
      result.addToDataNodeLocations(availableDataNodeMap.get(dataNodeId).getLocation());
    }
    return result.setRegionId(consensusGroupId);
  }
}
