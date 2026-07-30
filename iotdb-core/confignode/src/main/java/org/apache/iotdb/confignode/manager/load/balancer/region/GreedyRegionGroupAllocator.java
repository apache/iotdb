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
import org.apache.iotdb.confignode.i18n.ManagerMessages;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Allocate Region Greedily */
public class GreedyRegionGroupAllocator implements IRegionGroupAllocator {

  public static final SecureRandom RANDOM = new SecureRandom();

  public GreedyRegionGroupAllocator() {
    // Empty constructor
  }

  public static class DataNodeEntry implements Comparable<DataNodeEntry> {

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
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {
    // TODO: Implement this method
    throw new UnsupportedOperationException(
        ManagerMessages.THE_REMOVENODEREPLICASELECT_METHOD_OF_GREEDYREGIONGROUPALLOCATOR_IS_YET);
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
