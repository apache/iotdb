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
package org.apache.iotdb.confignode.persistence.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageGroupPartitionTable {

  // StorageGroupName
  private final String name;

  // Total number of SeriesPartitionSlots occupied by schema,
  // determines whether a new Region needs to be created
  private final AtomicInteger seriesPartitionSlotsCount;

  // Region
  private final Map<TConsensusGroupId, RegionInfo> regionInfoMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public StorageGroupPartitionTable(String name) {
    this.name = name;
    this.seriesPartitionSlotsCount = new AtomicInteger(0);
    this.regionInfoMap = new ConcurrentHashMap<>();
    this.schemaPartitionTable = new SchemaPartitionTable();
    this.dataPartitionTable = new DataPartitionTable();
  }

  /**
   * Cache allocation result of new Regions
   *
   * @param replicaSets List<TRegionReplicaSet>
   */
  public void createRegions(List<TRegionReplicaSet> replicaSets) {
    replicaSets.forEach(replicaSet -> regionInfoMap.put(replicaSet.getRegionId(), new RegionInfo(replicaSet)));
  }

  /** @return All Regions' RegionReplicaSet */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    List<TRegionReplicaSet> result = new ArrayList<>();

    for (RegionInfo regionInfo : regionInfoMap.values()) {
      result.add(regionInfo.getReplicaSet());
    }

    return result;
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup
   * TODO: Remove mapping process
   *
   * @param partitionSlots SeriesPartitionSlots
   * @param schemaPartition Where the results are stored
   */
  public void getSchemaPartition(List<TSeriesPartitionSlot> partitionSlots, Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaPartition) {
    // Get RegionIds
    SchemaPartitionTable regionIdsMap = schemaPartitionTable.getSchemaPartition(partitionSlots);
    // Map to RegionReplicaSets
    regionIdsMap.forEach((seriesPartitionSlot, consensusGroupId) ->
      schemaPartition.put(seriesPartitionSlot, regionInfoMap.get(consensusGroupId).getReplicaSet()));
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   * TODO: Remove mapping process
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @param dataPartition Where the results are stored
   */
  public void getDataPartition(Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots,
                               Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> dataPartition) {
    // Get RegionIds
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>> regionIdsMap = dataPartitionTable.getDataPartition(partitionSlots);
    // Map to RegionReplicaSets
    regionIdsMap.forEach(((seriesPartitionSlot, timePartitionSlotsMap) -> {
      dataPartition.put(seriesPartitionSlot, new ConcurrentHashMap<>());
      timePartitionSlotsMap.forEach(((timePartitionSlot, consensusGroupIds) -> {
        dataPartition.get(seriesPartitionSlot).put(timePartitionSlot, new Vector<>());
        consensusGroupIds.forEach(consensusGroupId -> dataPartition.get(seriesPartitionSlot).get(timePartitionSlot).add(regionInfoMap.get(consensusGroupId).getReplicaSet()));
      }));
    }));
  }

  /**
   * Thread-safely create SchemaPartition within the specific StorageGroup
   *
   * @param assignedSchemaPartition Map<TSeriesPartitionSlot, TConsensusGroupId>, assigned result
   */
  public void createSchemaPartition(Map<TSeriesPartitionSlot, TConsensusGroupId> assignedSchemaPartition) {
    // Cache assigned result
    Map<TConsensusGroupId, AtomicInteger> deltaMap = schemaPartitionTable.createSchemaPartition(assignedSchemaPartition);

    // Add counter
    AtomicInteger total = new AtomicInteger(0);
    deltaMap.forEach(((consensusGroupId, delta) -> {
      total.getAndAdd(delta.get());
      regionInfoMap.get(consensusGroupId).addCounter(delta.get());
    }));
    seriesPartitionSlotsCount.getAndAdd(total.get());
  }

  /**
   * Only Leader use this interface
   * Thread-safely filter no assigned SchemaPartitionSlots within the specific StorageGroup
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @param unAssignedPartitions Where the results are stored
   */
  public void filterNoAssignedSchemaPartitionSlots(List<TSeriesPartitionSlot> partitionSlots, Vector<TSeriesPartitionSlot> unAssignedPartitions) {
    schemaPartitionTable.filterNoAssignedSchemaPartitionSlots(partitionSlots, unAssignedPartitions);
  }
}
