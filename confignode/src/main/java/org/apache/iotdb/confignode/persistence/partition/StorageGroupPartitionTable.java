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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageGroupPartitionTable {
  private volatile boolean isPredeleted = false;
  // Total number of SeriesPartitionSlots occupied by schema,
  // determines whether a new Region needs to be created
  private final AtomicInteger seriesPartitionSlotsCount;

  // Region allocation particle
  private final AtomicBoolean schemaRegionParticle;
  private final AtomicBoolean dataRegionParticle;

  // Region
  private final Map<TConsensusGroupId, RegionGroup> regionInfoMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public StorageGroupPartitionTable() {
    this.seriesPartitionSlotsCount = new AtomicInteger(0);

    this.schemaRegionParticle = new AtomicBoolean(true);
    this.dataRegionParticle = new AtomicBoolean(true);
    this.regionInfoMap = new ConcurrentHashMap<>();

    this.schemaPartitionTable = new SchemaPartitionTable();
    this.dataPartitionTable = new DataPartitionTable();
  }

  public boolean isPredeleted() {
    return isPredeleted;
  }

  public void setPredeleted(boolean predeleted) {
    isPredeleted = predeleted;
  }

  /**
   * Cache allocation result of new Regions
   *
   * @param replicaSets List<TRegionReplicaSet>
   */
  public void createRegions(List<TRegionReplicaSet> replicaSets) {
    replicaSets.forEach(
        replicaSet -> regionInfoMap.put(replicaSet.getRegionId(), new RegionGroup(replicaSet)));
  }

  /** @return All Regions' RegionReplicaSet within one StorageGroup */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    List<TRegionReplicaSet> result = new ArrayList<>();

    for (RegionGroup regionGroup : regionInfoMap.values()) {
      result.add(regionGroup.getReplicaSet());
    }

    return result;
  }

  /**
   * Get regions currently owned by this StorageGroup
   *
   * @param type SchemaRegion or DataRegion
   * @return The regions currently owned by this StorageGroup
   */
  public Set<RegionGroup> getRegion(TConsensusGroupType type) {
    Set<RegionGroup> regionGroups = new HashSet<>();
    regionInfoMap
        .values()
        .forEach(
            regionGroup -> {
              if (regionGroup.getId().getType().equals(type)) {
                regionGroups.add(regionGroup);
              }
            });
    return regionGroups;
  }
  /**
   * Get the number of Regions currently owned by this StorageGroup
   *
   * @param type SchemaRegion or DataRegion
   * @return The number of Regions currently owned by this StorageGroup
   */
  public int getRegionCount(TConsensusGroupType type) {
    AtomicInteger result = new AtomicInteger(0);
    regionInfoMap
        .values()
        .forEach(
            regionGroup -> {
              if (regionGroup.getId().getType().equals(type)) {
                result.getAndIncrement();
              }
            });
    return result.getAndIncrement();
  }

  /**
   * Only leader use this interface. Contending the Region allocation particle
   *
   * @param type SchemaRegion or DataRegion
   * @return True when successfully get the allocation particle, false otherwise
   */
  public boolean getRegionAllocationParticle(TConsensusGroupType type) {
    switch (type) {
      case SchemaRegion:
        return schemaRegionParticle.getAndSet(false);
      case DataRegion:
        return dataRegionParticle.getAndSet(false);
      default:
        return false;
    }
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup TODO: Remove mapping process
   *
   * @param partitionSlots SeriesPartitionSlots
   * @param schemaPartition Where the results are stored
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getSchemaPartition(
      List<TSeriesPartitionSlot> partitionSlots,
      Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaPartition) {
    // Get RegionIds
    SchemaPartitionTable regionIds = new SchemaPartitionTable();
    boolean result = schemaPartitionTable.getSchemaPartition(partitionSlots, regionIds);
    // Map to RegionReplicaSets
    regionIds
        .getSchemaPartitionMap()
        .forEach(
            (seriesPartitionSlot, consensusGroupId) ->
                schemaPartition.put(
                    seriesPartitionSlot, regionInfoMap.get(consensusGroupId).getReplicaSet()));
    return result;
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup TODO: Remove mapping process
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @param dataPartition Where the results are stored
   * @return True if all the PartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots,
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> dataPartition) {
    // Get RegionIds
    DataPartitionTable regionIds = new DataPartitionTable();
    boolean result = dataPartitionTable.getDataPartition(partitionSlots, regionIds);
    // Map to RegionReplicaSets
    regionIds
        .getDataPartitionMap()
        .forEach(
            ((seriesPartitionSlot, seriesPartition) -> {
              dataPartition.put(seriesPartitionSlot, new ConcurrentHashMap<>());
              seriesPartition
                  .getSeriesPartitionMap()
                  .forEach(
                      ((timePartitionSlot, consensusGroupIds) -> {
                        dataPartition
                            .get(seriesPartitionSlot)
                            .put(timePartitionSlot, new Vector<>());
                        consensusGroupIds.forEach(
                            consensusGroupId ->
                                dataPartition
                                    .get(seriesPartitionSlot)
                                    .get(timePartitionSlot)
                                    .add(regionInfoMap.get(consensusGroupId).getReplicaSet()));
                      }));
            }));
    return result;
  }

  /**
   * Create SchemaPartition within the specific StorageGroup
   *
   * @param assignedSchemaPartition Assigned result
   */
  public void createSchemaPartition(SchemaPartitionTable assignedSchemaPartition) {
    // Cache assigned result
    Map<TConsensusGroupId, AtomicInteger> deltaMap =
        schemaPartitionTable.createSchemaPartition(assignedSchemaPartition);

    // Add counter
    AtomicInteger total = new AtomicInteger(0);
    deltaMap.forEach(
        ((consensusGroupId, delta) -> {
          total.getAndAdd(delta.get());
          regionInfoMap.get(consensusGroupId).addCounter(delta.get());
        }));
    seriesPartitionSlotsCount.getAndAdd(total.get());
  }

  /**
   * Create DataPartition within the specific StorageGroup
   *
   * @param assignedDataPartition Assigned result
   */
  public void createDataPartition(DataPartitionTable assignedDataPartition) {
    // Cache assigned result
    Map<TConsensusGroupId, AtomicInteger> deltaMap =
        dataPartitionTable.createDataPartition(assignedDataPartition);

    // Add counter
    AtomicInteger total = new AtomicInteger(0);
    deltaMap.forEach(
        ((consensusGroupId, delta) -> {
          total.getAndAdd(delta.get());
          regionInfoMap.get(consensusGroupId).addCounter(delta.get());
        }));
  }

  /**
   * Only Leader use this interface. Filter unassigned SchemaPartitionSlots within the specific
   * StorageGroup
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Unassigned PartitionSlots
   */
  public List<TSeriesPartitionSlot> filterUnassignedSchemaPartitionSlots(
      List<TSeriesPartitionSlot> partitionSlots) {
    return schemaPartitionTable.filterUnassignedSchemaPartitionSlots(partitionSlots);
  }

  /**
   * Only Leader use this interface. Filter unassigned DataPartitionSlots within the specific
   * StorageGroup
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Unassigned PartitionSlots
   */
  public Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> filterUnassignedDataPartitionSlots(
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots) {
    return dataPartitionTable.filterUnassignedDataPartitionSlots(partitionSlots);
  }

  /**
   * Only leader use this interface.
   *
   * @param type SchemaRegion or DataRegion
   * @return Regions that sorted by the number of allocated slots
   */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionSlotsCounter(TConsensusGroupType type) {
    List<Pair<Long, TConsensusGroupId>> result = new Vector<>();

    regionInfoMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (consensusGroupId.getType().equals(type)) {
            result.add(new Pair<>(regionGroup.getCounter(), consensusGroupId));
          }
        });

    result.sort(Comparator.comparingLong(Pair::getLeft));
    return result;
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(isPredeleted, outputStream);
    ReadWriteIOUtils.write(seriesPartitionSlotsCount.get(), outputStream);

    ReadWriteIOUtils.write(regionInfoMap.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, RegionGroup> regionInfoEntry : regionInfoMap.entrySet()) {
      regionInfoEntry.getKey().write(protocol);
      regionInfoEntry.getValue().serialize(outputStream, protocol);
    }

    schemaPartitionTable.serialize(outputStream, protocol);
    dataPartitionTable.serialize(outputStream, protocol);
  }

  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    isPredeleted = ReadWriteIOUtils.readBool(inputStream);
    seriesPartitionSlotsCount.set(ReadWriteIOUtils.readInt(inputStream));

    int length = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < length; i++) {
      TConsensusGroupId consensusGroupId = new TConsensusGroupId();
      consensusGroupId.read(protocol);
      RegionGroup regionGroup = new RegionGroup();
      regionGroup.deserialize(inputStream, protocol);
      regionInfoMap.put(consensusGroupId, regionGroup);
    }

    schemaPartitionTable.deserialize(inputStream, protocol);
    dataPartitionTable.deserialize(inputStream, protocol);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageGroupPartitionTable that = (StorageGroupPartitionTable) o;
    return isPredeleted == that.isPredeleted
        && regionInfoMap.equals(that.regionInfoMap)
        && schemaPartitionTable.equals(that.schemaPartitionTable)
        && dataPartitionTable.equals(that.dataPartitionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isPredeleted, regionInfoMap, schemaPartitionTable, dataPartitionTable);
  }
}
