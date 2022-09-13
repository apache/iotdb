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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StorageGroupPartitionTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupPartitionTable.class);

  private volatile boolean isPredeleted = false;
  // The name of storage group
  private String storageGroupName;

  // Region
  private final Map<TConsensusGroupId, RegionGroup> regionGroupMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public StorageGroupPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;

    this.regionGroupMap = new ConcurrentHashMap<>();

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
   * Cache allocation result of new RegionGroups
   *
   * @param replicaSets List<TRegionReplicaSet>
   */
  public void createRegionGroups(List<TRegionReplicaSet> replicaSets) {
    replicaSets.forEach(
        replicaSet -> regionGroupMap.put(replicaSet.getRegionId(), new RegionGroup(replicaSet)));
  }

  /**
   * Delete RegionGroups' cache
   *
   * @param replicaSets List<TRegionReplicaSet>
   */
  public void deleteRegionGroups(List<TRegionReplicaSet> replicaSets) {
    replicaSets.forEach(replicaSet -> regionGroupMap.remove(replicaSet.getRegionId()));
  }

  /** @return All Regions' RegionReplicaSet within one StorageGroup */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    List<TRegionReplicaSet> result = new ArrayList<>();

    for (RegionGroup regionGroup : regionGroupMap.values()) {
      result.add(regionGroup.getReplicaSet());
    }

    return result;
  }

  /**
   * Get all RegionGroups currently owned by this StorageGroup
   *
   * @param type SchemaRegion or DataRegion
   * @return The regions currently owned by this StorageGroup
   */
  public Set<RegionGroup> getRegionGroups(TConsensusGroupType type) {
    Set<RegionGroup> regionGroups = new HashSet<>();
    regionGroupMap
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
   * Get the number of RegionGroups currently owned by this StorageGroup
   *
   * @param type SchemaRegion or DataRegion
   * @return The number of Regions currently owned by this StorageGroup
   */
  public int getRegionGroupCount(TConsensusGroupType type) {
    AtomicInteger result = new AtomicInteger(0);
    regionGroupMap
        .values()
        .forEach(
            regionGroup -> {
              if (regionGroup.getId().getType().equals(type)) {
                result.getAndIncrement();
              }
            });
    return result.getAndIncrement();
  }

  public int getAssignedSeriesPartitionSlotsCount() {
    return Math.max(
        schemaPartitionTable.getSchemaPartitionMap().size(),
        dataPartitionTable.getDataPartitionMap().size());
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots
   * @param schemaPartition Where the results are stored
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getSchemaPartition(
      List<TSeriesPartitionSlot> partitionSlots, SchemaPartitionTable schemaPartition) {
    return schemaPartitionTable.getSchemaPartition(partitionSlots, schemaPartition);
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @param dataPartition Where the results are stored
   * @return True if all the PartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots,
      DataPartitionTable dataPartition) {
    return dataPartitionTable.getDataPartition(partitionSlots, dataPartition);
  }

  /**
   * Checks whether the specified DataPartition has a predecessor and returns if it does
   *
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @param timePartitionInterval Time partition interval
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPrecededDataPartition(
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      long timePartitionInterval) {
    return dataPartitionTable.getPrecededDataPartition(
        seriesPartitionSlot, timePartitionSlot, timePartitionInterval);
  }

  /**
   * Create SchemaPartition within the specific StorageGroup
   *
   * @param assignedSchemaPartition Assigned result
   */
  public void createSchemaPartition(SchemaPartitionTable assignedSchemaPartition) {
    // Cache assigned result
    // Map<TConsensusGroupId, Map<TSeriesPartitionSlot, deltaTimeSlotCount>>
    Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> groupDeltaMap =
        schemaPartitionTable.createSchemaPartition(assignedSchemaPartition);

    // Update counter
    groupDeltaMap.forEach(
        ((consensusGroupId, deltaMap) ->
            regionGroupMap.get(consensusGroupId).updateSlotCountMap(deltaMap)));
  }

  /**
   * Create DataPartition within the specific StorageGroup
   *
   * @param assignedDataPartition Assigned result
   */
  public void createDataPartition(DataPartitionTable assignedDataPartition) {
    // Cache assigned result
    // Map<TConsensusGroupId, Map<TSeriesPartitionSlot, deltaTimeSlotCount>>
    Map<TConsensusGroupId, Map<TSeriesPartitionSlot, AtomicLong>> groupDeltaMap =
        dataPartitionTable.createDataPartition(assignedDataPartition);

    // Update counter
    groupDeltaMap.forEach(
        ((consensusGroupId, deltaMap) ->
            regionGroupMap.get(consensusGroupId).updateSlotCountMap(deltaMap)));
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
   * Get the DataNodes who contain the specific StorageGroup's Schema or Data
   *
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getStorageGroupRelatedDataNodes(TConsensusGroupType type) {
    HashSet<TDataNodeLocation> result = new HashSet<>();
    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (consensusGroupId.getType().equals(type)) {
            result.addAll(regionGroup.getReplicaSet().getDataNodeLocations());
          }
        });
    return result;
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
   * @return RegionGroups' indexes that sorted by the number of allocated slots
   */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionGroupSlotsCounter(
      TConsensusGroupType type) {
    List<Pair<Long, TConsensusGroupId>> result = new Vector<>();

    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (consensusGroupId.getType().equals(type)) {
            result.add(new Pair<>(regionGroup.getSeriesSlotCount(), consensusGroupId));
          }
        });

    result.sort(Comparator.comparingLong(Pair::getLeft));
    return result;
  }

  public List<TRegionInfo> getRegionInfoList(GetRegionInfoListPlan regionsInfoPlan) {
    List<TRegionInfo> regionInfoList = new Vector<>();
    final TShowRegionReq showRegionReq = regionsInfoPlan.getShowRegionReq();

    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (showRegionReq == null || showRegionReq.getConsensusGroupType() == null) {
            regionInfoList.addAll(buildRegionInfoList(regionGroup));
          } else if (showRegionReq.getConsensusGroupType().equals(regionGroup.getId().getType())) {
            regionInfoList.addAll(buildRegionInfoList(regionGroup));
          }
        });

    return regionInfoList;
  }

  private List<TRegionInfo> buildRegionInfoList(RegionGroup regionGroup) {
    List<TRegionInfo> regionInfoList = new Vector<>();
    final TConsensusGroupId regionId = regionGroup.getId();

    regionGroup
        .getReplicaSet()
        .getDataNodeLocations()
        .forEach(
            (dataNodeLocation) -> {
              TRegionInfo regionInfo = new TRegionInfo();
              regionInfo.setConsensusGroupId(regionId);
              regionInfo.setStorageGroup(storageGroupName);
              regionInfo.setSeriesSlots(regionGroup.getSeriesSlotCount());
              regionInfo.setTimeSlots(regionGroup.getTimeSlotCount());
              regionInfo.setDataNodeId(dataNodeLocation.getDataNodeId());
              regionInfo.setClientRpcIp(dataNodeLocation.getClientRpcEndPoint().getIp());
              regionInfo.setClientRpcPort(dataNodeLocation.getClientRpcEndPoint().getPort());
              // TODO: Maintain Region status
              regionInfo.setStatus(RegionStatus.Up.getStatus());
              regionInfoList.add(regionInfo);
            });

    return regionInfoList;
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(isPredeleted, outputStream);
    ReadWriteIOUtils.write(storageGroupName, outputStream);

    ReadWriteIOUtils.write(regionGroupMap.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, RegionGroup> regionInfoEntry : regionGroupMap.entrySet()) {
      regionInfoEntry.getKey().write(protocol);
      regionInfoEntry.getValue().serialize(outputStream, protocol);
    }

    schemaPartitionTable.serialize(outputStream, protocol);
    dataPartitionTable.serialize(outputStream, protocol);
  }

  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    isPredeleted = ReadWriteIOUtils.readBool(inputStream);
    storageGroupName = ReadWriteIOUtils.readString(inputStream);

    int length = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < length; i++) {
      TConsensusGroupId consensusGroupId = new TConsensusGroupId();
      consensusGroupId.read(protocol);
      RegionGroup regionGroup = new RegionGroup();
      regionGroup.deserialize(inputStream, protocol);
      regionGroupMap.put(consensusGroupId, regionGroup);
    }

    schemaPartitionTable.deserialize(inputStream, protocol);
    dataPartitionTable.deserialize(inputStream, protocol);
  }

  /**
   * update region location
   *
   * @param regionId regionId
   * @param oldNode old location, will remove it
   * @param newNode new location, will add it
   */
  public void updateRegionLocation(
      TConsensusGroupId regionId, TDataNodeLocation oldNode, TDataNodeLocation newNode) {
    addRegionNewLocation(regionId, newNode);
    removeRegionOldLocation(regionId, oldNode);
  }

  private boolean addRegionNewLocation(TConsensusGroupId regionId, TDataNodeLocation node) {
    RegionGroup regionGroup = regionGroupMap.get(regionId);
    if (regionGroup == null) {
      LOGGER.warn("not find Region Group for region {}", regionId);
      return false;
    }
    if (regionGroup.getReplicaSet().getDataNodeLocations().contains(node)) {
      LOGGER.info("Node is already in region locations, node: {}, region: {}", node, regionId);
      return true;
    }
    return regionGroup.getReplicaSet().getDataNodeLocations().add(node);
  }

  private boolean removeRegionOldLocation(TConsensusGroupId regionId, TDataNodeLocation node) {
    RegionGroup regionGroup = regionGroupMap.get(regionId);
    if (regionGroup == null) {
      LOGGER.warn("not find Region Group for region {}", regionId);
      return false;
    }
    if (!regionGroup.getReplicaSet().getDataNodeLocations().contains(node)) {
      LOGGER.info(
          "Node is Not in region locations, no need to remove it, node: {}, region: {}",
          node,
          regionId);
      return true;
    }
    return regionGroup.getReplicaSet().getDataNodeLocations().remove(node);
  }

  /**
   * if the region contained?
   *
   * @param regionId TConsensusGroupId
   * @return true if contain
   */
  public boolean containRegion(TConsensusGroupId regionId) {
    return regionGroupMap.containsKey(regionId);
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public int getDataPartitionMapSize() {
    return dataPartitionTable.getDataPartitionMap().size();
  }

  public int getSchemaPartitionMapSize() {
    return schemaPartitionTable.getSchemaPartitionMap().size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageGroupPartitionTable that = (StorageGroupPartitionTable) o;
    return storageGroupName.equals(that.storageGroupName)
        && regionGroupMap.equals(that.regionGroupMap)
        && schemaPartitionTable.equals(that.schemaPartitionTable)
        && dataPartitionTable.equals(that.dataPartitionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroupName, regionGroupMap, schemaPartitionTable, dataPartitionTable);
  }
}
