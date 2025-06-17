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
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabasePartitionTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabasePartitionTable.class);

  // Is the Database pre-deleted
  private volatile boolean preDeleted = false;
  // The name of database
  private String databaseName;

  // RegionGroup
  private final Map<TConsensusGroupId, RegionGroup> regionGroupMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public DatabasePartitionTable(String databaseName) {
    this.databaseName = databaseName;

    this.regionGroupMap = new ConcurrentHashMap<>();

    this.schemaPartitionTable = new SchemaPartitionTable();
    this.dataPartitionTable = new DataPartitionTable();
  }

  public boolean isNotPreDeleted() {
    return !preDeleted;
  }

  public void setPreDeleted(boolean preDeleted) {
    this.preDeleted = preDeleted;
  }

  /**
   * Update the DataNodeLocation in cached RegionGroups.
   *
   * @param newDataNodeLocation The new DataNodeLocation.
   */
  public void updateDataNode(TDataNodeLocation newDataNodeLocation) {
    regionGroupMap.forEach(
        (regionGroupId, regionGroup) -> regionGroup.updateDataNode(newDataNodeLocation));
  }

  /**
   * Cache allocation result of new RegionGroups.
   *
   * @param replicaSets List<TRegionReplicaSet>
   */
  public void createRegionGroups(List<TRegionReplicaSet> replicaSets) {
    replicaSets.forEach(
        replicaSet ->
            regionGroupMap.put(
                replicaSet.getRegionId(),
                new RegionGroup(CommonDateTimeUtils.currentTime(), replicaSet)));
  }

  /**
   * @return Deep copy of all Regions' RegionReplicaSet within one StorageGroup
   */
  public Stream<TRegionReplicaSet> getAllReplicaSets() {
    return regionGroupMap.values().stream().map(RegionGroup::getReplicaSet);
  }

  /**
   * Get all RegionGroups currently owned by this Database.
   *
   * @param type The specified TConsensusGroupType
   * @return Deep copy of all Regions' RegionReplicaSet with the specified TConsensusGroupType
   */
  public List<TRegionReplicaSet> getAllReplicaSets(TConsensusGroupType type) {
    List<TRegionReplicaSet> result = new ArrayList<>();

    for (RegionGroup regionGroup : regionGroupMap.values()) {
      if (type.equals(regionGroup.getId().getType())) {
        result.add(regionGroup.getReplicaSet());
      }
    }

    return result;
  }

  /**
   * Get all RegionGroups currently owned by the specified Database.
   *
   * @param dataNodeId The specified dataNodeId
   * @return Deep copy of all RegionGroups' RegionReplicaSet with the specified dataNodeId
   */
  public List<TRegionReplicaSet> getAllReplicaSets(int dataNodeId) {
    return regionGroupMap.values().stream()
        .filter(regionGroup -> regionGroup.belongsToDataNode(dataNodeId))
        .map(RegionGroup::getReplicaSet)
        .collect(Collectors.toList());
  }

  /**
   * Get the RegionGroups with the specified RegionGroupIds.
   *
   * @param regionGroupIds The specified RegionGroupIds
   * @return Deep copy of the RegionGroups with the specified RegionGroupIds
   */
  public List<TRegionReplicaSet> getReplicaSets(List<TConsensusGroupId> regionGroupIds) {
    List<TRegionReplicaSet> result = new ArrayList<>();

    for (TConsensusGroupId regionGroupId : regionGroupIds) {
      if (regionGroupMap.containsKey(regionGroupId)) {
        result.add(regionGroupMap.get(regionGroupId).getReplicaSet());
      }
    }

    return result;
  }

  /**
   * Only leader use this interface.
   *
   * <p>Get the number of Regions currently owned by the specified DataNode
   *
   * @param dataNodeId The specified DataNode
   * @param type SchemaRegion or DataRegion
   * @return The number of Regions currently owned by the specified DataNode
   */
  public int getRegionCount(int dataNodeId, TConsensusGroupType type) {
    AtomicInteger result = new AtomicInteger(0);
    regionGroupMap
        .values()
        .forEach(
            regionGroup -> {
              if (type.equals(regionGroup.getId().getType())) {
                regionGroup
                    .getReplicaSet()
                    .getDataNodeLocations()
                    .forEach(
                        dataNodeLocation -> {
                          if (dataNodeLocation.getDataNodeId() == dataNodeId) {
                            result.getAndIncrement();
                          }
                        });
              }
            });
    return result.get();
  }

  /**
   * Only leader use this interface.
   *
   * <p>Count the scatter width of the specified DataNode
   *
   * @param dataNodeId The specified DataNode
   * @param type SchemaRegion or DataRegion
   * @param scatterSet The DataNodes in the cluster which have at least one identical schema/data
   *     replica as the specified DataNode will be set true in the scatterSet.
   */
  public void countDataNodeScatterWidth(
      int dataNodeId, TConsensusGroupType type, BitSet scatterSet) {
    regionGroupMap
        .values()
        .forEach(
            regionGroup -> {
              if (type.equals(regionGroup.getId().getType())) {
                Set<Integer> dataNodeIds =
                    regionGroup.getReplicaSet().getDataNodeLocations().stream()
                        .map(TDataNodeLocation::getDataNodeId)
                        .collect(Collectors.toSet());
                if (dataNodeIds.contains(dataNodeId)) {
                  dataNodeIds.forEach(scatterSet::set);
                }
              }
            });
  }

  /**
   * Get the number of RegionGroups currently owned by this StorageGroup.
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

  /**
   * Only leader use this interface.
   *
   * <p>Get all the RegionGroups currently owned by the specified Database
   *
   * @param type SchemaRegion or DataRegion
   * @return List of TConsensusGroupId
   */
  public List<TConsensusGroupId> getAllRegionGroupIds(TConsensusGroupType type) {
    return regionGroupMap.keySet().stream()
        .filter(regionGroupId -> regionGroupId.getType().equals(type))
        .collect(Collectors.toList());
  }

  public int getAssignedSeriesPartitionSlotsCount() {
    return Math.max(
        schemaPartitionTable.getSchemaPartitionMap().size(),
        dataPartitionTable.getDataPartitionMap().size());
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup.
   *
   * @param partitionSlots SeriesPartitionSlots
   * @param schemaPartition Where the results are stored
   * @return True if all the SeriesPartitionSlots are matched, false otherwise
   */
  public boolean getSchemaPartition(
      final List<TSeriesPartitionSlot> partitionSlots, final SchemaPartitionTable schemaPartition) {
    return schemaPartitionTable.getSchemaPartition(partitionSlots, schemaPartition);
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup.
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @param dataPartition Where the results are stored
   * @return True if all the PartitionSlots are matched, false otherwise
   */
  public boolean getDataPartition(
      final Map<TSeriesPartitionSlot, TTimeSlotList> partitionSlots,
      final DataPartitionTable dataPartition) {
    return dataPartitionTable.getDataPartition(partitionSlots, dataPartition);
  }

  /**
   * Checks whether the specified DataPartition has a successor and returns if it does.
   *
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specific DataPartition's successor if exists, null otherwise
   */
  public TConsensusGroupId getSuccessorDataPartition(
      TSeriesPartitionSlot seriesPartitionSlot, TTimePartitionSlot timePartitionSlot) {
    return dataPartitionTable.getSuccessorDataPartition(seriesPartitionSlot, timePartitionSlot);
  }

  /**
   * Checks whether the specified DataPartition has a predecessor and returns if it does.
   *
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPredecessorDataPartition(
      TSeriesPartitionSlot seriesPartitionSlot, TTimePartitionSlot timePartitionSlot) {
    return dataPartitionTable.getPredecessorDataPartition(seriesPartitionSlot, timePartitionSlot);
  }

  /**
   * Create SchemaPartition within the specific StorageGroup.
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
   * Create DataPartition within the specific StorageGroup.
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
   * StorageGroup.
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Unassigned PartitionSlots
   */
  public List<TSeriesPartitionSlot> filterUnassignedSchemaPartitionSlots(
      List<TSeriesPartitionSlot> partitionSlots) {
    return schemaPartitionTable.filterUnassignedSchemaPartitionSlots(partitionSlots);
  }

  /**
   * Get the DataNodes who contain the specific StorageGroup's Schema or Data.
   *
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getDatabaseRelatedDataNodes(TConsensusGroupType type) {
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
   * StorageGroup.
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Unassigned PartitionSlots
   */
  public Map<TSeriesPartitionSlot, TTimeSlotList> filterUnassignedDataPartitionSlots(
      Map<TSeriesPartitionSlot, TTimeSlotList> partitionSlots) {
    return dataPartitionTable.filterUnassignedDataPartitionSlots(partitionSlots);
  }

  /**
   * Only leader use this interface.
   *
   * @param type SchemaRegion or DataRegion
   * @return RegionGroups' slot count and index
   */
  public List<Pair<Long, TConsensusGroupId>> getRegionGroupSlotsCounter(TConsensusGroupType type) {
    List<Pair<Long, TConsensusGroupId>> result = new Vector<>();

    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (type.equals(consensusGroupId.getType())) {
            long slotCount =
                type.equals(TConsensusGroupType.SchemaRegion)
                    ? regionGroup.getSeriesSlotCount()
                    : regionGroup.getTimeSlotCount();
            result.add(new Pair<>(slotCount, consensusGroupId));
          }
        });

    return result;
  }

  public List<TRegionInfo> getRegionInfoList(GetRegionInfoListPlan regionsInfoPlan) {
    List<TRegionInfo> regionInfoList = new Vector<>();
    final TShowRegionReq showRegionReq = regionsInfoPlan.getShowRegionReq();

    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          if (showRegionReq == null
              || showRegionReq.getConsensusGroupType() == null
              || showRegionReq.getConsensusGroupType().equals(regionGroup.getId().getType())) {
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
            dataNodeLocation -> {
              TRegionInfo regionInfo = new TRegionInfo();
              regionInfo.setConsensusGroupId(regionId);
              regionInfo.setDatabase(databaseName);
              regionInfo.setSeriesSlots(regionGroup.getSeriesSlotCount());
              regionInfo.setTimeSlots(regionGroup.getTimeSlotCount());
              regionInfo.setDataNodeId(dataNodeLocation.getDataNodeId());
              regionInfo.setClientRpcIp(dataNodeLocation.getClientRpcEndPoint().getIp());
              regionInfo.setClientRpcPort(dataNodeLocation.getClientRpcEndPoint().getPort());
              regionInfo.setCreateTime(regionGroup.getCreateTime());
              regionInfo.setInternalAddress(dataNodeLocation.getInternalEndPoint().getIp());
              regionInfoList.add(regionInfo);
            });

    return regionInfoList;
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(preDeleted, outputStream);
    ReadWriteIOUtils.write(databaseName, outputStream);

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
    preDeleted = ReadWriteIOUtils.readBool(inputStream);
    databaseName = ReadWriteIOUtils.readString(inputStream);

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

  public List<TConsensusGroupId> getRegionId(
      TConsensusGroupType type,
      TSeriesPartitionSlot seriesSlotId,
      TTimePartitionSlot startTimeSlotId,
      TTimePartitionSlot endTimeSlotId) {
    if (type == TConsensusGroupType.DataRegion) {
      return dataPartitionTable.getRegionId(seriesSlotId, startTimeSlotId, endTimeSlotId);
    } else if (type == TConsensusGroupType.SchemaRegion) {
      return schemaPartitionTable.getRegionId(seriesSlotId);
    } else {
      return new ArrayList<>();
    }
  }

  public List<TTimePartitionSlot> getTimeSlotList(
      TSeriesPartitionSlot seriesSlotId, TConsensusGroupId regionId, long startTime, long endTime) {
    return dataPartitionTable.getTimeSlotList(seriesSlotId, regionId, startTime, endTime);
  }

  public long getTimeSlotCount() {
    return dataPartitionTable.getTimeSlotCount();
  }

  public List<TSeriesPartitionSlot> getSeriesSlotList(TConsensusGroupType type) {
    if (type == TConsensusGroupType.DataRegion) {
      return dataPartitionTable.getSeriesSlotList();
    } else {
      return schemaPartitionTable.getSeriesSlotList();
    }
  }

  void addRegionNewLocation(TConsensusGroupId regionId, TDataNodeLocation node) {
    RegionGroup regionGroup = regionGroupMap.get(regionId);
    if (regionGroup == null) {
      LOGGER.warn(
          "Cannot find RegionGroup for region {} when addRegionNewLocation in {}",
          regionId,
          databaseName);
      return;
    }
    if (regionGroup.getReplicaSet().getDataNodeLocations().contains(node)) {
      LOGGER.info(
          "Node is already in region locations when addRegionNewLocation in {}, "
              + "node: {}, region: {}",
          databaseName,
          node,
          regionId);
      return;
    }
    regionGroup.addRegionLocation(node);
  }

  void removeRegionLocation(TConsensusGroupId regionId, int nodeId) {
    RegionGroup regionGroup = regionGroupMap.get(regionId);
    if (regionGroup == null) {
      LOGGER.warn(
          "Cannot find RegionGroup for region {} when removeRegionOldLocation in {}",
          regionId,
          databaseName);
      return;
    }
    if (regionGroup.getReplicaSet().getDataNodeLocations().stream()
        .map(TDataNodeLocation::getDataNodeId)
        .noneMatch(id -> id == nodeId)) {
      LOGGER.info(
          "Node is not in region locations when removeRegionOldLocation in {}, "
              + "no need to remove it, node: {}, region: {}",
          databaseName,
          nodeId,
          regionId);
      return;
    }
    regionGroup.removeRegionLocation(nodeId);
  }

  /**
   * Check if the DatabasePartitionTable contains the specified Region.
   *
   * @param regionId TConsensusGroupId
   * @return True if contains.
   */
  public boolean containRegionGroup(TConsensusGroupId regionId) {
    return regionGroupMap.containsKey(regionId);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public List<Integer> getSchemaRegionIds() {
    List<Integer> schemaRegionIds = new ArrayList<>();
    for (TConsensusGroupId consensusGroupId : regionGroupMap.keySet()) {
      if (consensusGroupId.getType().equals(TConsensusGroupType.SchemaRegion)) {
        schemaRegionIds.add(consensusGroupId.getId());
      }
    }
    return schemaRegionIds;
  }

  public List<Integer> getDataRegionIds() {
    List<Integer> dataRegionIds = new ArrayList<>();
    for (TConsensusGroupId consensusGroupId : regionGroupMap.keySet()) {
      if (consensusGroupId.getType().equals(TConsensusGroupType.DataRegion)) {
        dataRegionIds.add(consensusGroupId.getId());
      }
    }
    return dataRegionIds;
  }

  public Optional<TConsensusGroupType> getRegionType(int regionId) {
    return regionGroupMap.keySet().stream()
        .filter(tConsensusGroupId -> tConsensusGroupId.getId() == regionId)
        .map(TConsensusGroupId::getType)
        .findFirst();
  }

  /**
   * Get the last DataAllotTable.
   *
   * @return The last DataAllotTable
   */
  public Map<TSeriesPartitionSlot, TConsensusGroupId> getLastDataAllotTable() {
    return dataPartitionTable.getLastDataAllotTable();
  }

  /**
   * Remove PartitionTable where the TimeSlot is expired.
   *
   * @param TTL The Time To Live
   * @param currentTimeSlot The current TimeSlot
   */
  public void autoCleanPartitionTable(long TTL, TTimePartitionSlot currentTimeSlot) {
    long[] removedTimePartitionSlots =
        dataPartitionTable.autoCleanPartitionTable(TTL, currentTimeSlot).stream()
            .map(TTimePartitionSlot::getStartTime)
            .collect(Collectors.toList())
            .stream()
            .mapToLong(Long::longValue)
            .toArray();
    if (removedTimePartitionSlots.length > 0) {
      LOGGER.info(
          "[PartitionTableCleaner] The TimePartitions: {} are removed from Database: {}",
          removedTimePartitionSlots,
          databaseName);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabasePartitionTable that = (DatabasePartitionTable) o;
    return databaseName.equals(that.databaseName)
        && regionGroupMap.equals(that.regionGroupMap)
        && schemaPartitionTable.equals(that.schemaPartitionTable)
        && dataPartitionTable.equals(that.dataPartitionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, regionGroupMap, schemaPartitionTable, dataPartitionTable);
  }
}
