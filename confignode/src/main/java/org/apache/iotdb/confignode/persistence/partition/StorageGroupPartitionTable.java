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
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;
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

public class StorageGroupPartitionTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupPartitionTable.class);

  private volatile boolean isPredeleted = false;
  // The name of storage group
  private String storageGroupName;

  // Total number of SeriesPartitionSlots occupied by schema,
  // determines whether a new Region needs to be created
  private final AtomicInteger seriesPartitionSlotsCount;

  // Region
  private final Map<TConsensusGroupId, RegionGroup> regionGroupMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public StorageGroupPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;
    this.seriesPartitionSlotsCount = new AtomicInteger(0);

    this.regionGroupMap = new ConcurrentHashMap<>();

    this.schemaPartitionTable = new SchemaPartitionTable();
    this.dataPartitionTable = new DataPartitionTable();

    addMetrics();
  }

  private void addMetrics() {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.REGION.toString(),
            MetricLevel.NORMAL,
            this,
            o -> o.getRegionGroupCount(TConsensusGroupType.SchemaRegion),
            Tag.NAME.toString(),
            storageGroupName,
            Tag.TYPE.toString(),
            TConsensusGroupType.SchemaRegion.toString());
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.REGION.toString(),
            MetricLevel.NORMAL,
            this,
            o -> o.getRegionGroupCount(TConsensusGroupType.DataRegion),
            Tag.NAME.toString(),
            storageGroupName,
            Tag.TYPE.toString(),
            TConsensusGroupType.DataRegion.toString());
    // TODO slot will be updated in the future
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.SLOT.toString(),
            MetricLevel.NORMAL,
            schemaPartitionTable,
            o -> o.getSchemaPartitionMap().size(),
            Tag.NAME.toString(),
            storageGroupName,
            Tag.TYPE.toString(),
            "schemaSlotNumber");
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.SLOT.toString(),
            MetricLevel.NORMAL,
            dataPartitionTable,
            o -> o.getDataPartitionMap().size(),
            Tag.NAME.toString(),
            storageGroupName,
            Tag.TYPE.toString(),
            "dataSlotNumber");
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

  public int getSlotsCount() {
    return seriesPartitionSlotsCount.get();
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
    Map<TConsensusGroupId, AtomicInteger> deltaMap =
        schemaPartitionTable.createSchemaPartition(assignedSchemaPartition);

    // Add counter
    AtomicInteger total = new AtomicInteger(0);
    deltaMap.forEach(
        ((consensusGroupId, delta) -> {
          total.getAndAdd(delta.get());
          regionGroupMap.get(consensusGroupId).addCounter(delta.get());
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
          regionGroupMap.get(consensusGroupId).addCounter(delta.get());
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
            result.add(new Pair<>(regionGroup.getCounter(), consensusGroupId));
          }
        });

    result.sort(Comparator.comparingLong(Pair::getLeft));
    return result;
  }

  public void getRegionInfoList(
      GetRegionInfoListPlan regionsInfoPlan, List<TRegionInfo> regionInfoList) {
    final TShowRegionReq showRegionReq = regionsInfoPlan.getShowRegionReq();
    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          TRegionReplicaSet replicaSet = regionGroup.getReplicaSet();
          if (showRegionReq == null || showRegionReq.getConsensusGroupType() == null) {
            buildTRegionsInfo(regionInfoList, replicaSet, regionGroup);
          } else if (regionsInfoPlan.getShowRegionReq().getConsensusGroupType().ordinal()
              == replicaSet.getRegionId().getType().ordinal()) {
            buildTRegionsInfo(regionInfoList, replicaSet, regionGroup);
          }
        });
  }

  private void buildTRegionsInfo(
      List<TRegionInfo> regionInfoList, TRegionReplicaSet replicaSet, RegionGroup regionGroup) {
    replicaSet
        .getDataNodeLocations()
        .forEach(
            (dataNodeLocation) -> {
              TRegionInfo regionInfo = new TRegionInfo();
              regionInfo.setConsensusGroupId(replicaSet.getRegionId());
              regionInfo.setStorageGroup(storageGroupName);
              if (replicaSet.getRegionId().getType() == TConsensusGroupType.DataRegion) {
                regionInfo.setSeriesSlots(dataPartitionTable.getDataPartitionMap().size());
                regionInfo.setTimeSlots(regionGroup.getCounter());
              } else if (replicaSet.getRegionId().getType() == TConsensusGroupType.SchemaRegion) {
                regionInfo.setSeriesSlots(regionGroup.getCounter());
                regionInfo.setTimeSlots(0);
              }
              regionInfo.setDataNodeId(dataNodeLocation.getDataNodeId());
              regionInfo.setClientRpcIp(dataNodeLocation.getClientRpcEndPoint().getIp());
              regionInfo.setClientRpcPort(dataNodeLocation.getClientRpcEndPoint().getPort());
              // TODO: Wait for data migration. And then add the state
              regionInfo.setStatus(RegionStatus.Up.getStatus());
              regionInfoList.add(regionInfo);
            });
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(isPredeleted, outputStream);
    ReadWriteIOUtils.write(storageGroupName, outputStream);
    ReadWriteIOUtils.write(seriesPartitionSlotsCount.get(), outputStream);

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
    seriesPartitionSlotsCount.set(ReadWriteIOUtils.readInt(inputStream));

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageGroupPartitionTable that = (StorageGroupPartitionTable) o;
    return isPredeleted == that.isPredeleted
        && regionGroupMap.equals(that.regionGroupMap)
        && schemaPartitionTable.equals(that.schemaPartitionTable)
        && dataPartitionTable.equals(that.dataPartitionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isPredeleted, regionGroupMap, schemaPartitionTable, dataPartitionTable);
  }
}
