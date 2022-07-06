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
import org.apache.iotdb.common.rpc.thrift.TRegionInfo;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
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
  // The name of storage group
  private String storageGroupName;

  // Total number of SeriesPartitionSlots occupied by schema,
  // determines whether a new Region needs to be created
  private final AtomicInteger seriesPartitionSlotsCount;

  // Region allocation particle
  private final AtomicBoolean schemaRegionParticle;
  private final AtomicBoolean dataRegionParticle;

  // Region
  private final Map<TConsensusGroupId, RegionGroup> regionGroupMap;
  // SchemaPartition
  private final SchemaPartitionTable schemaPartitionTable;
  // DataPartition
  private final DataPartitionTable dataPartitionTable;

  public StorageGroupPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;
    this.seriesPartitionSlotsCount = new AtomicInteger(0);

    this.schemaRegionParticle = new AtomicBoolean(true);
    this.dataRegionParticle = new AtomicBoolean(true);
    this.regionGroupMap = new ConcurrentHashMap<>();

    this.schemaPartitionTable = new SchemaPartitionTable();
    this.dataPartitionTable = new DataPartitionTable();

    addMetrics();
  }

  private void addMetrics() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.REGION.toString(),
              MetricLevel.NORMAL,
              this,
              o -> o.getRegionGroupCount(TConsensusGroupType.SchemaRegion),
              Tag.NAME.toString(),
              storageGroupName,
              Tag.TYPE.toString(),
              TConsensusGroupType.SchemaRegion.toString());
      MetricsService.getInstance()
          .getMetricManager()
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
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.SLOT.toString(),
              MetricLevel.NORMAL,
              schemaPartitionTable,
              o -> o.getSchemaPartitionMap().size(),
              Tag.NAME.toString(),
              storageGroupName,
              Tag.TYPE.toString(),
              "schemaSlotNumber");
      MetricsService.getInstance()
          .getMetricManager()
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

  /**
   * Only leader use this interface. Contending the Region allocation particle.
   *
   * @param type SchemaRegion or DataRegion
   * @return True when successfully get the allocation particle, false otherwise
   */
  public boolean contendRegionAllocationParticle(TConsensusGroupType type) {
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
   * Only leader use this interface. Put back the Region allocation particle.
   *
   * @param type SchemaRegion or DataRegion
   */
  public void putBackRegionAllocationParticle(TConsensusGroupType type) {
    switch (type) {
      case SchemaRegion:
        schemaRegionParticle.set(true);
      case DataRegion:
        dataRegionParticle.set(true);
    }
  }

  /**
   * Only leader use this interface. Get the Region allocation particle.
   *
   * @param type SchemaRegion or DataRegion
   */
  public boolean getRegionAllocationParticle(TConsensusGroupType type) {
    switch (type) {
      case SchemaRegion:
        return schemaRegionParticle.get();
      case DataRegion:
        return dataRegionParticle.get();
      default:
        return false;
    }
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
    regionGroupMap.forEach(
        (consensusGroupId, regionGroup) -> {
          TRegionReplicaSet replicaSet = regionGroup.getReplicaSet();
          if (regionsInfoPlan.getRegionType() == null) {
            buildTRegionsInfo(regionInfoList, replicaSet, regionGroup);
          } else if (regionsInfoPlan.getRegionType().ordinal()
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
              TRegionInfo tRegionInfoList = new TRegionInfo();
              tRegionInfoList.setConsensusGroupId(replicaSet.getRegionId());
              tRegionInfoList.setStorageGroup(storageGroupName);
              if (replicaSet.getRegionId().getType() == TConsensusGroupType.DataRegion) {
                tRegionInfoList.setSeriesSlots(dataPartitionTable.getDataPartitionMap().size());
                tRegionInfoList.setTimeSlots(regionGroup.getCounter());
              } else if (replicaSet.getRegionId().getType() == TConsensusGroupType.SchemaRegion) {
                tRegionInfoList.setSeriesSlots(regionGroup.getCounter());
                tRegionInfoList.setTimeSlots(0);
              }
              tRegionInfoList.setDataNodeId(dataNodeLocation.getDataNodeId());
              tRegionInfoList.setClientRpcIp(dataNodeLocation.getClientRpcEndPoint().getIp());
              tRegionInfoList.setClientRpcPort(dataNodeLocation.getClientRpcEndPoint().getPort());
              // TODO: Wait for data migration. And then add the state
              tRegionInfoList.setStatus(RegionStatus.Up.getStatus());
              regionInfoList.add(tRegionInfoList);
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
