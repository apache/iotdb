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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The PartitionInfo stores cluster PartitionTable. The PartitionTable including: 1. regionMap:
 * location of Region member 2. schemaPartition: location of schema 3. dataPartition: location of
 * data
 */
public class PartitionInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfo.class);

  // For allocating Regions
  private final AtomicInteger nextRegionGroupId;
  // Map<StorageGroupName, StorageGroupPartitionInfo>
  private final ConcurrentHashMap<String, StorageGroupPartitionTable> storageGroupPartitionTables;

  private final Set<TRegionReplicaSet> deletedRegionSet;

  private final String snapshotFileName = "partition_info.bin";

  public PartitionInfo() {
    this.storageGroupPartitionTables = new ConcurrentHashMap<>();
    this.nextRegionGroupId = new AtomicInteger(0);

    // Ensure that the PartitionTables of the StorageGroups who've been logically deleted
    // are unreadable and un-writable
    // For RegionCleaner
    this.deletedRegionSet = Collections.synchronizedSet(new HashSet<>());
    addMetrics();
  }

  private void addMetrics() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.PARTITION_TABLE.toString(),
              MetricLevel.CORE,
              storageGroupPartitionTables,
              Map::size,
              Tag.NAME.toString(),
              "number");
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.REGION.toString(),
              MetricLevel.CORE,
              nextRegionGroupId,
              AtomicInteger::get,
              Tag.NAME.toString(),
              "total");
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.REGION.toString(),
              MetricLevel.IMPORTANT,
              this,
              o -> o.getTotalRegionCount(TConsensusGroupType.SchemaRegion),
              Tag.NAME.toString(),
              "schemaRegion");
      MetricsService.getInstance()
          .getMetricManager()
          .getOrCreateAutoGauge(
              Metric.REGION.toString(),
              MetricLevel.IMPORTANT,
              this,
              o -> o.getTotalRegionCount(TConsensusGroupType.DataRegion),
              Tag.NAME.toString(),
              "dataRegion");
    }
  }

  public int generateNextRegionGroupId() {
    return nextRegionGroupId.getAndIncrement();
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Thread-safely create new StorageGroupPartitionInfo
   *
   * @param req SetStorageGroupReq
   * @return SUCCESS_STATUS if the new StorageGroupPartitionInfo is created successfully.
   */
  public TSStatus setStorageGroup(SetStorageGroupReq req) {
    storageGroupPartitionTables.put(req.getSchema().getName(), new StorageGroupPartitionTable());

    LOGGER.info("Successfully set StorageGroup: {}", req.getSchema());

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Thread-safely cache allocation result of new Regions
   *
   * @param req CreateRegionsReq
   * @return SUCCESS_STATUS
   */
  public TSStatus createRegions(CreateRegionsReq req) {
    TSStatus result;
    AtomicInteger maxRegionId = new AtomicInteger(Integer.MIN_VALUE);

    req.getRegionMap()
        .forEach(
            (storageGroup, regionReplicaSets) -> {
              storageGroupPartitionTables.get(storageGroup).createRegions(regionReplicaSets);
              regionReplicaSets.forEach(
                  regionReplicaSet ->
                      maxRegionId.set(
                          Math.max(maxRegionId.get(), regionReplicaSet.getRegionId().getId())));
            });

    if (nextRegionGroupId.get() < maxRegionId.get()) {
      // In this case, at least one Region is created by the leader ConfigNode,
      // so the nextRegionGroupID of the followers needs to be added
      nextRegionGroupId.getAndAdd(req.getRegionMap().size());
    }

    result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    return result;
  }

  /**
   * Thread-safely pre-delete the specific StorageGroup
   *
   * @param preDeleteStorageGroupReq PreDeleteStorageGroupReq
   * @return SUCCESS_STATUS
   */
  public TSStatus preDeleteStorageGroup(PreDeleteStorageGroupReq preDeleteStorageGroupReq) {
    final PreDeleteStorageGroupReq.PreDeleteType preDeleteType =
        preDeleteStorageGroupReq.getPreDeleteType();
    final String storageGroup = preDeleteStorageGroupReq.getStorageGroup();
    switch (preDeleteType) {
      case EXECUTE:
        storageGroupPartitionTables.get(storageGroup).setPredeleted(true);
        break;
      case ROLLBACK:
        storageGroupPartitionTables.get(storageGroup).setPredeleted(false);
        break;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Thread-safely delete StorageGroup
   *
   * @param req DeleteRegionsReq
   */
  public void deleteStorageGroup(DeleteStorageGroupReq req) {
    // Cache RegionReplicaSets
    synchronized (deletedRegionSet) {
      deletedRegionSet.addAll(storageGroupPartitionTables.get(req.getName()).getAllReplicaSets());
    }
    // Clean the cache
    storageGroupPartitionTables.remove(req.getName());
  }

  /** @return The Regions that should be deleted among the DataNodes */
  public Set<TRegionReplicaSet> getDeletedRegionSet() {
    synchronized (deletedRegionSet) {
      return deletedRegionSet;
    }
  }

  /**
   * Thread-safely get SchemaPartition
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionReq req) {
    AtomicBoolean isAllPartitionsExist = new AtomicBoolean(true);
    // TODO: Replace this map whit new SchemaPartition
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartition =
        new ConcurrentHashMap<>();

    if (req.getPartitionSlotsMap().size() == 0) {
      // Return all SchemaPartitions when the queried PartitionSlots are empty
      storageGroupPartitionTables.forEach(
          (storageGroup, storageGroupPartitionTable) -> {
            if (!storageGroupPartitionTable.isPredeleted()) {
              schemaPartition.put(storageGroup, new ConcurrentHashMap<>());
              storageGroupPartitionTable.getSchemaPartition(
                  new ArrayList<>(), schemaPartition.get(storageGroup));

              if (schemaPartition.get(storageGroup).size() == 0) {
                // Remove empty Map
                schemaPartition.remove(storageGroup);
              }
            }
          });
    } else {
      // Return the SchemaPartition for each StorageGroup
      req.getPartitionSlotsMap()
          .forEach(
              (storageGroup, partitionSlots) -> {
                if (isStorageGroupExisted(storageGroup)) {
                  schemaPartition.put(storageGroup, new ConcurrentHashMap<>());

                  if (!storageGroupPartitionTables
                      .get(storageGroup)
                      .getSchemaPartition(partitionSlots, schemaPartition.get(storageGroup))) {
                    isAllPartitionsExist.set(false);
                  }

                  if (schemaPartition.get(storageGroup).size() == 0) {
                    // Remove empty Map
                    schemaPartition.remove(storageGroup);
                  }
                }
              });
    }

    SchemaPartitionResp schemaPartitionResp = new SchemaPartitionResp();
    schemaPartitionResp.setAllPartitionsExist(isAllPartitionsExist.get());
    // Notice: SchemaPartition contains two redundant structural parameters currently.
    schemaPartitionResp.setSchemaPartition(
        new SchemaPartition(
            schemaPartition,
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum()));
    schemaPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return schemaPartitionResp;
  }

  /**
   * Thread-safely get DataPartition
   *
   * @param req DataPartitionPlan with partitionSlotsMap
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionReq req) {
    AtomicBoolean isAllPartitionsExist = new AtomicBoolean(true);
    // TODO: Replace this map whit new DataPartition
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dataPartition = new ConcurrentHashMap<>();

    req.getPartitionSlotsMap()
        .forEach(
            (storageGroup, partitionSlots) -> {
              if (isStorageGroupExisted(storageGroup)) {
                dataPartition.put(storageGroup, new ConcurrentHashMap<>());
                if (!storageGroupPartitionTables
                    .get(storageGroup)
                    .getDataPartition(partitionSlots, dataPartition.get(storageGroup))) {
                  isAllPartitionsExist.set(false);
                }

                if (dataPartition.get(storageGroup).size() == 0) {
                  // Remove empty Map
                  dataPartition.remove(storageGroup);
                }
              }
            });

    DataPartitionResp dataPartitionResp = new DataPartitionResp();
    dataPartitionResp.setAllPartitionsExist(isAllPartitionsExist.get());
    // Notice: DataPartition contains two redundant structural parameters currently.
    dataPartitionResp.setDataPartition(
        new DataPartition(
            dataPartition,
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum()));
    dataPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return dataPartitionResp;
  }

  private boolean isStorageGroupExisted(String storageGroup) {
    final StorageGroupPartitionTable storageGroupPartitionTable =
        storageGroupPartitionTables.get(storageGroup);
    return storageGroupPartitionTable != null && !storageGroupPartitionTable.isPredeleted();
  }

  /**
   * Create SchemaPartition
   *
   * @param req CreateSchemaPartitionPlan with SchemaPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS
   */
  public TSStatus createSchemaPartition(CreateSchemaPartitionReq req) {
    req.getAssignedSchemaPartition()
        .forEach(
            (storageGroup, schemaPartitionTable) -> {
              if (isStorageGroupExisted(storageGroup)) {
                storageGroupPartitionTables
                    .get(storageGroup)
                    .createSchemaPartition(schemaPartitionTable);
              }
            });

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Create DataPartition
   *
   * @param req CreateDataPartitionPlan with DataPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS
   */
  public TSStatus createDataPartition(CreateDataPartitionReq req) {
    req.getAssignedDataPartition()
        .forEach(
            (storageGroup, dataPartitionTable) -> {
              if (isStorageGroupExisted(storageGroup)) {
                storageGroupPartitionTables
                    .get(storageGroup)
                    .createDataPartition(dataPartitionTable);
              }
            });

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /** Get SchemaNodeManagementPartition through matched storageGroup */
  public DataSet getSchemaNodeManagementPartition(List<String> matchedStorageGroups) {
    SchemaNodeManagementResp schemaNodeManagementResp = new SchemaNodeManagementResp();
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
        new ConcurrentHashMap<>();

    matchedStorageGroups.stream()
        .filter(this::isStorageGroupExisted)
        .forEach(
            storageGroup -> {
              schemaPartitionMap.put(storageGroup, new ConcurrentHashMap<>());
              storageGroupPartitionTables
                  .get(storageGroup)
                  .getSchemaPartition(new ArrayList<>(), schemaPartitionMap.get(storageGroup));

              if (schemaPartitionMap.get(storageGroup).size() == 0) {
                // Remove empty Map
                schemaPartitionMap.remove(storageGroup);
              }
            });

    schemaNodeManagementResp.setSchemaPartition(
        new SchemaPartition(
            schemaPartitionMap,
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum()));
    schemaNodeManagementResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return schemaNodeManagementResp;
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only Leader use this interface. Filter unassigned SchemaPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, List<TSeriesPartitionSlot>>
   * @return Map<StorageGroupName, List<TSeriesPartitionSlot>>, SchemaPartitionSlots that is not
   *     assigned in partitionSlotsMap
   */
  public Map<String, List<TSeriesPartitionSlot>> filterUnassignedSchemaPartitionSlots(
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<TSeriesPartitionSlot>> result = new ConcurrentHashMap<>();

    partitionSlotsMap.forEach(
        (storageGroup, partitionSlots) -> {
          if (isStorageGroupExisted(storageGroup)) {
            result.put(
                storageGroup,
                storageGroupPartitionTables
                    .get(storageGroup)
                    .filterUnassignedSchemaPartitionSlots(partitionSlots));
          }
        });

    return result;
  }

  /**
   * Only Leader use this interface. Filter unassigned SchemaPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<TSeriesPartitionSlot,
   *     List<TTimePartitionSlot>>>
   * @return Map<StorageGroupName, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>,
   *     DataPartitionSlots that is not assigned in partitionSlotsMap
   */
  public Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      filterUnassignedDataPartitionSlots(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result =
        new ConcurrentHashMap<>();

    partitionSlotsMap.forEach(
        (storageGroup, partitionSlots) -> {
          if (isStorageGroupExisted(storageGroup)) {
            result.put(
                storageGroup,
                storageGroupPartitionTables
                    .get(storageGroup)
                    .filterUnassignedDataPartitionSlots(partitionSlots));
          }
        });

    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @return All Regions' RegionReplicaSet
   */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    List<TRegionReplicaSet> result = new Vector<>();
    storageGroupPartitionTables
        .values()
        .forEach(
            storageGroupPartitionTable ->
                result.addAll(storageGroupPartitionTable.getAllReplicaSets()));
    return result;
  }

  /**
   * Only leader use this interface. Get the number of Regions currently owned by the specific
   * StorageGroup
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return Number of Regions currently owned by the specific StorageGroup
   * @throws StorageGroupNotExistsException When the specific StorageGroup doesn't exist
   */
  public int getRegionCount(String storageGroup, TConsensusGroupType type)
      throws StorageGroupNotExistsException {
    if (!isStorageGroupExisted(storageGroup)) {
      throw new StorageGroupNotExistsException(storageGroup);
    }

    return storageGroupPartitionTables.get(storageGroup).getRegionCount(type);
  }

  /**
   * Only leader use this interface. Contending the Region allocation particle
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return True when successfully get the allocation particle, false otherwise
   */
  public boolean getRegionAllocationParticle(String storageGroup, TConsensusGroupType type) {
    return storageGroupPartitionTables.get(storageGroup).getRegionAllocationParticle(type);
  }

  /**
   * Only leader use this interface.
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return The specific StorageGroup's Regions that sorted by the number of allocated slots
   */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionSlotsCounter(
      String storageGroup, TConsensusGroupType type) {
    return storageGroupPartitionTables.get(storageGroup).getSortedRegionSlotsCounter(type);
  }

  /**
   * Get total region number
   * @param type SchemaRegion or DataRegion
   * @return the number of SchemaRegion or DataRegion
   */
  private int getTotalRegionCount(TConsensusGroupType type){
    Set<RegionGroup> regionGroups = new HashSet<>();
    for(Map.Entry<String, StorageGroupPartitionTable> entry : storageGroupPartitionTables.entrySet()) {
      regionGroups.addAll(entry.getValue().getRegion(type));
    }
    return regionGroups.size();
  }

  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    // prevents temporary files from being damaged and cannot be deleted, which affects the next
    // snapshot operation.
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    // TODO: Lock PartitionInfo
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      // serialize nextRegionGroupId
      ReadWriteIOUtils.write(nextRegionGroupId.get(), fileOutputStream);
      // serialize StorageGroupPartitionTable
      ReadWriteIOUtils.write(storageGroupPartitionTables.size(), fileOutputStream);
      for (Map.Entry<String, StorageGroupPartitionTable> storageGroupPartitionTableEntry :
          storageGroupPartitionTables.entrySet()) {
        ReadWriteIOUtils.write(storageGroupPartitionTableEntry.getKey(), fileOutputStream);
        storageGroupPartitionTableEntry.getValue().serialize(fileOutputStream, protocol);
      }
      // serialize deletedRegionSet
      ReadWriteIOUtils.write(deletedRegionSet.size(), fileOutputStream);
      for (TRegionReplicaSet regionReplicaSet : deletedRegionSet) {
        regionReplicaSet.write(protocol);
      }

      // write to file
      fileOutputStream.flush();
      fileOutputStream.close();
      // rename file
      return tmpFile.renameTo(snapshotFile);
    } finally {
      // with or without success, delete temporary files anyway
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
    }
  }

  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {

    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    // TODO: Lock PartitionInfo
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileInputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);
      // before restoring a snapshot, clear all old data
      clear();
      // start to restore
      nextRegionGroupId.set(ReadWriteIOUtils.readInt(fileInputStream));

      // restore StorageGroupPartitionTable
      int length = ReadWriteIOUtils.readInt(fileInputStream);
      for (int i = 0; i < length; i++) {
        String storageGroup = ReadWriteIOUtils.readString(fileInputStream);
        StorageGroupPartitionTable storageGroupPartitionTable = new StorageGroupPartitionTable();
        storageGroupPartitionTable.deserialize(fileInputStream, protocol);
        storageGroupPartitionTables.put(storageGroup, storageGroupPartitionTable);
      }
      // restore deletedRegionSet
      length = ReadWriteIOUtils.readInt(fileInputStream);
      for (int i = 0; i < length; i++) {
        TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
        regionReplicaSet.read(protocol);
        deletedRegionSet.add(regionReplicaSet);
      }
    }
  }

  public void clear() {
    nextRegionGroupId.set(0);
    storageGroupPartitionTables.clear();
    deletedRegionSet.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionInfo that = (PartitionInfo) o;
    return storageGroupPartitionTables.equals(that.storageGroupPartitionTables)
        && deletedRegionSet.equals(that.deletedRegionSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroupPartitionTables, deletedRegionSet);
  }
}
