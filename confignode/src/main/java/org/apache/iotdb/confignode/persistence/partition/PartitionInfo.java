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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.GetRegionIdResp;
import org.apache.iotdb.confignode.consensus.response.GetSeriesSlotListResp;
import org.apache.iotdb.confignode.consensus.response.GetTimeSlotListResp;
import org.apache.iotdb.confignode.consensus.response.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.persistence.metric.PartitionInfoMetrics;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

  /** For Cluster Partition */
  // For allocating Regions
  private final AtomicInteger nextRegionGroupId;
  // Map<StorageGroupName, StorageGroupPartitionInfo>
  private final Map<String, StorageGroupPartitionTable> storageGroupPartitionTables;

  /** For Region-Maintainer */
  // For RegionReplicas' asynchronous management
  private final List<RegionMaintainTask> regionMaintainTaskList;

  private final String snapshotFileName = "partition_info.bin";

  public PartitionInfo() {
    this.nextRegionGroupId = new AtomicInteger(-1);
    this.storageGroupPartitionTables = new ConcurrentHashMap<>();

    this.regionMaintainTaskList = Collections.synchronizedList(new ArrayList<>());
  }

  public int generateNextRegionGroupId() {
    return nextRegionGroupId.incrementAndGet();
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Thread-safely create new StorageGroupPartitionInfo
   *
   * @param plan SetStorageGroupPlan
   * @return SUCCESS_STATUS if the new StorageGroupPartitionInfo is created successfully.
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan plan) {
    String storageGroupName = plan.getSchema().getName();
    StorageGroupPartitionTable storageGroupPartitionTable =
        new StorageGroupPartitionTable(storageGroupName);
    storageGroupPartitionTables.put(storageGroupName, storageGroupPartitionTable);
    MetricService.getInstance()
        .addMetricSet(
            new PartitionInfoMetrics.StorageGroupPartitionTableMetrics(storageGroupPartitionTable));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Thread-safely cache allocation result of new RegionGroups
   *
   * @param plan CreateRegionGroupsPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus createRegionGroups(CreateRegionGroupsPlan plan) {
    TSStatus result;
    AtomicInteger maxRegionId = new AtomicInteger(Integer.MIN_VALUE);

    plan.getRegionGroupMap()
        .forEach(
            (storageGroup, regionReplicaSets) -> {
              storageGroupPartitionTables.get(storageGroup).createRegionGroups(regionReplicaSets);
              regionReplicaSets.forEach(
                  regionReplicaSet ->
                      maxRegionId.set(
                          Math.max(maxRegionId.get(), regionReplicaSet.getRegionId().getId())));
            });

    // To ensure that the nextRegionGroupId is updated correctly when
    // the ConfigNode-followers concurrently processes CreateRegionsPlan,
    // we need to add a synchronization lock here
    synchronized (nextRegionGroupId) {
      if (nextRegionGroupId.get() < maxRegionId.get()) {
        nextRegionGroupId.set(maxRegionId.get());
      }
    }

    result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    return result;
  }

  /**
   * Offer a batch of RegionMaintainTasks for the RegionMaintainer
   *
   * @return SUCCESS_STATUS
   */
  public TSStatus offerRegionMaintainTasks(
      OfferRegionMaintainTasksPlan offerRegionMaintainTasksPlan) {
    synchronized (regionMaintainTaskList) {
      regionMaintainTaskList.addAll(offerRegionMaintainTasksPlan.getRegionMaintainTaskList());
      return RpcUtils.SUCCESS_STATUS;
    }
  }

  /**
   * Poll the head of RegionMaintainTasks from the regionMaintainTaskList after it's executed
   * successfully
   *
   * @return SUCCESS_STATUS
   */
  public TSStatus pollRegionMaintainTask() {
    synchronized (regionMaintainTaskList) {
      regionMaintainTaskList.remove(0);
      return RpcUtils.SUCCESS_STATUS;
    }
  }

  /**
   * Get a deep copy of RegionCleanList for RegionCleaner to maintain cluster RegionReplicas
   *
   * @return A deep copy of RegionCleanList
   */
  public List<RegionMaintainTask> getRegionMaintainEntryList() {
    synchronized (regionMaintainTaskList) {
      return new ArrayList<>(regionMaintainTaskList);
    }
  }

  /**
   * Thread-safely pre-delete the specific StorageGroup
   *
   * @param preDeleteStorageGroupPlan PreDeleteStorageGroupPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus preDeleteStorageGroup(PreDeleteStorageGroupPlan preDeleteStorageGroupPlan) {
    final PreDeleteStorageGroupPlan.PreDeleteType preDeleteType =
        preDeleteStorageGroupPlan.getPreDeleteType();
    final String storageGroup = preDeleteStorageGroupPlan.getStorageGroup();
    StorageGroupPartitionTable storageGroupPartitionTable =
        storageGroupPartitionTables.get(storageGroup);
    if (storageGroupPartitionTable == null) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    switch (preDeleteType) {
      case EXECUTE:
        storageGroupPartitionTable.setPredeleted(true);
        break;
      case ROLLBACK:
        storageGroupPartitionTable.setPredeleted(false);
        break;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Thread-safely delete StorageGroup
   *
   * @param plan DeleteStorageGroupPlan
   */
  public void deleteStorageGroup(DeleteStorageGroupPlan plan) {
    // Clean the StorageGroupTable cache
    storageGroupPartitionTables.remove(plan.getName());
  }

  /**
   * Thread-safely get SchemaPartition
   *
   * @param plan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionPlan plan) {
    AtomicBoolean isAllPartitionsExist = new AtomicBoolean(true);
    // TODO: Replace this map with new SchemaPartition
    Map<String, SchemaPartitionTable> schemaPartition = new ConcurrentHashMap<>();

    if (plan.getPartitionSlotsMap().size() == 0) {
      // Return all SchemaPartitions when the queried PartitionSlots are empty
      storageGroupPartitionTables.forEach(
          (storageGroup, storageGroupPartitionTable) -> {
            if (!storageGroupPartitionTable.isPredeleted()) {
              schemaPartition.put(storageGroup, new SchemaPartitionTable());

              storageGroupPartitionTable.getSchemaPartition(
                  new ArrayList<>(), schemaPartition.get(storageGroup));

              if (schemaPartition.get(storageGroup).getSchemaPartitionMap().isEmpty()) {
                // Remove empty Map
                schemaPartition.remove(storageGroup);
              }
            }
          });
    } else {
      // Return the SchemaPartition for each StorageGroup
      plan.getPartitionSlotsMap()
          .forEach(
              (storageGroup, partitionSlots) -> {
                if (isStorageGroupExisted(storageGroup)) {
                  schemaPartition.put(storageGroup, new SchemaPartitionTable());

                  if (!storageGroupPartitionTables
                      .get(storageGroup)
                      .getSchemaPartition(partitionSlots, schemaPartition.get(storageGroup))) {
                    isAllPartitionsExist.set(false);
                  }

                  if (schemaPartition.get(storageGroup).getSchemaPartitionMap().isEmpty()) {
                    // Remove empty Map
                    schemaPartition.remove(storageGroup);
                  }
                } else {
                  isAllPartitionsExist.set(false);
                }
              });
    }

    return new SchemaPartitionResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        isAllPartitionsExist.get(),
        schemaPartition);
  }

  /**
   * Thread-safely get DataPartition
   *
   * @param plan DataPartitionPlan with partitionSlotsMap
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionPlan plan) {
    AtomicBoolean isAllPartitionsExist = new AtomicBoolean(true);
    // TODO: Replace this map whit new DataPartition
    Map<String, DataPartitionTable> dataPartition = new ConcurrentHashMap<>();

    plan.getPartitionSlotsMap()
        .forEach(
            (storageGroup, partitionSlots) -> {
              if (isStorageGroupExisted(storageGroup)) {
                dataPartition.put(storageGroup, new DataPartitionTable());

                if (!storageGroupPartitionTables
                    .get(storageGroup)
                    .getDataPartition(partitionSlots, dataPartition.get(storageGroup))) {
                  isAllPartitionsExist.set(false);
                }

                if (dataPartition.get(storageGroup).getDataPartitionMap().isEmpty()) {
                  // Remove empty Map
                  dataPartition.remove(storageGroup);
                }
              } else {
                isAllPartitionsExist.set(false);
              }
            });

    return new DataPartitionResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        isAllPartitionsExist.get(),
        dataPartition);
  }

  /**
   * Checks whether the specified DataPartition has a predecessor and returns if it does
   *
   * @param storageGroup StorageGroupName
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @param timePartitionInterval Time partition interval
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPrecededDataPartition(
      String storageGroup,
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      long timePartitionInterval) {
    if (storageGroupPartitionTables.containsKey(storageGroup)) {
      return storageGroupPartitionTables
          .get(storageGroup)
          .getPrecededDataPartition(seriesPartitionSlot, timePartitionSlot, timePartitionInterval);
    } else {
      return null;
    }
  }

  private boolean isStorageGroupExisted(String storageGroup) {
    final StorageGroupPartitionTable storageGroupPartitionTable =
        storageGroupPartitionTables.get(storageGroup);
    return storageGroupPartitionTable != null && !storageGroupPartitionTable.isPredeleted();
  }

  /**
   * Create SchemaPartition
   *
   * @param plan CreateSchemaPartitionPlan with SchemaPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS
   */
  public TSStatus createSchemaPartition(CreateSchemaPartitionPlan plan) {
    plan.getAssignedSchemaPartition()
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
   * @param plan CreateDataPartitionPlan with DataPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS
   */
  public TSStatus createDataPartition(CreateDataPartitionPlan plan) {
    plan.getAssignedDataPartition()
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
    Map<String, SchemaPartitionTable> schemaPartitionMap = new ConcurrentHashMap<>();

    matchedStorageGroups.stream()
        .filter(this::isStorageGroupExisted)
        .forEach(
            storageGroup -> {
              schemaPartitionMap.put(storageGroup, new SchemaPartitionTable());

              storageGroupPartitionTables
                  .get(storageGroup)
                  .getSchemaPartition(new ArrayList<>(), schemaPartitionMap.get(storageGroup));

              if (schemaPartitionMap.get(storageGroup).getSchemaPartitionMap().isEmpty()) {
                // Remove empty Map
                schemaPartitionMap.remove(storageGroup);
              }
            });

    schemaNodeManagementResp.setSchemaPartition(schemaPartitionMap);
    schemaNodeManagementResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return schemaNodeManagementResp;
  }

  /** Get region information */
  public DataSet getRegionInfoList(GetRegionInfoListPlan regionsInfoPlan) {
    RegionInfoListResp regionResp = new RegionInfoListResp();
    List<TRegionInfo> regionInfoList = new Vector<>();
    if (storageGroupPartitionTables.isEmpty()) {
      regionResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      regionResp.setRegionInfoList(new ArrayList<>());
      return regionResp;
    }
    TShowRegionReq showRegionReq = regionsInfoPlan.getShowRegionReq();
    final List<String> storageGroups =
        showRegionReq != null ? showRegionReq.getStorageGroups() : null;
    storageGroupPartitionTables.forEach(
        (storageGroup, storageGroupPartitionTable) -> {
          if (storageGroups != null && !storageGroups.contains(storageGroup)) {
            return;
          }
          regionInfoList.addAll(storageGroupPartitionTable.getRegionInfoList(regionsInfoPlan));
        });
    regionInfoList.sort(
        (o1, o2) ->
            o1.getConsensusGroupId().getId() != o2.getConsensusGroupId().getId()
                ? o1.getConsensusGroupId().getId() - o2.getConsensusGroupId().getId()
                : o1.getDataNodeId() - o2.getDataNodeId());
    regionResp.setRegionInfoList(regionInfoList);
    regionResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return regionResp;
  }

  /**
   * Update the location info of given regionId
   *
   * @param req UpdateRegionLocationReq
   * @return TSStatus
   */
  public TSStatus updateRegionLocation(UpdateRegionLocationPlan req) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    TConsensusGroupId regionId = req.getRegionId();
    TDataNodeLocation oldNode = req.getOldNode();
    TDataNodeLocation newNode = req.getNewNode();
    storageGroupPartitionTables
        .values()
        .forEach(s -> s.updateRegionLocation(regionId, oldNode, newNode));

    return status;
  }

  /**
   * get storage group for region
   *
   * @param regionId regionId
   * @return storage group name
   */
  public String getRegionStorageGroup(TConsensusGroupId regionId) {
    Optional<StorageGroupPartitionTable> sgPartitionTableOptional =
        storageGroupPartitionTables.values().stream()
            .filter(s -> s.containRegion(regionId))
            .findFirst();
    return sgPartitionTableOptional
        .map(StorageGroupPartitionTable::getStorageGroupName)
        .orElse(null);
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
   * @return Deep copy of all Regions' RegionReplicaSet
   */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    List<TRegionReplicaSet> result = new ArrayList<>();
    storageGroupPartitionTables
        .values()
        .forEach(
            storageGroupPartitionTable ->
                result.addAll(storageGroupPartitionTable.getAllReplicaSets()));
    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @param type The specified TConsensusGroupType
   * @return Deep copy of all Regions' RegionReplicaSet with the specified TConsensusGroupType
   */
  public List<TRegionReplicaSet> getAllReplicaSets(TConsensusGroupType type) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    storageGroupPartitionTables
        .values()
        .forEach(
            storageGroupPartitionTable ->
                result.addAll(storageGroupPartitionTable.getAllReplicaSets(type)));
    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @param storageGroup The specified StorageGroup
   * @return All Regions' RegionReplicaSet of the specified StorageGroup
   */
  public List<TRegionReplicaSet> getAllReplicaSets(String storageGroup) {
    if (storageGroupPartitionTables.containsKey(storageGroup)) {
      return storageGroupPartitionTables.get(storageGroup).getAllReplicaSets();
    } else {
      return new ArrayList<>();
    }
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

    return storageGroupPartitionTables.get(storageGroup).getRegionGroupCount(type);
  }

  public int getAssignedSeriesPartitionSlotsCount(String storageGroup) {
    return storageGroupPartitionTables.get(storageGroup).getAssignedSeriesPartitionSlotsCount();
  }

  /**
   * Get the DataNodes who contain the specific StorageGroup's Schema or Data
   *
   * @param storageGroup The specific StorageGroup's name
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getStorageGroupRelatedDataNodes(
      String storageGroup, TConsensusGroupType type) {
    return storageGroupPartitionTables.get(storageGroup).getStorageGroupRelatedDataNodes(type);
  }

  /**
   * Only leader use this interface.
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return The StorageGroup's Running or Available Regions that sorted by the number of allocated
   *     slots
   */
  public List<Pair<Long, TConsensusGroupId>> getRegionGroupSlotsCounter(
      String storageGroup, TConsensusGroupType type) {
    return storageGroupPartitionTables.get(storageGroup).getRegionGroupSlotsCounter(type);
  }

  /**
   * Update RegionGroup-related metric
   *
   * @param type SchemaRegion or DataRegion
   * @return the number of SchemaRegion or DataRegion
   */
  public int updateRegionGroupMetric(TConsensusGroupType type) {
    Set<RegionGroup> regionGroups = new HashSet<>();
    for (Map.Entry<String, StorageGroupPartitionTable> entry :
        storageGroupPartitionTables.entrySet()) {
      regionGroups.addAll(entry.getValue().getRegionGroups(type));
    }
    int result = regionGroups.size();
    // datanode location -> region number
    Map<TDataNodeLocation, Integer> dataNodeLocationIntegerMap = new HashMap<>();
    for (RegionGroup regionGroup : regionGroups) {
      TRegionReplicaSet regionReplicaSet = regionGroup.getReplicaSet();
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        if (!dataNodeLocationIntegerMap.containsKey(dataNodeLocation)) {
          dataNodeLocationIntegerMap.put(dataNodeLocation, 0);
        }
        dataNodeLocationIntegerMap.put(
            dataNodeLocation, dataNodeLocationIntegerMap.get(dataNodeLocation) + 1);
      }
    }
    for (Map.Entry<TDataNodeLocation, Integer> entry : dataNodeLocationIntegerMap.entrySet()) {
      TDataNodeLocation dataNodeLocation = entry.getKey();
      String name =
          "EndPoint("
              + dataNodeLocation.getClientRpcEndPoint().ip
              + ":"
              + dataNodeLocation.getClientRpcEndPoint().port
              + ")";
      // TODO: this metric can be optimized
      MetricService.getInstance()
          .getOrCreateGauge(
              Metric.REGION.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              type.toString())
          .set(dataNodeLocationIntegerMap.get(dataNodeLocation));
    }
    return result;
  }

  @Override
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

      // serialize regionCleanList
      ReadWriteIOUtils.write(regionMaintainTaskList.size(), fileOutputStream);
      for (RegionMaintainTask task : regionMaintainTaskList) {
        task.serialize(fileOutputStream, protocol);
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
        if (storageGroup == null) {
          throw new IOException("Failed to load snapshot because get null StorageGroup name");
        }
        StorageGroupPartitionTable storageGroupPartitionTable =
            new StorageGroupPartitionTable(storageGroup);
        storageGroupPartitionTable.deserialize(fileInputStream, protocol);
        storageGroupPartitionTables.put(storageGroup, storageGroupPartitionTable);
      }

      // restore deletedRegionSet
      length = ReadWriteIOUtils.readInt(fileInputStream);
      for (int i = 0; i < length; i++) {
        RegionMaintainTask task = RegionMaintainTask.Factory.create(fileInputStream, protocol);
        regionMaintainTaskList.add(task);
      }
    }
  }

  public DataSet getRegionId(GetRegionIdPlan plan) {
    if (!isStorageGroupExisted(plan.getStorageGroup())) {
      return new GetRegionIdResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
    }
    StorageGroupPartitionTable sgPartitionTable =
        storageGroupPartitionTables.get(plan.getStorageGroup());
    return new GetRegionIdResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        sgPartitionTable.getRegionId(
            plan.getPartitionType(), plan.getSeriesSlotId(), plan.getTimeSlotId()));
  }

  public DataSet getTimeSlotList(GetTimeSlotListPlan plan) {
    if (!isStorageGroupExisted(plan.getStorageGroup())) {
      return new GetTimeSlotListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
    }
    StorageGroupPartitionTable sgPartitionTable =
        storageGroupPartitionTables.get(plan.getStorageGroup());
    return new GetTimeSlotListResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        sgPartitionTable.getTimeSlotList(
            plan.getSeriesSlotId(), plan.getStartTime(), plan.getEndTime()));
  }

  public DataSet getSeriesSlotList(GetSeriesSlotListPlan plan) {
    if (!isStorageGroupExisted(plan.getStorageGroup())) {
      return new GetSeriesSlotListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
    }
    StorageGroupPartitionTable sgPartitionTable =
        storageGroupPartitionTables.get(plan.getStorageGroup());
    return new GetSeriesSlotListResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        sgPartitionTable.getSeriesSlotList(plan.getPartitionType()));
  }

  public int getStorageGroupPartitionTableSize() {
    return storageGroupPartitionTables.size();
  }

  public void clear() {
    nextRegionGroupId.set(-1);
    storageGroupPartitionTables.clear();
    regionMaintainTaskList.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionInfo that = (PartitionInfo) o;
    return nextRegionGroupId.get() == that.nextRegionGroupId.get()
        && storageGroupPartitionTables.equals(that.storageGroupPartitionTables)
        && regionMaintainTaskList.equals(that.regionMaintainTaskList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nextRegionGroupId, storageGroupPartitionTables, regionMaintainTaskList);
  }
}
