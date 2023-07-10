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
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.partition.CountTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.response.partition.CountTimeSlotListResp;
import org.apache.iotdb.confignode.consensus.response.partition.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetRegionIdResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetSeriesSlotListResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetTimeSlotListResp;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.common.DataSet;
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.stream.Collectors;

/**
 * The PartitionInfo stores cluster PartitionTable. The PartitionTable including: 1. regionMap:
 * location of Region member 2. schemaPartition: location of schemaengine 3. dataPartition: location
 * of data
 */
public class PartitionInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfo.class);

  // Allocate 8MB buffer for load snapshot of PartitionInfo
  private static final int PARTITION_TABLE_BUFFER_SIZE = 32 * 1024 * 1024;

  /** For Cluster Partition */
  // For allocating Regions
  private final AtomicInteger nextRegionGroupId;
  // Map<DatabaseName, DatabasePartitionInfo>
  private final Map<String, DatabasePartitionTable> databasePartitionTables;

  /** For Region-Maintainer */
  // For RegionReplicas' asynchronous management
  private final List<RegionMaintainTask> regionMaintainTaskList;

  private static final String SNAPSHOT_FILENAME = "partition_info.bin";

  public PartitionInfo() {
    this.nextRegionGroupId = new AtomicInteger(-1);
    this.databasePartitionTables = new ConcurrentHashMap<>();

    this.regionMaintainTaskList = Collections.synchronizedList(new ArrayList<>());
  }

  public int generateNextRegionGroupId() {
    return nextRegionGroupId.incrementAndGet();
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Thread-safely create new DatabasePartitionTable
   *
   * @param plan DatabaseSchemaPlan
   * @return SUCCESS_STATUS if the new DatabasePartitionTable is created successfully.
   */
  public TSStatus createDatabase(DatabaseSchemaPlan plan) {
    String databaseName = plan.getSchema().getName();
    DatabasePartitionTable databasePartitionTable = new DatabasePartitionTable(databaseName);
    databasePartitionTables.put(databaseName, databasePartitionTable);
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
              databasePartitionTables.get(storageGroup).createRegionGroups(regionReplicaSets);
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
   * Poll the head of RegionMaintainTasks of target regions from regionMaintainTaskList after they
   * are executed successfully. Tasks of each region group are treated as single independent queue.
   *
   * @param plan provides target region ids
   * @return SUCCESS_STATUS
   */
  public TSStatus pollSpecificRegionMaintainTask(PollSpecificRegionMaintainTaskPlan plan) {
    synchronized (regionMaintainTaskList) {
      Set<TConsensusGroupId> removingRegionIdSet = new HashSet<>(plan.getRegionIdSet());
      TConsensusGroupId regionId;
      for (int i = 0; i < regionMaintainTaskList.size(); i++) {
        regionId = regionMaintainTaskList.get(i).getRegionId();
        if (removingRegionIdSet.contains(regionId)) {
          regionMaintainTaskList.remove(i);
          removingRegionIdSet.remove(regionId);
          i--;
        }
        if (removingRegionIdSet.isEmpty()) {
          break;
        }
      }
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
   * @param preDeleteDatabasePlan PreDeleteStorageGroupPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus preDeleteDatabase(PreDeleteDatabasePlan preDeleteDatabasePlan) {
    final PreDeleteDatabasePlan.PreDeleteType preDeleteType =
        preDeleteDatabasePlan.getPreDeleteType();
    final String storageGroup = preDeleteDatabasePlan.getStorageGroup();
    DatabasePartitionTable databasePartitionTable = databasePartitionTables.get(storageGroup);
    if (databasePartitionTable == null) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    switch (preDeleteType) {
      case EXECUTE:
        databasePartitionTable.setPreDeleted(true);
        break;
      case ROLLBACK:
        databasePartitionTable.setPreDeleted(false);
        break;
      default:
        break;
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public boolean isDatabasePreDeleted(String database) {
    DatabasePartitionTable databasePartitionTable = databasePartitionTables.get(database);
    return databasePartitionTable != null && !databasePartitionTable.isNotPreDeleted();
  }

  /**
   * Thread-safely delete StorageGroup
   *
   * @param plan DeleteStorageGroupPlan
   */
  public void deleteDatabase(DeleteDatabasePlan plan) {
    // Clean the StorageGroupTable cache
    databasePartitionTables.remove(plan.getName());
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
      databasePartitionTables.forEach(
          (storageGroup, databasePartitionTable) -> {
            if (databasePartitionTable.isNotPreDeleted()) {
              schemaPartition.put(storageGroup, new SchemaPartitionTable());

              databasePartitionTable.getSchemaPartition(
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
              (database, partitionSlots) -> {
                if (isDatabaseExisted(database)) {
                  schemaPartition.put(database, new SchemaPartitionTable());

                  if (!databasePartitionTables
                      .get(database)
                      .getSchemaPartition(partitionSlots, schemaPartition.get(database))) {
                    isAllPartitionsExist.set(false);
                  }

                  if (schemaPartition.get(database).getSchemaPartitionMap().isEmpty()) {
                    // Remove empty Map
                    schemaPartition.remove(database);
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
            (database, partitionSlots) -> {
              if (isDatabaseExisted(database)) {
                dataPartition.put(database, new DataPartitionTable());

                if (!databasePartitionTables
                    .get(database)
                    .getDataPartition(partitionSlots, dataPartition.get(database))) {
                  isAllPartitionsExist.set(false);
                }

                if (dataPartition.get(database).getDataPartitionMap().isEmpty()) {
                  // Remove empty Map
                  dataPartition.remove(database);
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
   * Checks whether the specified DataPartition has a predecessor or successor and returns if it
   * does
   *
   * @param database DatabaseName
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @param timePartitionInterval Time partition interval
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getAdjacentDataPartition(
      String database,
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      long timePartitionInterval) {
    if (databasePartitionTables.containsKey(database)) {
      return databasePartitionTables
          .get(database)
          .getAdjacentDataPartition(seriesPartitionSlot, timePartitionSlot, timePartitionInterval);
    } else {
      return null;
    }
  }

  /**
   * Check if the specified Database exists.
   *
   * @param database The specified Database
   * @return True if the DatabaseSchema is exists and the Database is not pre-deleted
   */
  public boolean isDatabaseExisted(String database) {
    final DatabasePartitionTable databasePartitionTable = databasePartitionTables.get(database);
    return databasePartitionTable != null && databasePartitionTable.isNotPreDeleted();
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
            (database, schemaPartitionTable) -> {
              if (isDatabaseExisted(database)) {
                databasePartitionTables.get(database).createSchemaPartition(schemaPartitionTable);
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
            (database, dataPartitionTable) -> {
              if (isDatabaseExisted(database)) {
                databasePartitionTables.get(database).createDataPartition(dataPartitionTable);
              }
            });

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /** Get SchemaNodeManagementPartition through matched Database. */
  public DataSet getSchemaNodeManagementPartition(List<String> matchedDatabases) {
    SchemaNodeManagementResp schemaNodeManagementResp = new SchemaNodeManagementResp();
    Map<String, SchemaPartitionTable> schemaPartitionMap = new ConcurrentHashMap<>();

    matchedDatabases.stream()
        .filter(this::isDatabaseExisted)
        .forEach(
            storageGroup -> {
              schemaPartitionMap.put(storageGroup, new SchemaPartitionTable());

              databasePartitionTables
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

  /** Get Region information */
  public DataSet getRegionInfoList(GetRegionInfoListPlan regionsInfoPlan) {
    RegionInfoListResp regionResp = new RegionInfoListResp();
    List<TRegionInfo> regionInfoList = new Vector<>();
    if (databasePartitionTables.isEmpty()) {
      regionResp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      regionResp.setRegionInfoList(new ArrayList<>());
      return regionResp;
    }
    TShowRegionReq showRegionReq = regionsInfoPlan.getShowRegionReq();
    final List<String> storageGroups = showRegionReq != null ? showRegionReq.getDatabases() : null;
    databasePartitionTables.forEach(
        (storageGroup, databasePartitionTable) -> {
          if (storageGroups != null && !storageGroups.contains(storageGroup)) {
            return;
          }
          regionInfoList.addAll(databasePartitionTable.getRegionInfoList(regionsInfoPlan));
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
   * Check if the specified RegionGroup exists.
   *
   * @param regionGroupId The specified RegionGroup
   */
  public boolean isRegionGroupExisted(TConsensusGroupId regionGroupId) {
    return databasePartitionTables.values().stream()
        .anyMatch(
            databasePartitionTable -> databasePartitionTable.containRegionGroup(regionGroupId));
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
    databasePartitionTables.values().stream()
        .filter(databasePartitionTable -> databasePartitionTable.containRegionGroup(regionId))
        .forEach(
            databasePartitionTable ->
                databasePartitionTable.updateRegionLocation(regionId, oldNode, newNode));

    return status;
  }

  /**
   * get database for region
   *
   * @param regionId regionId
   * @return database name
   */
  public String getRegionStorageGroup(TConsensusGroupId regionId) {
    Optional<DatabasePartitionTable> sgPartitionTableOptional =
        databasePartitionTables.values().stream()
            .filter(s -> s.containRegionGroup(regionId))
            .findFirst();
    return sgPartitionTableOptional.map(DatabasePartitionTable::getDatabaseName).orElse(null);
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
        (database, partitionSlots) -> {
          if (isDatabaseExisted(database)) {
            result.put(
                database,
                databasePartitionTables
                    .get(database)
                    .filterUnassignedSchemaPartitionSlots(partitionSlots));
          }
        });

    return result;
  }

  /**
   * Only Leader use this interface. Filter unassigned SchemaPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<TSeriesPartitionSlot, TTimeSlotList>>
   * @return Map<StorageGroupName, Map<TSeriesPartitionSlot, TTimeSlotList>>, DataPartitionSlots
   *     that is not assigned in partitionSlotsMap
   */
  public Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> filterUnassignedDataPartitionSlots(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> result = new ConcurrentHashMap<>();

    partitionSlotsMap.forEach(
        (database, partitionSlots) -> {
          if (isDatabaseExisted(database)) {
            result.put(
                database,
                databasePartitionTables
                    .get(database)
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
    databasePartitionTables
        .values()
        .forEach(
            databasePartitionTable -> result.addAll(databasePartitionTable.getAllReplicaSets()));
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
    databasePartitionTables
        .values()
        .forEach(
            databasePartitionTable ->
                result.addAll(databasePartitionTable.getAllReplicaSets(type)));
    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @param database The specified Database
   * @return All Regions' RegionReplicaSet of the specified Database
   */
  public List<TRegionReplicaSet> getAllReplicaSets(String database) {
    if (databasePartitionTables.containsKey(database)) {
      return databasePartitionTables.get(database).getAllReplicaSets();
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * Get all RegionGroups currently owned by the specified Database.
   *
   * @param dataNodeId The specified dataNodeId
   * @return Deep copy of all RegionGroups' RegionReplicaSet with the specified dataNodeId
   */
  public List<TRegionReplicaSet> getAllReplicaSets(int dataNodeId) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    databasePartitionTables
        .values()
        .forEach(
            databasePartitionTable ->
                result.addAll(databasePartitionTable.getAllReplicaSets(dataNodeId)));
    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @param database The specified Database
   * @param regionGroupIds The specified RegionGroupIds
   * @return All Regions' RegionReplicaSet of the specified Database
   */
  public List<TRegionReplicaSet> getReplicaSets(
      String database, List<TConsensusGroupId> regionGroupIds) {
    if (databasePartitionTables.containsKey(database)) {
      return databasePartitionTables.get(database).getReplicaSets(regionGroupIds);
    } else {
      return new ArrayList<>();
    }
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
    databasePartitionTables
        .values()
        .forEach(
            databasePartitionTable ->
                result.getAndAdd(databasePartitionTable.getRegionCount(dataNodeId, type)));
    return result.get();
  }

  /**
   * Only leader use this interface.
   *
   * <p>Get the number of RegionGroups currently owned by the specified Database
   *
   * @param database DatabaseName
   * @param type SchemaRegion or DataRegion
   * @return Number of Regions currently owned by the specific StorageGroup
   * @throws DatabaseNotExistsException When the specific StorageGroup doesn't exist
   */
  public int getRegionGroupCount(String database, TConsensusGroupType type)
      throws DatabaseNotExistsException {
    if (!isDatabaseExisted(database)) {
      throw new DatabaseNotExistsException(database);
    }

    return databasePartitionTables.get(database).getRegionGroupCount(type);
  }

  /**
   * Only leader use this interface.
   *
   * <p>Get the assigned SeriesPartitionSlots count in the specified Database
   *
   * @param database The specified Database
   * @return The assigned SeriesPartitionSlots count
   */
  public int getAssignedSeriesPartitionSlotsCount(String database) {
    return databasePartitionTables.get(database).getAssignedSeriesPartitionSlotsCount();
  }

  /**
   * Get the DataNodes who contain the specific StorageGroup's Schema or Data
   *
   * @param database The specific StorageGroup's name
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getDatabaseRelatedDataNodes(
      String database, TConsensusGroupType type) {
    return databasePartitionTables.get(database).getDatabaseRelatedDataNodes(type);
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
    return databasePartitionTables.get(storageGroup).getRegionGroupSlotsCounter(type);
  }

  /**
   * Only leader use this interface.
   *
   * @return Integer set of all schemaengine region id
   */
  public Set<Integer> getAllSchemaPartition() {
    Set<Integer> schemaPartitionSet = new HashSet<>();
    databasePartitionTables
        .values()
        .forEach(i -> schemaPartitionSet.addAll(i.getSchemaRegionIds()));
    return schemaPartitionSet;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {

    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    // prevents temporary files from being damaged and cannot be deleted, which affects the next
    // snapshot operation.
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    try (BufferedOutputStream fileOutputStream =
            new BufferedOutputStream(
                Files.newOutputStream(tmpFile.toPath()), PARTITION_TABLE_BUFFER_SIZE);
        TIOStreamTransport tioStreamTransport = new TIOStreamTransport(fileOutputStream)) {
      TProtocol protocol = new TBinaryProtocol(tioStreamTransport);

      // serialize nextRegionGroupId
      ReadWriteIOUtils.write(nextRegionGroupId.get(), fileOutputStream);

      // serialize StorageGroupPartitionTable
      ReadWriteIOUtils.write(databasePartitionTables.size(), fileOutputStream);
      for (Map.Entry<String, DatabasePartitionTable> storageGroupPartitionTableEntry :
          databasePartitionTables.entrySet()) {
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

    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (BufferedInputStream fileInputStream =
            new BufferedInputStream(
                Files.newInputStream(snapshotFile.toPath()), PARTITION_TABLE_BUFFER_SIZE);
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
        DatabasePartitionTable databasePartitionTable = new DatabasePartitionTable(storageGroup);
        databasePartitionTable.deserialize(fileInputStream, protocol);
        databasePartitionTables.put(storageGroup, databasePartitionTable);
      }

      // restore deletedRegionSet
      length = ReadWriteIOUtils.readInt(fileInputStream);
      for (int i = 0; i < length; i++) {
        RegionMaintainTask task = RegionMaintainTask.Factory.create(fileInputStream, protocol);
        regionMaintainTaskList.add(task);
      }
    }
  }

  /**
   * Get the RegionId of the specific Database or seriesSlotId(device).
   *
   * @param plan GetRegionIdPlan with the specific Database ,seriesSlotId(device) , timeSlotId.
   * @return GetRegionIdResp with STATUS and List<TConsensusGroupId>.
   */
  public DataSet getRegionId(GetRegionIdPlan plan) {
    if (!isDatabaseExisted(plan.getDatabase())) {
      // Return empty result if Database doesn't exist
      return new GetRegionIdResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
    }

    DatabasePartitionTable databasePartitionTable = databasePartitionTables.get(plan.getDatabase());
    return new GetRegionIdResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        databasePartitionTable
            .getRegionId(plan.getPartitionType(), plan.getSeriesSlotId(), plan.getTimeSlotId())
            .stream()
            .distinct()
            .sorted(Comparator.comparing(TConsensusGroupId::getId))
            .collect(Collectors.toList()));
  }

  /**
   * Get the timePartition of the specific Database or seriesSlotId(device) or regionId.
   *
   * @param plan GetRegionIdPlan with the specific Database ,seriesSlotId(device) , regionId.
   * @return GetRegionIdResp with STATUS and List<TTimePartitionSlot>.
   */
  public DataSet getTimeSlotList(GetTimeSlotListPlan plan) {
    if (!plan.getDatabase().equals("")) {
      if (!isDatabaseExisted(plan.getDatabase())) {
        return new GetTimeSlotListResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
      } else {
        DatabasePartitionTable sgPartitionTable = databasePartitionTables.get(plan.getDatabase());
        return new GetTimeSlotListResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            sgPartitionTable
                .getTimeSlotList(
                    plan.getSeriesSlotId(),
                    plan.getRegionId(),
                    plan.getStartTime(),
                    plan.getEndTime())
                .stream()
                .distinct()
                .sorted(Comparator.comparing(TTimePartitionSlot::getStartTime))
                .collect(Collectors.toList()));
      }
    } else {
      List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
      databasePartitionTables.forEach(
          (database, databasePartitionTable) ->
              timePartitionSlots.addAll(
                  databasePartitionTable.getTimeSlotList(
                      plan.getSeriesSlotId(),
                      plan.getRegionId(),
                      plan.getStartTime(),
                      plan.getEndTime())));
      return new GetTimeSlotListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          timePartitionSlots.stream()
              .distinct()
              .sorted(Comparator.comparing(TTimePartitionSlot::getStartTime))
              .collect(Collectors.toList()));
    }
  }

  public DataSet countTimeSlotList(CountTimeSlotListPlan plan) {
    if (!plan.getDatabase().equals("")) {
      if (!isDatabaseExisted(plan.getDatabase())) {
        return new CountTimeSlotListResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), 0);
      } else {
        DatabasePartitionTable sgPartitionTable = databasePartitionTables.get(plan.getDatabase());
        return new CountTimeSlotListResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            sgPartitionTable
                .getTimeSlotList(
                    plan.getSeriesSlotId(),
                    plan.getRegionId(),
                    plan.getStartTime(),
                    plan.getEndTime())
                .stream()
                .distinct()
                .count());
      }
    } else {
      List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
      databasePartitionTables.forEach(
          (database, databasePartitionTable) ->
              timePartitionSlots.addAll(
                  databasePartitionTable.getTimeSlotList(
                      plan.getSeriesSlotId(),
                      plan.getRegionId(),
                      plan.getStartTime(),
                      plan.getEndTime())));
      return new CountTimeSlotListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          timePartitionSlots.stream().distinct().count());
    }
  }

  public DataSet getSeriesSlotList(GetSeriesSlotListPlan plan) {
    if (!isDatabaseExisted(plan.getDatabase())) {
      return new GetSeriesSlotListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), new ArrayList<>());
    }
    DatabasePartitionTable sgPartitionTable = databasePartitionTables.get(plan.getDatabase());
    return new GetSeriesSlotListResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        sgPartitionTable.getSeriesSlotList(plan.getPartitionType()));
  }

  public void getSchemaRegionIds(
      List<String> databases, Map<String, List<Integer>> schemaRegionIds) {
    for (String database : databases) {
      if (databasePartitionTables.containsKey(database)) {
        schemaRegionIds.put(database, databasePartitionTables.get(database).getSchemaRegionIds());
      }
    }
  }

  public void getDataRegionIds(List<String> databases, Map<String, List<Integer>> dataRegionIds) {
    for (String database : databases) {
      if (databasePartitionTables.containsKey(database)) {
        dataRegionIds.put(database, databasePartitionTables.get(database).getDataRegionIds());
      }
    }
  }

  public void clear() {
    nextRegionGroupId.set(-1);
    databasePartitionTables.clear();
    regionMaintainTaskList.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) o;
    return nextRegionGroupId.get() == that.nextRegionGroupId.get()
        && databasePartitionTables.equals(that.databasePartitionTables)
        && regionMaintainTaskList.equals(that.regionMaintainTaskList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nextRegionGroupId, databasePartitionTables, regionMaintainTaskList);
  }
}
