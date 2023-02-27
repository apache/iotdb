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
package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.response.partition.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetRegionIdResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetSeriesSlotListResp;
import org.apache.iotdb.confignode.consensus.response.partition.GetTimeSlotListResp;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupCache;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** The PartitionManager Manages cluster PartitionTable read and write requests. */
public class PartitionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final RegionGroupExtensionPolicy SCHEMA_REGION_GROUP_EXTENSION_POLICY =
      CONF.getSchemaRegionGroupExtensionPolicy();
  private static final RegionGroupExtensionPolicy DATA_REGION_GROUP_EXTENSION_POLICY =
      CONF.getDataRegionGroupExtensionPolicy();

  private final IManager configManager;
  private final PartitionInfo partitionInfo;

  private SeriesPartitionExecutor executor;

  /** Region cleaner */
  // Monitor for leadership change
  private final Object scheduleMonitor = new Object();
  // Try to delete Regions in every 10s
  private static final int REGION_MAINTAINER_WORK_INTERVAL = 10;
  private final ScheduledExecutorService regionMaintainer;
  private Future<?> currentRegionMaintainerFuture;

  // Map<RegionId, RegionGroupCache>
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;

  public PartitionManager(IManager configManager, PartitionInfo partitionInfo) {
    this.configManager = configManager;
    this.partitionInfo = partitionInfo;
    this.regionMaintainer =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("IoTDB-Region-Maintainer");
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
    setSeriesPartitionExecutor();
  }

  /** Construct SeriesPartitionExecutor by iotdb-confignode.properties */
  private void setSeriesPartitionExecutor() {
    this.executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            CONF.getSeriesPartitionExecutorClass(), CONF.getSeriesSlotNum());
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Thread-safely get SchemaPartition
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionPlan req) {
    return getConsensusManager().read(req).getDataset();
  }

  /**
   * Thread-safely get DataPartition
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     TTimeSlotList>>
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionPlan req) {
    return getConsensusManager().read(req).getDataset();
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   *     STORAGE_GROUP_NOT_EXIST if some StorageGroup don't exist.
   */
  public SchemaPartitionResp getOrCreateSchemaPartition(GetOrCreateSchemaPartitionPlan req) {
    // After all the SchemaPartitions are allocated,
    // all the read requests about SchemaPartitionTable are parallel.
    SchemaPartitionResp resp = (SchemaPartitionResp) getSchemaPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of SchemaPartitions to
    // ensure that each SchemaPartition is created by a unique CreateSchemaPartitionReq.
    // Because the number of SchemaPartitions per database is limited
    // by the number of SeriesPartitionSlots,
    // the number of serialized CreateSchemaPartitionReqs is acceptable.
    synchronized (this) {
      // Filter unassigned SchemaPartitionSlots
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap =
          partitionInfo.filterUnassignedSchemaPartitionSlots(req.getPartitionSlotsMap());

      // Here we ensure that each StorageGroup has at least one SchemaRegion.
      // And if some StorageGroups own too many slots, extend SchemaRegion for them.

      // Map<StorageGroup, unassigned SeriesPartitionSlot count>
      Map<String, Integer> unassignedSchemaPartitionSlotsCountMap = new ConcurrentHashMap<>();
      unassignedSchemaPartitionSlotsMap.forEach(
          (storageGroup, unassignedSchemaPartitionSlots) ->
              unassignedSchemaPartitionSlotsCountMap.put(
                  storageGroup, unassignedSchemaPartitionSlots.size()));
      TSStatus status =
          extendRegionGroupIfNecessary(
              unassignedSchemaPartitionSlotsCountMap, TConsensusGroupType.SchemaRegion);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Return an error code if Region extension failed
        resp.setStatus(status);
        return resp;
      }

      status = getConsensusManager().confirmLeader();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Here we check the leadership second time
        // since the RegionGroup creating process might take some time
        resp.setStatus(status);
        return resp;
      } else {
        // Allocate SchemaPartitions only if
        // the current ConfigNode still holds its leadership
        Map<String, SchemaPartitionTable> assignedSchemaPartition;
        try {
          assignedSchemaPartition =
              getLoadManager().allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
        } catch (NoAvailableRegionGroupException e) {
          LOGGER.error("Create SchemaPartition failed because: ", e);
          resp.setStatus(
              new TSStatus(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())
                  .setMessage(e.getMessage()));
          return resp;
        }

        // Cache allocating result
        CreateSchemaPartitionPlan createPlan = new CreateSchemaPartitionPlan();
        createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
        getConsensusManager().write(createPlan);
      }
    }

    return (SchemaPartitionResp) getSchemaPartition(req);
  }

  /**
   * Get DataPartition and create a new one if it does not exist
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   *     STORAGE_GROUP_NOT_EXIST if some StorageGroup don't exist.
   */
  public DataPartitionResp getOrCreateDataPartition(GetOrCreateDataPartitionPlan req) {
    // After all the DataPartitions are allocated,
    // all the read requests about DataPartitionTable are parallel.
    DataPartitionResp resp = (DataPartitionResp) getDataPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of DataPartitions to
    // ensure that each DataPartition is created by a unique CreateDataPartitionReq.
    // Because the number of DataPartitions per database is limited
    // by the number of SeriesPartitionSlots,
    // the number of serialized CreateDataPartitionReqs is acceptable.
    synchronized (this) {
      // Filter unassigned DataPartitionSlots
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap =
          partitionInfo.filterUnassignedDataPartitionSlots(req.getPartitionSlotsMap());

      // Here we ensure that each StorageGroup has at least one DataRegion.
      // And if some StorageGroups own too many slots, extend DataRegion for them.

      // Map<StorageGroup, unassigned SeriesPartitionSlot count>
      Map<String, Integer> unassignedDataPartitionSlotsCountMap = new ConcurrentHashMap<>();
      unassignedDataPartitionSlotsMap.forEach(
          (storageGroup, unassignedDataPartitionSlots) ->
              unassignedDataPartitionSlotsCountMap.put(
                  storageGroup, unassignedDataPartitionSlots.size()));
      TSStatus status =
          extendRegionGroupIfNecessary(
              unassignedDataPartitionSlotsCountMap, TConsensusGroupType.DataRegion);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Return an error code if Region extension failed
        resp.setStatus(status);
        return resp;
      }

      status = getConsensusManager().confirmLeader();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Here we check the leadership second time
        // since the RegionGroup creating process might take some time
        resp.setStatus(status);
        return resp;
      } else {
        // Allocate DataPartitions only if
        // the current ConfigNode still holds its leadership
        Map<String, DataPartitionTable> assignedDataPartition;
        try {
          assignedDataPartition =
              getLoadManager().allocateDataPartition(unassignedDataPartitionSlotsMap);
        } catch (NoAvailableRegionGroupException e) {
          LOGGER.error("Create DataPartition failed because: ", e);
          resp.setStatus(
              new TSStatus(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())
                  .setMessage(e.getMessage()));
          return resp;
        }

        // Cache allocating result
        CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
        createPlan.setAssignedDataPartition(assignedDataPartition);
        getConsensusManager().write(createPlan);
      }
    }

    resp = (DataPartitionResp) getDataPartition(req);
    if (!resp.isAllPartitionsExist()) {
      LOGGER.error(
          "Lacked some data partition allocation result in the response of getOrCreateDataPartition method");
      resp.setStatus(
          new TSStatus(TSStatusCode.LACK_DATA_PARTITION_ALLOCATION.getStatusCode())
              .setMessage("Lacked some data partition allocation result in the response"));
      return resp;
    }
    return resp;
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Allocate more RegionGroup to the specified StorageGroups if necessary.
   *
   * @param unassignedPartitionSlotsCountMap Map<StorageGroup, unassigned Partition count>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return SUCCESS_STATUS when RegionGroup extension successful; NOT_ENOUGH_DATA_NODE when there
   *     are not enough DataNodes; STORAGE_GROUP_NOT_EXIST when some StorageGroups don't exist
   */
  private TSStatus extendRegionGroupIfNecessary(
      Map<String, Integer> unassignedPartitionSlotsCountMap,
      TConsensusGroupType consensusGroupType) {

    TSStatus result = new TSStatus();

    try {
      if (TConsensusGroupType.SchemaRegion.equals(consensusGroupType)) {
        switch (SCHEMA_REGION_GROUP_EXTENSION_POLICY) {
          case CUSTOM:
            return customExtendRegionGroupIfNecessary(
                unassignedPartitionSlotsCountMap, consensusGroupType);
          case AUTO:
          default:
            return autoExtendRegionGroupIfNecessary(
                unassignedPartitionSlotsCountMap, consensusGroupType);
        }
      } else {
        switch (DATA_REGION_GROUP_EXTENSION_POLICY) {
          case CUSTOM:
            return customExtendRegionGroupIfNecessary(
                unassignedPartitionSlotsCountMap, consensusGroupType);
          case AUTO:
          default:
            return autoExtendRegionGroupIfNecessary(
                unassignedPartitionSlotsCountMap, consensusGroupType);
        }
      }
    } catch (NotEnoughDataNodeException e) {
      String prompt = "ConfigNode failed to extend Region because there are not enough DataNodes";
      LOGGER.error(prompt);
      result.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
      result.setMessage(prompt);
    } catch (DatabaseNotExistsException e) {
      String prompt = "ConfigNode failed to extend Region because some StorageGroup doesn't exist.";
      LOGGER.error(prompt);
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      result.setMessage(prompt);
    }

    return result;
  }

  private TSStatus customExtendRegionGroupIfNecessary(
      Map<String, Integer> unassignedPartitionSlotsCountMap, TConsensusGroupType consensusGroupType)
      throws DatabaseNotExistsException, NotEnoughDataNodeException {

    // Map<StorageGroup, Region allotment>
    Map<String, Integer> allotmentMap = new ConcurrentHashMap<>();

    for (Map.Entry<String, Integer> entry : unassignedPartitionSlotsCountMap.entrySet()) {
      final String database = entry.getKey();
      int minRegionGroupNum =
          getClusterSchemaManager().getMinRegionGroupNum(database, consensusGroupType);
      int allocatedRegionGroupCount =
          partitionInfo.getRegionGroupCount(database, consensusGroupType);

      // Extend RegionGroups until allocatedRegionGroupCount == minRegionGroupNum
      if (allocatedRegionGroupCount < minRegionGroupNum) {
        allotmentMap.put(database, minRegionGroupNum - allocatedRegionGroupCount);
      }
    }

    return generateAndAllocateRegionGroups(allotmentMap, consensusGroupType);
  }

  private TSStatus autoExtendRegionGroupIfNecessary(
      Map<String, Integer> unassignedPartitionSlotsCountMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, DatabaseNotExistsException {

    // Map<StorageGroup, Region allotment>
    Map<String, Integer> allotmentMap = new ConcurrentHashMap<>();

    for (Map.Entry<String, Integer> entry : unassignedPartitionSlotsCountMap.entrySet()) {
      final String database = entry.getKey();
      final int unassignedPartitionSlotsCount = entry.getValue();

      float allocatedRegionGroupCount =
          partitionInfo.getRegionGroupCount(database, consensusGroupType);
      // The slotCount equals to the sum of assigned slot count and unassigned slot count
      float slotCount =
          (float) partitionInfo.getAssignedSeriesPartitionSlotsCount(database)
              + unassignedPartitionSlotsCount;
      float maxRegionGroupCount =
          getClusterSchemaManager().getMaxRegionGroupNum(database, consensusGroupType);
      float maxSlotCount = CONF.getSeriesSlotNum();

      /* RegionGroup extension is required in the following cases */
      // 1. The number of current RegionGroup of the StorageGroup is less than the minimum number
      int minRegionGroupNum =
          getClusterSchemaManager().getMinRegionGroupNum(database, consensusGroupType);
      if (allocatedRegionGroupCount < minRegionGroupNum) {
        // Let the sum of unassignedPartitionSlotsCount and allocatedRegionGroupCount
        // no less than the minRegionGroupNum
        int delta =
            (int)
                Math.min(
                    unassignedPartitionSlotsCount, minRegionGroupNum - allocatedRegionGroupCount);
        allotmentMap.put(database, delta);
        continue;
      }

      // 2. The average number of partitions held by each Region will be greater than the
      // expected average number after the partition allocation is completed
      if (allocatedRegionGroupCount < maxRegionGroupCount
          && slotCount / allocatedRegionGroupCount > maxSlotCount / maxRegionGroupCount) {
        // The delta is equal to the smallest integer solution that satisfies the inequality:
        // slotCount / (allocatedRegionGroupCount + delta) < maxSlotCount / maxRegionGroupCount
        int delta =
            Math.min(
                (int) (maxRegionGroupCount - allocatedRegionGroupCount),
                Math.max(
                    1,
                    (int)
                        Math.ceil(
                            slotCount * maxRegionGroupCount / maxSlotCount
                                - allocatedRegionGroupCount)));
        allotmentMap.put(database, delta);
        continue;
      }

      // 3. All RegionGroups in the specified StorageGroup are disabled currently
      if (allocatedRegionGroupCount
              == filterRegionGroupThroughStatus(database, RegionGroupStatus.Disabled).size()
          && allocatedRegionGroupCount < maxRegionGroupCount) {
        allotmentMap.put(database, 1);
      }
    }

    return generateAndAllocateRegionGroups(allotmentMap, consensusGroupType);
  }

  private TSStatus generateAndAllocateRegionGroups(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, DatabaseNotExistsException {
    if (!allotmentMap.isEmpty()) {
      CreateRegionGroupsPlan createRegionGroupsPlan =
          getLoadManager().allocateRegionGroups(allotmentMap, consensusGroupType);
      LOGGER.info("[CreateRegionGroups] Starting to create the following RegionGroups:");
      createRegionGroupsPlan.planLog(LOGGER);
      return getProcedureManager().createRegionGroups(consensusGroupType, createRegionGroupsPlan);
    } else {
      return RpcUtils.SUCCESS_STATUS;
    }
  }

  /**
   * Only leader use this interface. Checks whether the specified DataPartition has a predecessor
   * and returns if it does
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
    return partitionInfo.getPrecededDataPartition(
        storageGroup, seriesPartitionSlot, timePartitionSlot, timePartitionInterval);
  }

  /**
   * Get the DataNodes who contain the specified Database's Schema or Data
   *
   * @param database The specific Database's name
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getDatabaseRelatedDataNodes(
      String database, TConsensusGroupType type) {
    return partitionInfo.getDatabaseRelatedDataNodes(database, type);
  }

  /**
   * Only leader use this interface
   *
   * @param type The specified TConsensusGroupType
   * @return Deep copy of all Regions' RegionReplicaSet and organized to Map
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getAllReplicaSetsMap(TConsensusGroupType type) {
    return partitionInfo.getAllReplicaSets(type).stream()
        .collect(
            Collectors.toMap(TRegionReplicaSet::getRegionId, regionReplicaSet -> regionReplicaSet));
  }

  /**
   * Only leader use this interface
   *
   * @return Deep copy of all Regions' RegionReplicaSet
   */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    return partitionInfo.getAllReplicaSets();
  }

  /**
   * Only leader use this interface.
   *
   * @param type The specified TConsensusGroupType
   * @return Deep copy of all Regions' RegionReplicaSet with the specified TConsensusGroupType
   */
  public List<TRegionReplicaSet> getAllReplicaSets(TConsensusGroupType type) {
    return partitionInfo.getAllReplicaSets(type);
  }

  /**
   * Only leader use this interface.
   *
   * @param database The specified Database
   * @return All Regions' RegionReplicaSet of the specified StorageGroup
   */
  public List<TRegionReplicaSet> getAllReplicaSets(String database) {
    return partitionInfo.getAllReplicaSets(database);
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
    return partitionInfo.getRegionCount(dataNodeId, type);
  }

  /**
   * Only leader use this interface.
   *
   * <p>Get the number of RegionGroups currently owned by the specified Database
   *
   * @param database DatabaseName
   * @param type SchemaRegion or DataRegion
   * @return Number of Regions currently owned by the specified Database
   * @throws DatabaseNotExistsException When the specified Database doesn't exist
   */
  public int getRegionGroupCount(String database, TConsensusGroupType type)
      throws DatabaseNotExistsException {
    return partitionInfo.getRegionGroupCount(database, type);
  }

  public boolean isDatabaseExisted(String database) {
    return partitionInfo.isDatabaseExisted(database);
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
    return partitionInfo.getAssignedSeriesPartitionSlotsCount(database);
  }

  /**
   * Only leader use this interface.
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return The specific StorageGroup's Regions that sorted by the number of allocated slots
   * @throws NoAvailableRegionGroupException When all RegionGroups within the specified StorageGroup
   *     are unavailable currently
   */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionGroupSlotsCounter(
      String storageGroup, TConsensusGroupType type) throws NoAvailableRegionGroupException {
    // Collect static data
    List<Pair<Long, TConsensusGroupId>> regionGroupSlotsCounter =
        partitionInfo.getRegionGroupSlotsCounter(storageGroup, type);

    // Filter RegionGroups that have Disabled status
    List<Pair<Long, TConsensusGroupId>> result = new ArrayList<>();
    for (Pair<Long, TConsensusGroupId> slotsCounter : regionGroupSlotsCounter) {
      RegionGroupStatus status = getRegionGroupStatus(slotsCounter.getRight());
      if (!RegionGroupStatus.Disabled.equals(status)) {
        result.add(slotsCounter);
      }
    }

    if (result.isEmpty()) {
      throw new NoAvailableRegionGroupException(type);
    }

    result.sort(
        (o1, o2) -> {
          // Use the number of partitions as the first priority
          if (o1.getLeft() < o2.getLeft()) {
            return -1;
          } else if (o1.getLeft() > o2.getLeft()) {
            return 1;
          } else {
            // Use RegionGroup status as second priority, Running > Available > Discouraged
            return getRegionGroupStatus(o1.getRight())
                .compareTo(getRegionGroupStatus(o2.getRight()));
          }
        });
    return result;
  }

  /**
   * Only leader use this interface
   *
   * @return the next RegionGroupId
   */
  public int generateNextRegionGroupId() {
    return partitionInfo.generateNextRegionGroupId();
  }

  /**
   * GetNodePathsPartition
   *
   * @param physicalPlan GetNodesPathsPartitionReq
   * @return SchemaNodeManagementPartitionDataSet that contains only existing matched
   *     SchemaPartition and matched child paths aboveMTree
   */
  public SchemaNodeManagementResp getNodePathsPartition(GetNodePathsPartitionPlan physicalPlan) {
    SchemaNodeManagementResp schemaNodeManagementResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaNodeManagementResp = (SchemaNodeManagementResp) consensusReadResponse.getDataset();
    return schemaNodeManagementResp;
  }

  public void preDeleteStorageGroup(
      String storageGroup, PreDeleteDatabasePlan.PreDeleteType preDeleteType) {
    final PreDeleteDatabasePlan preDeleteDatabasePlan =
        new PreDeleteDatabasePlan(storageGroup, preDeleteType);
    getConsensusManager().write(preDeleteDatabasePlan);
  }

  /**
   * Get TSeriesPartitionSlot
   *
   * @param devicePath Full path ending with device name
   * @return SeriesPartitionSlot
   */
  public TSeriesPartitionSlot getSeriesPartitionSlot(String devicePath) {
    return executor.getSeriesPartitionSlot(devicePath);
  }

  public RegionInfoListResp getRegionInfoList(GetRegionInfoListPlan req) {
    // Get static result
    RegionInfoListResp regionInfoListResp =
        (RegionInfoListResp) getConsensusManager().read(req).getDataset();

    // Get cached result
    Map<TConsensusGroupId, Integer> allLeadership = getLoadManager().getLatestRegionLeaderMap();
    regionInfoListResp
        .getRegionInfoList()
        .forEach(
            regionInfo -> {
              regionInfo.setStatus(
                  getRegionStatus(regionInfo.getConsensusGroupId(), regionInfo.getDataNodeId())
                      .getStatus());

              String regionType =
                  regionInfo.getDataNodeId()
                          == allLeadership.getOrDefault(regionInfo.getConsensusGroupId(), -1)
                      ? RegionRoleType.Leader.toString()
                      : RegionRoleType.Follower.toString();
              regionInfo.setRoleType(regionType);
            });

    return regionInfoListResp;
  }

  /**
   * update region location
   *
   * @param req UpdateRegionLocationReq
   * @return TSStatus
   */
  public TSStatus updateRegionLocation(UpdateRegionLocationPlan req) {
    // Remove heartbeat cache if exists
    if (regionGroupCacheMap.containsKey(req.getRegionId())) {
      regionGroupCacheMap
          .get(req.getRegionId())
          .removeCacheIfExists(req.getOldNode().getDataNodeId());
    }

    return getConsensusManager().write(req).getStatus();
  }

  public GetRegionIdResp getRegionId(TGetRegionIdReq req) {
    GetRegionIdPlan plan =
        new GetRegionIdPlan(
            req.getDatabase(),
            req.getType(),
            req.isSetSeriesSlotId()
                ? req.getSeriesSlotId()
                : executor.getSeriesPartitionSlot(req.getDeviceId()),
            req.isSetTimeSlotId()
                ? req.getTimeSlotId()
                : (req.isSetTimeStamp()
                    ? new TTimePartitionSlot(
                        req.getTimeStamp() - req.getTimeStamp() % CONF.getTimePartitionInterval())
                    : null));
    return (GetRegionIdResp) getConsensusManager().read(plan).getDataset();
  }

  public GetTimeSlotListResp getTimeSlotList(GetTimeSlotListPlan plan) {
    return (GetTimeSlotListResp) getConsensusManager().read(plan).getDataset();
  }

  public GetSeriesSlotListResp getSeriesSlotList(GetSeriesSlotListPlan plan) {
    return (GetSeriesSlotListResp) getConsensusManager().read(plan).getDataset();
  }

  /**
   * get database for region
   *
   * @param regionId regionId
   * @return database name
   */
  public String getRegionStorageGroup(TConsensusGroupId regionId) {
    return partitionInfo.getRegionStorageGroup(regionId);
  }

  /**
   * Called by {@link PartitionManager#regionMaintainer}
   *
   * <p>Periodically maintain the RegionReplicas to be created or deleted
   */
  public void maintainRegionReplicas() {
    // the consensusManager of configManager may not be fully initialized at this time
    Optional.ofNullable(getConsensusManager())
        .ifPresent(
            consensusManager -> {
              if (getConsensusManager().isLeader()) {
                List<RegionMaintainTask> regionMaintainTaskList =
                    partitionInfo.getRegionMaintainEntryList();

                if (regionMaintainTaskList.isEmpty()) {
                  return;
                }

                // group tasks by region id
                Map<TConsensusGroupId, Queue<RegionMaintainTask>> regionMaintainTaskMap =
                    new HashMap<>();
                for (RegionMaintainTask regionMaintainTask : regionMaintainTaskList) {
                  regionMaintainTaskMap
                      .computeIfAbsent(regionMaintainTask.getRegionId(), k -> new LinkedList<>())
                      .add(regionMaintainTask);
                }

                while (!regionMaintainTaskMap.isEmpty()) {
                  // select same type task from each region group
                  List<RegionMaintainTask> selectedRegionMaintainTask = new ArrayList<>();
                  RegionMaintainType currentType = null;
                  for (Map.Entry<TConsensusGroupId, Queue<RegionMaintainTask>> entry :
                      regionMaintainTaskMap.entrySet()) {
                    RegionMaintainTask regionMaintainTask = entry.getValue().peek();
                    if (regionMaintainTask == null) {
                      continue;
                    }

                    if (currentType == null) {
                      currentType = regionMaintainTask.getType();
                      selectedRegionMaintainTask.add(entry.getValue().peek());
                    } else {
                      if (!currentType.equals(regionMaintainTask.getType())) {
                        continue;
                      }

                      if (currentType.equals(RegionMaintainType.DELETE)
                          || entry
                              .getKey()
                              .getType()
                              .equals(selectedRegionMaintainTask.get(0).getRegionId().getType())) {
                        // delete or same create task
                        selectedRegionMaintainTask.add(entry.getValue().peek());
                      }
                    }
                  }

                  if (selectedRegionMaintainTask.isEmpty()) {
                    break;
                  }

                  Set<TConsensusGroupId> successfulTask = new HashSet<>();
                  switch (currentType) {
                    case CREATE:
                      // create region
                      switch (selectedRegionMaintainTask.get(0).getRegionId().getType()) {
                        case SchemaRegion:
                          // create SchemaRegion
                          AsyncClientHandler<TCreateSchemaRegionReq, TSStatus>
                              createSchemaRegionHandler =
                                  new AsyncClientHandler<>(
                                      DataNodeRequestType.CREATE_SCHEMA_REGION);
                          for (RegionMaintainTask regionMaintainTask : selectedRegionMaintainTask) {
                            RegionCreateTask schemaRegionCreateTask =
                                (RegionCreateTask) regionMaintainTask;
                            LOGGER.info(
                                "Start to create Region: {} on DataNode: {}",
                                schemaRegionCreateTask.getRegionReplicaSet().getRegionId(),
                                schemaRegionCreateTask.getTargetDataNode());
                            createSchemaRegionHandler.putRequest(
                                schemaRegionCreateTask.getRegionId().getId(),
                                new TCreateSchemaRegionReq(
                                    schemaRegionCreateTask.getRegionReplicaSet(),
                                    schemaRegionCreateTask.getStorageGroup()));
                            createSchemaRegionHandler.putDataNodeLocation(
                                schemaRegionCreateTask.getRegionId().getId(),
                                schemaRegionCreateTask.getTargetDataNode());
                          }

                          AsyncDataNodeClientPool.getInstance()
                              .sendAsyncRequestToDataNodeWithRetry(createSchemaRegionHandler);

                          for (Map.Entry<Integer, TSStatus> entry :
                              createSchemaRegionHandler.getResponseMap().entrySet()) {
                            if (entry.getValue().getCode()
                                == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                              successfulTask.add(
                                  new TConsensusGroupId(
                                      TConsensusGroupType.SchemaRegion, entry.getKey()));
                            }
                          }
                          break;
                        case DataRegion:
                          // create DataRegion
                          AsyncClientHandler<TCreateDataRegionReq, TSStatus>
                              createDataRegionHandler =
                                  new AsyncClientHandler<>(DataNodeRequestType.CREATE_DATA_REGION);
                          for (RegionMaintainTask regionMaintainTask : selectedRegionMaintainTask) {
                            RegionCreateTask dataRegionCreateTask =
                                (RegionCreateTask) regionMaintainTask;
                            LOGGER.info(
                                "Start to create Region: {} on DataNode: {}",
                                dataRegionCreateTask.getRegionReplicaSet().getRegionId(),
                                dataRegionCreateTask.getTargetDataNode());
                            createDataRegionHandler.putRequest(
                                dataRegionCreateTask.getRegionId().getId(),
                                new TCreateDataRegionReq(
                                        dataRegionCreateTask.getRegionReplicaSet(),
                                        dataRegionCreateTask.getStorageGroup())
                                    .setTtl(dataRegionCreateTask.getTTL()));
                            createDataRegionHandler.putDataNodeLocation(
                                dataRegionCreateTask.getRegionId().getId(),
                                dataRegionCreateTask.getTargetDataNode());
                          }

                          AsyncDataNodeClientPool.getInstance()
                              .sendAsyncRequestToDataNodeWithRetry(createDataRegionHandler);

                          for (Map.Entry<Integer, TSStatus> entry :
                              createDataRegionHandler.getResponseMap().entrySet()) {
                            if (entry.getValue().getCode()
                                == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                              successfulTask.add(
                                  new TConsensusGroupId(
                                      TConsensusGroupType.DataRegion, entry.getKey()));
                            }
                          }
                          break;
                      }
                      break;
                    case DELETE:
                      // delete region
                      AsyncClientHandler<TConsensusGroupId, TSStatus> deleteRegionHandler =
                          new AsyncClientHandler<>(DataNodeRequestType.DELETE_REGION);
                      Map<Integer, TConsensusGroupId> regionIdMap = new HashMap<>();
                      for (RegionMaintainTask regionMaintainTask : selectedRegionMaintainTask) {
                        RegionDeleteTask regionDeleteTask = (RegionDeleteTask) regionMaintainTask;
                        LOGGER.info(
                            "Start to delete Region: {} on DataNode: {}",
                            regionDeleteTask.getRegionId(),
                            regionDeleteTask.getTargetDataNode());
                        deleteRegionHandler.putRequest(
                            regionDeleteTask.getRegionId().getId(), regionDeleteTask.getRegionId());
                        deleteRegionHandler.putDataNodeLocation(
                            regionDeleteTask.getRegionId().getId(),
                            regionDeleteTask.getTargetDataNode());
                        regionIdMap.put(
                            regionDeleteTask.getRegionId().getId(), regionDeleteTask.getRegionId());
                      }

                      long startTime = System.currentTimeMillis();
                      AsyncDataNodeClientPool.getInstance()
                          .sendAsyncRequestToDataNodeWithRetry(deleteRegionHandler);

                      LOGGER.info(
                          "Deleting regions costs {}ms", (System.currentTimeMillis() - startTime));

                      for (Map.Entry<Integer, TSStatus> entry :
                          deleteRegionHandler.getResponseMap().entrySet()) {
                        if (entry.getValue().getCode()
                            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                          successfulTask.add(regionIdMap.get(entry.getKey()));
                        }
                      }
                      break;
                  }

                  if (successfulTask.isEmpty()) {
                    break;
                  }

                  for (TConsensusGroupId regionId : successfulTask) {
                    regionMaintainTaskMap.compute(
                        regionId,
                        (k, v) -> {
                          if (v == null) {
                            throw new IllegalStateException();
                          }
                          v.poll();
                          if (v.isEmpty()) {
                            return null;
                          } else {
                            return v;
                          }
                        });
                  }

                  // Poll the head entry if success
                  getConsensusManager()
                      .write(new PollSpecificRegionMaintainTaskPlan(successfulTask));

                  if (successfulTask.size() < selectedRegionMaintainTask.size()) {
                    // Here we just break and wait until next schedule task
                    // due to all the RegionMaintainEntry should be executed by
                    // the order of they were offered
                    break;
                  }
                }
              }
            });
  }

  public void startRegionCleaner() {
    synchronized (scheduleMonitor) {
      if (currentRegionMaintainerFuture == null) {
        /* Start the RegionCleaner service */
        currentRegionMaintainerFuture =
            ScheduledExecutorUtil.safelyScheduleAtFixedRate(
                regionMaintainer,
                this::maintainRegionReplicas,
                0,
                REGION_MAINTAINER_WORK_INTERVAL,
                TimeUnit.SECONDS);
        LOGGER.info("RegionCleaner is started successfully.");
      }
    }
  }

  public void stopRegionCleaner() {
    synchronized (scheduleMonitor) {
      if (currentRegionMaintainerFuture != null) {
        /* Stop the RegionCleaner service */
        currentRegionMaintainerFuture.cancel(false);
        currentRegionMaintainerFuture = null;
        regionGroupCacheMap.clear();
        LOGGER.info("RegionCleaner is stopped successfully.");
      }
    }
  }

  public Map<TConsensusGroupId, RegionGroupCache> getRegionGroupCacheMap() {
    return regionGroupCacheMap;
  }

  public void removeRegionGroupCache(TConsensusGroupId consensusGroupId) {
    regionGroupCacheMap.remove(consensusGroupId);
  }

  /**
   * Filter the RegionGroups in the specified StorageGroup through the RegionGroupStatus
   *
   * @param storageGroup The specified StorageGroup
   * @param status The specified RegionGroupStatus
   * @return Filtered RegionGroups with the specific RegionGroupStatus
   */
  public List<TRegionReplicaSet> filterRegionGroupThroughStatus(
      String storageGroup, RegionGroupStatus... status) {
    return getAllReplicaSets(storageGroup).stream()
        .filter(
            regionReplicaSet -> {
              TConsensusGroupId regionGroupId = regionReplicaSet.getRegionId();
              return regionGroupCacheMap.containsKey(regionGroupId)
                  && Arrays.stream(status)
                      .anyMatch(
                          s ->
                              s.equals(
                                  regionGroupCacheMap
                                      .get(regionGroupId)
                                      .getStatistics()
                                      .getRegionGroupStatus()));
            })
        .collect(Collectors.toList());
  }

  /**
   * Count the number of cluster Regions with specified RegionStatus
   *
   * @param type The specified RegionGroupType
   * @param status The specified statues
   * @return The number of cluster Regions with specified RegionStatus
   */
  public int countRegionWithSpecifiedStatus(TConsensusGroupType type, RegionStatus... status) {
    AtomicInteger result = new AtomicInteger(0);
    regionGroupCacheMap.forEach(
        (regionGroupId, regionGroupCache) -> {
          if (type.equals(regionGroupId.getType())) {
            regionGroupCache
                .getStatistics()
                .getRegionStatisticsMap()
                .values()
                .forEach(
                    regionStatistics -> {
                      if (Arrays.stream(status)
                          .anyMatch(s -> s.equals(regionStatistics.getRegionStatus()))) {
                        result.getAndIncrement();
                      }
                    });
          }
        });
    return result.get();
  }

  /**
   * Safely get RegionStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @param dataNodeId Specified RegionReplicaId
   * @return Corresponding RegionStatus if cache exists, Unknown otherwise
   */
  public RegionStatus getRegionStatus(TConsensusGroupId consensusGroupId, int dataNodeId) {
    return regionGroupCacheMap.containsKey(consensusGroupId)
        ? regionGroupCacheMap.get(consensusGroupId).getStatistics().getRegionStatus(dataNodeId)
        : RegionStatus.Unknown;
  }

  /**
   * Safely get RegionGroupStatus.
   *
   * @param consensusGroupId Specified RegionGroupId
   * @return Corresponding RegionGroupStatus if cache exists, Disabled otherwise
   */
  public RegionGroupStatus getRegionGroupStatus(TConsensusGroupId consensusGroupId) {
    return regionGroupCacheMap.containsKey(consensusGroupId)
        ? regionGroupCacheMap.get(consensusGroupId).getStatistics().getRegionGroupStatus()
        : RegionGroupStatus.Disabled;
  }

  /** Initialize the regionGroupCacheMap when the ConfigNode-Leader is switched. */
  public void initRegionGroupHeartbeatCache() {
    regionGroupCacheMap.clear();
    getAllReplicaSets()
        .forEach(
            regionReplicaSet ->
                regionGroupCacheMap.put(
                    regionReplicaSet.getRegionId(),
                    new RegionGroupCache(regionReplicaSet.getRegionId())));
  }

  public ScheduledExecutorService getRegionMaintainer() {
    return regionMaintainer;
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  private ProcedureManager getProcedureManager() {
    return configManager.getProcedureManager();
  }
}
