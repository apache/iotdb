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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.CountTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
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
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** The {@link PartitionManager} manages cluster PartitionTable read and write requests. */
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

  private static final String CONSENSUS_READ_ERROR =
      "Failed in the read API executing the consensus layer due to: ";

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  /** Region cleaner. */
  // Monitor for leadership change
  private final Object scheduleMonitor = new Object();

  // Try to delete Regions in every 10s
  private static final int REGION_MAINTAINER_WORK_INTERVAL = 10;
  private final ScheduledExecutorService regionMaintainer;
  private Future<?> currentRegionMaintainerFuture;

  public PartitionManager(IManager configManager, PartitionInfo partitionInfo) {
    this.configManager = configManager;
    this.partitionInfo = partitionInfo;
    this.regionMaintainer =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.CONFIG_NODE_REGION_MAINTAINER.getName());
    setSeriesPartitionExecutor();
  }

  /** Construct SeriesPartitionExecutor by iotdb-confignode{@literal .}properties. */
  private void setSeriesPartitionExecutor() {
    this.executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            CONF.getSeriesPartitionExecutorClass(), CONF.getSeriesSlotNum());
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Thread-safely get SchemaPartition.
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public SchemaPartitionResp getSchemaPartition(GetSchemaPartitionPlan req) {
    try {
      return (SchemaPartitionResp) getConsensusManager().read(req);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new SchemaPartitionResp(res, false, Collections.emptyMap());
    }
  }

  /**
   * Thread-safely get DataPartition
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     TTimeSlotList>>
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataPartitionResp getDataPartition(GetDataPartitionPlan req) {
    try {
      return (DataPartitionResp) getConsensusManager().read(req);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new DataPartitionResp(res, false, Collections.emptyMap());
    }
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist.
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   *     STORAGE_GROUP_NOT_EXIST if some StorageGroup don't exist.
   */
  public SchemaPartitionResp getOrCreateSchemaPartition(GetOrCreateSchemaPartitionPlan req) {
    // Check if the related Databases exist
    for (String database : req.getPartitionSlotsMap().keySet()) {
      if (!isDatabaseExist(database)) {
        return new SchemaPartitionResp(
            new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
                .setMessage(
                    String.format(
                        "Create SchemaPartition failed because the database: %s is not exists",
                        database)),
            false,
            null);
      }
    }

    // After all the SchemaPartitions are allocated,
    // all the read requests about SchemaPartitionTable are parallel.
    SchemaPartitionResp resp = getSchemaPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of SchemaPartitions to
    // ensure that each SchemaPartition is created by a unique CreateSchemaPartitionReq.
    // Because the number of SchemaPartitions per database is limited
    // by the number of SeriesPartitionSlots,
    // the number of serialized CreateSchemaPartitionReqs is acceptable.
    synchronized (this) {
      // Here we should check again if the SchemaPartition
      // has been created by other threads to improve concurrent performance
      resp = getSchemaPartition(req);
      if (resp.isAllPartitionsExist()) {
        return resp;
      }

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

      Map<String, SchemaPartitionTable> assignedSchemaPartition;
      try {
        assignedSchemaPartition =
            getLoadManager().allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
      } catch (NoAvailableRegionGroupException e) {
        status = getConsensusManager().confirmLeader();
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // The allocation might fail due to leadership change
          resp.setStatus(status);
          return resp;
        }

        LOGGER.error("Create SchemaPartition failed because: ", e);
        resp.setStatus(
            new TSStatus(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())
                .setMessage(e.getMessage()));
        return resp;
      }

      // Cache allocating result only if the current ConfigNode still holds its leadership
      CreateSchemaPartitionPlan createPlan = new CreateSchemaPartitionPlan();
      createPlan.setAssignedSchemaPartition(assignedSchemaPartition);

      status = consensusWritePartitionResult(createPlan);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // The allocation might fail due to consensus error
        resp.setStatus(status);
        return resp;
      }
    }

    resp = getSchemaPartition(req);
    if (!resp.isAllPartitionsExist()) {
      // Count the fail rate
      AtomicInteger totalSlotNum = new AtomicInteger();
      req.getPartitionSlotsMap()
          .forEach((database, partitionSlots) -> totalSlotNum.addAndGet(partitionSlots.size()));

      AtomicInteger unassignedSlotNum = new AtomicInteger();
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap =
          partitionInfo.filterUnassignedSchemaPartitionSlots(req.getPartitionSlotsMap());
      unassignedSchemaPartitionSlotsMap.forEach(
          (database, unassignedSchemaPartitionSlots) ->
              unassignedSlotNum.addAndGet(unassignedSchemaPartitionSlots.size()));

      String errMsg =
          String.format(
              "Lacked %d/%d SchemaPartition allocation result in the response of getOrCreateSchemaPartition method",
              unassignedSlotNum.get(), totalSlotNum.get());
      LOGGER.error(errMsg);
      resp.setStatus(
          new TSStatus(TSStatusCode.LACK_PARTITION_ALLOCATION.getStatusCode()).setMessage(errMsg));
      return resp;
    }
    return resp;
  }

  /**
   * Get DataPartition and create a new one if it does not exist.
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   *     STORAGE_GROUP_NOT_EXIST if some StorageGroup don't exist.
   */
  public DataPartitionResp getOrCreateDataPartition(GetOrCreateDataPartitionPlan req) {
    // Check if the related Databases exist
    for (String database : req.getPartitionSlotsMap().keySet()) {
      if (!isDatabaseExist(database)) {
        return new DataPartitionResp(
            new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
                .setMessage(
                    String.format(
                        "Create DataPartition failed because the database: %s is not exists",
                        database)),
            false,
            null);
      }
    }

    // After all the DataPartitions are allocated,
    // all the read requests about DataPartitionTable are parallel.
    DataPartitionResp resp = getDataPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of DataPartitions to
    // ensure that each DataPartition is created by a unique CreateDataPartitionReq.
    // Because the number of DataPartitions per database is limited
    // by the number of SeriesPartitionSlots,
    // the number of serialized CreateDataPartitionReqs is acceptable.
    synchronized (this) {
      // Here we should check again if the DataPartition
      // has been created by other threads to improve concurrent performance
      resp = getDataPartition(req);
      if (resp.isAllPartitionsExist()) {
        return resp;
      }

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

      Map<String, DataPartitionTable> assignedDataPartition;
      try {
        assignedDataPartition =
            getLoadManager().allocateDataPartition(unassignedDataPartitionSlotsMap);
      } catch (DatabaseNotExistsException | NoAvailableRegionGroupException e) {
        status = getConsensusManager().confirmLeader();
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // The allocation might fail due to leadership change
          resp.setStatus(status);
          return resp;
        }

        LOGGER.error("Create DataPartition failed because: ", e);
        if (e instanceof DatabaseNotExistsException) {
          resp.setStatus(
              new TSStatus(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode())
                  .setMessage(e.getMessage()));
        } else {
          resp.setStatus(
              new TSStatus(TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())
                  .setMessage(e.getMessage()));
        }
        return resp;
      }

      // Cache allocating result only if the current ConfigNode still holds its leadership
      CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
      createPlan.setAssignedDataPartition(assignedDataPartition);

      status = consensusWritePartitionResult(createPlan);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // The allocation might fail due to consensus error
        resp.setStatus(status);
        return resp;
      }
    }

    resp = getDataPartition(req);
    if (!resp.isAllPartitionsExist()) {
      // Count the fail rate
      AtomicInteger totalSlotNum = new AtomicInteger();
      req.getPartitionSlotsMap()
          .forEach(
              (database, partitionSlots) ->
                  partitionSlots.forEach(
                      (seriesPartitionSlot, timeSlotList) ->
                          totalSlotNum.addAndGet(timeSlotList.getTimePartitionSlots().size())));

      AtomicInteger unassignedSlotNum = new AtomicInteger();
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap =
          partitionInfo.filterUnassignedDataPartitionSlots(req.getPartitionSlotsMap());
      unassignedDataPartitionSlotsMap.forEach(
          (database, unassignedDataPartitionSlots) ->
              unassignedDataPartitionSlots.forEach(
                  (seriesPartitionSlot, timeSlotList) ->
                      unassignedSlotNum.addAndGet(timeSlotList.getTimePartitionSlots().size())));

      String errMsg =
          String.format(
              "Lacked %d/%d DataPartition allocation result in the response of getOrCreateDataPartition method",
              unassignedSlotNum.get(), totalSlotNum.get());
      LOGGER.error(errMsg);
      resp.setStatus(
          new TSStatus(TSStatusCode.LACK_PARTITION_ALLOCATION.getStatusCode()).setMessage(errMsg));
      return resp;
    }
    return resp;
  }

  private TSStatus consensusWritePartitionResult(ConfigPhysicalPlan plan) {
    TSStatus status = getConsensusManager().confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Here we check the leadership second time
      // since the RegionGroup creating process might take some time
      return status;
    }
    try {
      return getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      // The allocation might fail due to consensus error
      LOGGER.error("Write partition allocation result failed because: {}", status);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
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
      LOGGER.error(e.getMessage());
      result.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
      result.setMessage(e.getMessage());
    } catch (DatabaseNotExistsException e) {
      LOGGER.error(e.getMessage());
      result.setCode(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
      result.setMessage(e.getMessage());
    }

    return result;
  }

  private TSStatus customExtendRegionGroupIfNecessary(
      Map<String, Integer> unassignedPartitionSlotsCountMap, TConsensusGroupType consensusGroupType)
      throws DatabaseNotExistsException, NotEnoughDataNodeException {

    // Map<Database, Region allotment>
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

    // Map<Database, Region allotment>
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
      float maxRegionGroupNum =
          getClusterSchemaManager().getMaxRegionGroupNum(database, consensusGroupType);
      float maxSlotCount = CONF.getSeriesSlotNum();

      /* RegionGroup extension is required in the following cases */
      // 1. The number of current RegionGroup of the Database is less than the minimum number
      int minRegionGroupNum =
          getClusterSchemaManager().getMinRegionGroupNum(database, consensusGroupType);
      if (allocatedRegionGroupCount < minRegionGroupNum
          // Ensure the number of RegionGroups is enough
          // for current SeriesPartitionSlots after extension
          // Otherwise, more RegionGroups should be extended through case 2.
          && slotCount <= (maxSlotCount / maxRegionGroupNum) * minRegionGroupNum) {

        // Let the sum of unassignedPartitionSlotsCount and allocatedRegionGroupCount
        // no less than the minRegionGroupNum
        int delta =
            (int)
                Math.min(
                    unassignedPartitionSlotsCount, minRegionGroupNum - allocatedRegionGroupCount);
        allotmentMap.put(database, delta);

      } else if (allocatedRegionGroupCount < maxRegionGroupNum
          && slotCount / allocatedRegionGroupCount > maxSlotCount / maxRegionGroupNum) {
        // 2. The average number of partitions held by each Region will be greater than the
        // expected average number after the partition allocation is completed.

        // The delta is equal to the smallest integer solution that satisfies the inequality:
        // slotCount / (allocatedRegionGroupCount + delta) < maxSlotCount / maxRegionGroupNum
        int delta =
            Math.min(
                (int) (maxRegionGroupNum - allocatedRegionGroupCount),
                Math.max(
                    1,
                    (int)
                        Math.ceil(
                            slotCount * maxRegionGroupNum / maxSlotCount
                                - allocatedRegionGroupCount)));
        allotmentMap.put(database, delta);

      } else if (allocatedRegionGroupCount
              == filterRegionGroupThroughStatus(database, RegionGroupStatus.Disabled).size()
          && allocatedRegionGroupCount < maxRegionGroupNum) {
        // 3. All RegionGroups in the specified Database are disabled currently
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
   * Only leader use this interface. Checks whether the specified DataPartition has a successor and
   * returns if it does.
   *
   * @param database DatabaseName
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specific DataPartition's successor if exists, null otherwise
   */
  public TConsensusGroupId getSuccessorDataPartition(
      String database,
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot) {
    return partitionInfo.getSuccessorDataPartition(
        database, seriesPartitionSlot, timePartitionSlot);
  }

  /**
   * Only leader use this interface. Checks whether the specified DataPartition has a predecessor
   * and returns if it does.
   *
   * @param database DatabaseName
   * @param seriesPartitionSlot Corresponding SeriesPartitionSlot
   * @param timePartitionSlot Corresponding TimePartitionSlot
   * @return The specific DataPartition's predecessor if exists, null otherwise
   */
  public TConsensusGroupId getPredecessorDataPartition(
      String database,
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot) {
    return partitionInfo.getPredecessorDataPartition(
        database, seriesPartitionSlot, timePartitionSlot);
  }

  /**
   * Get the DataNodes who contain the specified Database's Schema or Data.
   *
   * @param database The specific Database's name
   * @param type SchemaRegion or DataRegion
   * @return Set {@literal <}TDataNodeLocation{@literal >}, the related DataNodes
   */
  public Set<TDataNodeLocation> getDatabaseRelatedDataNodes(
      String database, TConsensusGroupType type) {
    return partitionInfo.getDatabaseRelatedDataNodes(database, type);
  }

  /**
   * Only leader use this interface.
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
   * Only leader use this interface.
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
   * @return All Regions' RegionReplicaSet of the specified Database
   */
  public List<TRegionReplicaSet> getAllReplicaSets(String database) {
    return partitionInfo.getAllReplicaSets(database);
  }

  /**
   * Only leader use this interface.
   *
   * @param database The specified Database
   * @param type SchemaRegion or DataRegion
   * @return Deep copy of all Regions' RegionReplicaSet with the specified Database and
   *     TConsensusGroupType
   */
  public List<TRegionReplicaSet> getAllReplicaSets(String database, TConsensusGroupType type) {
    return partitionInfo.getAllReplicaSets(database, type);
  }

  /**
   * Get all RegionGroups currently owned by the specified Database.
   *
   * @param dataNodeId The specified dataNodeId
   * @return Deep copy of all RegionGroups' RegionReplicaSet with the specified dataNodeId
   */
  public List<TRegionReplicaSet> getAllReplicaSets(int dataNodeId) {
    return partitionInfo.getAllReplicaSets(dataNodeId);
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
    return partitionInfo.getReplicaSets(database, regionGroupIds);
  }

  public boolean isDataNodeContainsRegion(int dataNodeId, TConsensusGroupId regionId) {
    return getAllReplicaSets(dataNodeId).stream()
        .anyMatch(tRegionReplicaSet -> tRegionReplicaSet.getRegionId().equals(regionId));
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
   * <p>Count the scatter width of the specified DataNode
   *
   * @param dataNodeId The specified DataNode
   * @param type SchemaRegion or DataRegion
   * @return The schema/data scatter width of the specified DataNode. The scatter width refers to
   *     the number of other DataNodes in the cluster which have at least one identical schema/data
   *     replica as the specified DataNode.
   */
  public int countDataNodeScatterWidth(int dataNodeId, TConsensusGroupType type) {
    int clusterNodeCount = getNodeManager().getRegisteredNodeCount();
    return partitionInfo.countDataNodeScatterWidth(dataNodeId, type, clusterNodeCount);
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

  /**
   * Only leader use this interface.
   *
   * <p>Get the all RegionGroups currently in the cluster
   *
   * @param type SchemaRegion or DataRegion
   * @return Map<Database, List<RegionGroupIds>>
   */
  public Map<String, List<TConsensusGroupId>> getAllRegionGroupIdMap(TConsensusGroupType type) {
    return partitionInfo.getAllRegionGroupIdMap(type);
  }

  /**
   * Only leader use this interface.
   *
   * <p>Get all the RegionGroups currently owned by the specified Database
   *
   * @param database DatabaseName
   * @param type SchemaRegion or DataRegion
   * @return List of TConsensusGroupId
   * @throws DatabaseNotExistsException When the specified Database doesn't exist
   */
  public List<TConsensusGroupId> getAllRegionGroupIds(String database, TConsensusGroupType type)
      throws DatabaseNotExistsException {
    return partitionInfo.getAllRegionGroupIds(database, type);
  }

  /**
   * Check if the specified Database exists.
   *
   * @param database The specified Database
   * @return True if the DatabaseSchema is exists and the Database is not pre-deleted
   */
  public boolean isDatabaseExist(String database) {
    return partitionInfo.isDatabaseExisted(database);
  }

  /**
   * Filter the un-exist Databases.
   *
   * @param databases the Databases to check
   * @return List of PartialPath the Databases that not exist
   */
  public List<PartialPath> filterUnExistDatabases(List<PartialPath> databases) {
    List<PartialPath> unExistDatabases = new ArrayList<>();
    if (databases == null) {
      return unExistDatabases;
    }
    for (PartialPath database : databases) {
      if (!isDatabaseExist(database.getFullPath())) {
        unExistDatabases.add(database);
      }
    }
    return unExistDatabases;
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
   * <p>Get the assigned TimePartitionSlots count in the specified Database
   *
   * @param database The specified Database
   * @return The assigned TimePartitionSlots count
   */
  public long getAssignedTimePartitionSlotsCount(String database) {
    return partitionInfo.getAssignedTimePartitionSlotsCount(database);
  }

  /**
   * Only leader use this interface.
   *
   * @param database DatabaseName
   * @param type SchemaRegion or DataRegion
   * @return The specific StorageGroup's Regions that sorted by the number of allocated slots
   * @throws NoAvailableRegionGroupException When all RegionGroups within the specified StorageGroup
   *     are unavailable currently
   */
  public List<Pair<Long, TConsensusGroupId>> getSortedRegionGroupSlotsCounter(
      String database, TConsensusGroupType type) throws NoAvailableRegionGroupException {
    // Collect static data
    List<Pair<Long, TConsensusGroupId>> regionGroupSlotsCounter =
        partitionInfo.getRegionGroupSlotsCounter(database, type);

    // Filter RegionGroups that have Disabled status
    List<Pair<Long, TConsensusGroupId>> result = new ArrayList<>();
    for (Pair<Long, TConsensusGroupId> slotsCounter : regionGroupSlotsCounter) {
      RegionGroupStatus status = getLoadManager().getRegionGroupStatus(slotsCounter.getRight());
      if (!RegionGroupStatus.Disabled.equals(status)) {
        result.add(slotsCounter);
      }
    }

    if (result.isEmpty()) {
      throw new NoAvailableRegionGroupException(type);
    }

    Map<TConsensusGroupId, RegionGroupStatus> regionGroupStatusMap =
        getLoadManager()
            .getRegionGroupStatus(result.stream().map(Pair::getRight).collect(Collectors.toList()));
    result.sort(
        (o1, o2) -> {
          // Use the number of partitions as the first priority
          if (o1.getLeft() < o2.getLeft()) {
            return -1;
          } else if (o1.getLeft() > o2.getLeft()) {
            return 1;
          } else {
            // Use RegionGroup status as second priority, Running > Available > Discouraged
            return regionGroupStatusMap
                .get(o1.getRight())
                .compare(regionGroupStatusMap.get(o2.getRight()));
          }
        });
    return result;
  }

  /**
   * Only leader use this interface.
   *
   * @return Integer set of all schemaengine region id
   */
  public Set<Integer> getAllSchemaPartition() {
    return partitionInfo.getAllSchemaPartition();
  }

  /**
   * Only leader use this interface.
   *
   * @return the next RegionGroupId
   */
  public int generateNextRegionGroupId() {
    return partitionInfo.generateNextRegionGroupId();
  }

  public Optional<TConsensusGroupId> generateTConsensusGroupIdByRegionId(final int regionId) {
    if (configManager
        .getPartitionManager()
        .isRegionGroupExists(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, regionId))) {
      return Optional.of(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, regionId));
    }
    if (configManager
        .getPartitionManager()
        .isRegionGroupExists(new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId))) {
      return Optional.of(new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId));
    }
    String msg =
        String.format(
            "Submit RegionMigrateProcedure failed, because RegionGroup: %s doesn't exist",
            regionId);
    LOGGER.warn(msg);
    return Optional.empty();
  }

  /**
   * GetNodePathsPartition.
   *
   * @param physicalPlan GetNodesPathsPartitionReq
   * @return SchemaNodeManagementPartitionDataSet that contains only existing matched
   *     SchemaPartition and matched child paths aboveMTree
   */
  public SchemaNodeManagementResp getNodePathsPartition(GetNodePathsPartitionPlan physicalPlan) {
    try {
      return (SchemaNodeManagementResp) getConsensusManager().read(physicalPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      SchemaNodeManagementResp resp = new SchemaNodeManagementResp();
      resp.setStatus(res);
      return resp;
    }
  }

  public void preDeleteDatabase(
      String database, PreDeleteDatabasePlan.PreDeleteType preDeleteType) {
    final PreDeleteDatabasePlan preDeleteDatabasePlan =
        new PreDeleteDatabasePlan(database, preDeleteType);
    try {
      getConsensusManager().write(preDeleteDatabasePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
    }
  }

  public boolean isDatabasePreDeleted(String database) {
    return partitionInfo.isDatabasePreDeleted(database);
  }

  /**
   * Get TSeriesPartitionSlot.
   *
   * @param deviceID
   * @return SeriesPartitionSlot
   */
  public TSeriesPartitionSlot getSeriesPartitionSlot(IDeviceID deviceID) {
    return executor.getSeriesPartitionSlot(deviceID);
  }

  public RegionInfoListResp getRegionInfoList(GetRegionInfoListPlan req) {
    try {
      // Get static result
      RegionInfoListResp regionInfoListResp = (RegionInfoListResp) getConsensusManager().read(req);
      // Get cached result
      Map<TConsensusGroupId, Integer> allLeadership = getLoadManager().getRegionLeaderMap();
      regionInfoListResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                regionInfo.setStatus(
                    getLoadManager()
                        .getRegionStatus(
                            regionInfo.getConsensusGroupId(), regionInfo.getDataNodeId())
                        .getStatus());

                String regionType =
                    regionInfo.getDataNodeId()
                            == allLeadership.getOrDefault(regionInfo.getConsensusGroupId(), -1)
                        ? RegionRoleType.Leader.toString()
                        : RegionRoleType.Follower.toString();
                regionInfo.setRoleType(regionType);
              });

      return regionInfoListResp;

    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      RegionInfoListResp resp = new RegionInfoListResp();
      resp.setStatus(res);
      return resp;
    }
  }

  /**
   * Check if the specified RegionGroup exists.
   *
   * @param regionGroupId The specified RegionGroup
   */
  public boolean isRegionGroupExists(TConsensusGroupId regionGroupId) {
    return partitionInfo.isRegionGroupExisted(regionGroupId);
  }

  public TSStatus addRegionLocation(AddRegionLocationPlan req) {
    try {
      return getConsensusManager().write(req);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus removeRegionLocation(RemoveRegionLocationPlan req) {
    try {
      return getConsensusManager().write(req);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public GetRegionIdResp getRegionId(TGetRegionIdReq req) {
    GetRegionIdPlan plan = new GetRegionIdPlan(req.getType());
    if (req.isSetDatabase()) {
      plan.setDatabase(req.getDatabase());
    } else {
      IDeviceID deviceID =
          Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(ByteBuffer.wrap(req.getDevice()));
      plan.setDatabase(getClusterSchemaManager().getDatabaseNameByDevice(deviceID));
      plan.setSeriesSlotId(executor.getSeriesPartitionSlot(deviceID));
    }
    if (Objects.equals(plan.getDatabase(), "")) {
      // Return empty result if Database not specified
      return new GetRegionIdResp(RpcUtils.SUCCESS_STATUS, new ArrayList<>());
    }
    if (req.isSetStartTimeSlot()) {
      plan.setStartTimeSlotId(req.getStartTimeSlot());
    } else {
      plan.setStartTimeSlotId(new TTimePartitionSlot(0));
    }
    if (req.isSetEndTimeSlot()) {
      plan.setEndTimeSlotId(req.getEndTimeSlot());
    } else {
      plan.setEndTimeSlotId(new TTimePartitionSlot(Long.MAX_VALUE));
    }

    try {
      return (GetRegionIdResp) getConsensusManager().read(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new GetRegionIdResp(res, Collections.emptyList());
    }
  }

  public GetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) {
    long startTime = req.isSetStartTime() ? req.getStartTime() : Long.MIN_VALUE;
    long endTime = req.isSetEndTime() ? req.getEndTime() : Long.MAX_VALUE;
    GetTimeSlotListPlan plan = new GetTimeSlotListPlan(startTime, endTime);
    if (req.isSetDatabase()) {
      plan.setDatabase(req.getDatabase());
    } else if (req.isSetDevice()) {
      IDeviceID deviceID =
          Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(ByteBuffer.wrap(req.getDevice()));
      plan.setDatabase(getClusterSchemaManager().getDatabaseNameByDevice(deviceID));
      plan.setSeriesSlotId(executor.getSeriesPartitionSlot(deviceID));
      if (Objects.equals(plan.getDatabase(), "")) {
        // Return empty result if Database not specified
        return new GetTimeSlotListResp(RpcUtils.SUCCESS_STATUS, new ArrayList<>());
      }
    } else {
      plan.setRegionId(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, (int) req.getRegionId()));
    }
    try {
      return (GetTimeSlotListResp) getConsensusManager().read(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new GetTimeSlotListResp(res, Collections.emptyList());
    }
  }

  public CountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) {
    long startTime = req.isSetStartTime() ? req.getStartTime() : Long.MIN_VALUE;
    long endTime = req.isSetEndTime() ? req.getEndTime() : Long.MAX_VALUE;
    CountTimeSlotListPlan plan = new CountTimeSlotListPlan(startTime, endTime);
    if (req.isSetDatabase()) {
      plan.setDatabase(req.getDatabase());
    } else if (req.isSetDevice()) {
      IDeviceID deviceID =
          Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(ByteBuffer.wrap(req.getDevice()));
      plan.setDatabase(getClusterSchemaManager().getDatabaseNameByDevice(deviceID));
      plan.setSeriesSlotId(executor.getSeriesPartitionSlot(deviceID));
      if (Objects.equals(plan.getDatabase(), "")) {
        // Return empty result if Database not specified
        return new CountTimeSlotListResp(RpcUtils.SUCCESS_STATUS, 0);
      }
    } else {
      plan.setRegionId(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, (int) req.getRegionId()));
    }
    try {
      return (CountTimeSlotListResp) getConsensusManager().read(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new CountTimeSlotListResp(res, 0);
    }
  }

  public GetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) {
    GetSeriesSlotListPlan plan = new GetSeriesSlotListPlan(req.getDatabase(), req.getType());
    try {
      return (GetSeriesSlotListResp) getConsensusManager().read(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_READ_ERROR, e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new GetSeriesSlotListResp(res, Collections.emptyList());
    }
  }

  /**
   * Get database for region.
   *
   * @param regionId regionId
   * @return database name
   */
  public String getRegionStorageGroup(TConsensusGroupId regionId) {
    return partitionInfo.getRegionStorageGroup(regionId);
  }

  /**
   * Called by {@link PartitionManager#regionMaintainer}.
   *
   * <p>Periodically maintain the RegionReplicas to be created or deleted
   */
  public void maintainRegionReplicas() {
    // The consensusManager of configManager may not be fully initialized at this time
    Optional.ofNullable(getConsensusManager())
        .ifPresent(
            consensusManager -> {
              if (getConsensusManager().isLeader()) {
                List<RegionMaintainTask> regionMaintainTaskList =
                    partitionInfo.getRegionMaintainEntryList();

                if (regionMaintainTaskList.isEmpty()) {
                  return;
                }

                // Group tasks by region id
                Map<TConsensusGroupId, Queue<RegionMaintainTask>> regionMaintainTaskMap =
                    new HashMap<>();
                for (RegionMaintainTask regionMaintainTask : regionMaintainTaskList) {
                  regionMaintainTaskMap
                      .computeIfAbsent(regionMaintainTask.getRegionId(), k -> new LinkedList<>())
                      .add(regionMaintainTask);
                }

                while (!regionMaintainTaskMap.isEmpty()) {
                  // Select same type task from each region group
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
                        // Delete or same create task
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
                          DataNodeAsyncRequestContext<TCreateSchemaRegionReq, TSStatus>
                              createSchemaRegionHandler =
                                  new DataNodeAsyncRequestContext<>(
                                      CnToDnRequestType.CREATE_SCHEMA_REGION);
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
                            createSchemaRegionHandler.putNodeLocation(
                                schemaRegionCreateTask.getRegionId().getId(),
                                schemaRegionCreateTask.getTargetDataNode());
                          }

                          CnToDnInternalServiceAsyncRequestManager.getInstance()
                              .sendAsyncRequestWithRetry(createSchemaRegionHandler);

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
                          // Create DataRegion
                          DataNodeAsyncRequestContext<TCreateDataRegionReq, TSStatus>
                              createDataRegionHandler =
                                  new DataNodeAsyncRequestContext<>(
                                      CnToDnRequestType.CREATE_DATA_REGION);
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
                                    dataRegionCreateTask.getStorageGroup()));
                            createDataRegionHandler.putNodeLocation(
                                dataRegionCreateTask.getRegionId().getId(),
                                dataRegionCreateTask.getTargetDataNode());
                          }

                          CnToDnInternalServiceAsyncRequestManager.getInstance()
                              .sendAsyncRequestWithRetry(createDataRegionHandler);

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
                      DataNodeAsyncRequestContext<TConsensusGroupId, TSStatus> deleteRegionHandler =
                          new DataNodeAsyncRequestContext<>(CnToDnRequestType.DELETE_REGION);
                      Map<Integer, TConsensusGroupId> regionIdMap = new HashMap<>();
                      for (RegionMaintainTask regionMaintainTask : selectedRegionMaintainTask) {
                        RegionDeleteTask regionDeleteTask = (RegionDeleteTask) regionMaintainTask;
                        LOGGER.info(
                            "Start to delete Region: {} on DataNode: {}",
                            regionDeleteTask.getRegionId(),
                            regionDeleteTask.getTargetDataNode());
                        deleteRegionHandler.putRequest(
                            regionDeleteTask.getRegionId().getId(), regionDeleteTask.getRegionId());
                        deleteRegionHandler.putNodeLocation(
                            regionDeleteTask.getRegionId().getId(),
                            regionDeleteTask.getTargetDataNode());
                        regionIdMap.put(
                            regionDeleteTask.getRegionId().getId(), regionDeleteTask.getRegionId());
                      }

                      long startTime = System.currentTimeMillis();
                      CnToDnInternalServiceAsyncRequestManager.getInstance()
                          .sendAsyncRequestWithRetry(deleteRegionHandler);

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
                  try {
                    getConsensusManager()
                        .write(new PollSpecificRegionMaintainTaskPlan(successfulTask));
                  } catch (ConsensusException e) {
                    LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
                  }

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
        LOGGER.info("RegionCleaner is stopped successfully.");
      }
    }
  }

  /**
   * Filter the RegionGroups in the specified Database through the RegionGroupStatus.
   *
   * @param database The specified Database
   * @param status The specified RegionGroupStatus
   * @return Filtered RegionGroups with the specified RegionGroupStatus
   */
  public List<TRegionReplicaSet> filterRegionGroupThroughStatus(
      String database, RegionGroupStatus... status) {
    List<TConsensusGroupId> matchedRegionGroups =
        getLoadManager().filterRegionGroupThroughStatus(status);
    return getReplicaSets(database, matchedRegionGroups);
  }

  public void getSchemaRegionIds(
      List<String> databases, Map<String, List<Integer>> schemaRegionIds) {
    partitionInfo.getSchemaRegionIds(databases, schemaRegionIds);
  }

  public void getDataRegionIds(List<String> databases, Map<String, List<Integer>> dataRegionIds) {
    partitionInfo.getDataRegionIds(databases, dataRegionIds);
  }

  /**
   * Get the {@link TConsensusGroupType} of the given integer regionId.
   *
   * @param regionId The specified integer regionId
   * @return {@link Optional#of(Object tConsensusGroupType)} of the given integer regionId, or
   *     {@link Optional#empty()} if the integer regionId does not match any of the regionGroups.
   */
  public Optional<TConsensusGroupType> getRegionType(int regionId) {
    return partitionInfo.getRegionType(regionId);
  }

  /**
   * Get the last DataAllotTable of the specified Database.
   *
   * @param database The specified Database
   * @return The last DataAllotTable
   */
  public Map<TSeriesPartitionSlot, TConsensusGroupId> getLastDataAllotTable(String database) {
    return partitionInfo.getLastDataAllotTable(database);
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

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }
}
