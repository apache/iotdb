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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.client.sync.datanode.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.heartbeat.IRegionGroupCache;
import org.apache.iotdb.confignode.persistence.metric.PartitionInfoMetrics;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** The PartitionManager Manages cluster PartitionTable read and write requests. */
public class PartitionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  private final IManager configManager;
  private final PartitionInfo partitionInfo;

  private SeriesPartitionExecutor executor;

  /** Region cleaner */
  // Monitor for leadership change
  private final Object scheduleMonitor = new Object();
  // Try to delete Regions in every 10s
  private static final int REGION_CLEANER_WORK_INTERVAL = 10;
  private final ScheduledExecutorService regionCleaner;
  private Future<?> currentRegionCleanerFuture;

  // Map<RegionId, RegionGroupCache>
  private final Map<TConsensusGroupId, IRegionGroupCache> regionGroupCacheMap;

  public PartitionManager(IManager configManager, PartitionInfo partitionInfo) {
    this.configManager = configManager;
    this.partitionInfo = partitionInfo;
    this.regionCleaner =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("IoTDB-Region-Cleaner");
    this.regionGroupCacheMap = new ConcurrentHashMap<>();
    setSeriesPartitionExecutor();
  }

  /** Construct SeriesPartitionExecutor by iotdb-confignode.properties */
  private void setSeriesPartitionExecutor() {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    this.executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            conf.getSeriesPartitionExecutorClass(), conf.getSeriesPartitionSlotNum());
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
   *     List<TimePartitionSlot>>>
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
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionPlan req) {
    // After all the SchemaPartitions are allocated,
    // all the read requests about SchemaPartitionTable are parallel.
    SchemaPartitionResp resp = (SchemaPartitionResp) getSchemaPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of SchemaPartitions to
    // ensure that each SchemaPartition is created by a unique CreateSchemaPartitionReq.
    // Because the number of SchemaPartitions per storage group is limited
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
          extendRegionsIfNecessary(
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
        Map<String, SchemaPartitionTable> assignedSchemaPartition =
            getLoadManager().allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
        // Cache allocating result
        CreateSchemaPartitionPlan createPlan = new CreateSchemaPartitionPlan();
        createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
        getConsensusManager().write(createPlan);
      }
    }

    return getSchemaPartition(req);
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
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionPlan req) {
    // After all the DataPartitions are allocated,
    // all the read requests about DataPartitionTable are parallel.
    DataPartitionResp resp = (DataPartitionResp) getDataPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // We serialize the creation process of DataPartitions to
    // ensure that each DataPartition is created by a unique CreateDataPartitionReq.
    // Because the number of DataPartitions per storage group is limited
    // by the number of SeriesPartitionSlots,
    // the number of serialized CreateDataPartitionReqs is acceptable.
    synchronized (this) {
      // Filter unassigned DataPartitionSlots
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
          unassignedDataPartitionSlotsMap =
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
          extendRegionsIfNecessary(
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
        Map<String, DataPartitionTable> assignedDataPartition =
            getLoadManager().allocateDataPartition(unassignedDataPartitionSlotsMap);
        // Cache allocating result
        CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
        createPlan.setAssignedDataPartition(assignedDataPartition);
        getConsensusManager().write(createPlan);
      }
    }

    return getDataPartition(req);
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Allocate more Regions to StorageGroups who have too many slots.
   *
   * @param unassignedPartitionSlotsCountMap Map<StorageGroup, unassigned Partition count>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return SUCCESS_STATUS when Region extension successful; NOT_ENOUGH_DATA_NODE when there are
   *     not enough DataNodes; STORAGE_GROUP_NOT_EXIST when some StorageGroups don't exist
   */
  private TSStatus extendRegionsIfNecessary(
      Map<String, Integer> unassignedPartitionSlotsCountMap,
      TConsensusGroupType consensusGroupType) {
    TSStatus result = new TSStatus();

    try {
      // Map<StorageGroup, Region allotment>
      Map<String, Integer> allotmentMap = new ConcurrentHashMap<>();

      for (Map.Entry<String, Integer> entry : unassignedPartitionSlotsCountMap.entrySet()) {
        float allocatedRegionCount =
            partitionInfo.getRegionCount(entry.getKey(), consensusGroupType);
        // The slotCount equals to the sum of assigned slot count and unassigned slot count
        float slotCount =
            partitionInfo.getAssignedSeriesPartitionSlotsCount(entry.getKey()) + entry.getValue();
        float maxRegionCount =
            getClusterSchemaManager().getMaxRegionGroupCount(entry.getKey(), consensusGroupType);
        float maxSlotCount =
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum();

        /* Region extension is required in the following two cases  */
        // 1. There are no Region has been created for the current StorageGroup
        if (allocatedRegionCount == 0) {
          // The delta is equal to the smallest integer solution that satisfies the inequality:
          // slotCount / delta < maxSlotCount / maxRegionCount
          int delta =
              Math.min(
                  (int) maxRegionCount,
                  Math.max(1, (int) Math.ceil(slotCount * maxRegionCount / maxSlotCount)));
          allotmentMap.put(entry.getKey(), delta);
          continue;
        }

        // 2. The average number of partitions held by each Region will be greater than the
        // expected average number after the partition allocation is completed
        if (allocatedRegionCount < maxRegionCount
            && slotCount / allocatedRegionCount > maxSlotCount / maxRegionCount) {
          // The delta is equal to the smallest integer solution that satisfies the inequality:
          // slotCount / (allocatedRegionCount + delta) < maxSlotCount / maxRegionCount
          int delta =
              Math.min(
                  (int) (maxRegionCount - allocatedRegionCount),
                  Math.max(
                      1,
                      (int)
                          Math.ceil(
                              slotCount * maxRegionCount / maxSlotCount - allocatedRegionCount)));
          allotmentMap.put(entry.getKey(), delta);
        }
      }

      if (!allotmentMap.isEmpty()) {
        CreateRegionGroupsPlan createRegionGroupsPlan =
            getLoadManager().allocateRegionGroups(allotmentMap, consensusGroupType);
        result = getProcedureManager().createRegionGroups(createRegionGroupsPlan);
      } else {
        result = RpcUtils.SUCCESS_STATUS;
      }
    } catch (NotEnoughDataNodeException e) {
      LOGGER.error("ConfigNode failed to extend Region because there are not enough DataNodes");
      result.setCode(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode());
    } catch (StorageGroupNotExistsException e) {
      LOGGER.error("ConfigNode failed to extend Region because some StorageGroup doesn't exist.");
      result.setCode(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode());
    }

    return result;
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
   * Get the DataNodes who contain the specific StorageGroup's Schema or Data
   *
   * @param storageGroup The specific StorageGroup's name
   * @param type SchemaRegion or DataRegion
   * @return Set<TDataNodeLocation>, the related DataNodes
   */
  public Set<TDataNodeLocation> getStorageGroupRelatedDataNodes(
      String storageGroup, TConsensusGroupType type) {
    return partitionInfo.getStorageGroupRelatedDataNodes(storageGroup, type);
  }
  /**
   * Only leader use this interface
   *
   * @return All Regions' RegionReplicaSet
   */
  public List<TRegionReplicaSet> getAllReplicaSets() {
    return partitionInfo.getAllReplicaSets();
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
    return partitionInfo.getRegionCount(storageGroup, type);
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
    return partitionInfo.getSortedRegionSlotsCounter(storageGroup, type);
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
  public DataSet getNodePathsPartition(GetNodePathsPartitionPlan physicalPlan) {
    SchemaNodeManagementResp schemaNodeManagementResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaNodeManagementResp = (SchemaNodeManagementResp) consensusReadResponse.getDataset();
    return schemaNodeManagementResp;
  }

  public void preDeleteStorageGroup(
      String storageGroup, PreDeleteStorageGroupPlan.PreDeleteType preDeleteType) {
    final PreDeleteStorageGroupPlan preDeleteStorageGroupPlan =
        new PreDeleteStorageGroupPlan(storageGroup, preDeleteType);
    getConsensusManager().write(preDeleteStorageGroupPlan);
  }

  public void addMetrics() {
    MetricService.getInstance().addMetricSet(new PartitionInfoMetrics(partitionInfo));
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

  public DataSet getRegionInfoList(GetRegionInfoListPlan req) {
    return getConsensusManager().read(req).getDataset();
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

  /**
   * get storage group for region
   *
   * @param regionId regionId
   * @return storage group name
   */
  public String getRegionStorageGroup(TConsensusGroupId regionId) {
    return partitionInfo.getRegionStorageGroup(regionId);
  }

  /** Called by {@link PartitionManager#regionCleaner} Delete RegionGroups periodically. */
  public void clearDeletedRegions() {
    // the consensusManager of configManager may not be fully initialized at this time
    Optional.ofNullable(getConsensusManager())
        .ifPresent(
            consensusManager -> {
              if (getConsensusManager().isLeader()) {
                final Set<TRegionReplicaSet> deletedRegionSet = partitionInfo.getDeletedRegionSet();
                if (!deletedRegionSet.isEmpty()) {
                  LOGGER.info(
                      "DELETE REGIONS {} START",
                      deletedRegionSet.stream()
                          .map(TRegionReplicaSet::getRegionId)
                          .collect(Collectors.toList()));
                  deletedRegionSet.forEach(
                      regionReplicaSet -> removeRegionGroupCache(regionReplicaSet.regionId));
                  SyncDataNodeClientPool.getInstance().deleteRegions(deletedRegionSet);
                }
              }
            });
  }

  public void startRegionCleaner() {
    synchronized (scheduleMonitor) {
      if (currentRegionCleanerFuture == null) {
        /* Start the RegionCleaner service */
        currentRegionCleanerFuture =
            ScheduledExecutorUtil.safelyScheduleAtFixedRate(
                regionCleaner,
                this::clearDeletedRegions,
                0,
                REGION_CLEANER_WORK_INTERVAL,
                TimeUnit.SECONDS);
        LOGGER.info("RegionCleaner is started successfully.");
      }
    }
  }

  public void stopRegionCleaner() {
    synchronized (scheduleMonitor) {
      if (currentRegionCleanerFuture != null) {
        /* Stop the RegionCleaner service */
        currentRegionCleanerFuture.cancel(false);
        currentRegionCleanerFuture = null;
        regionGroupCacheMap.clear();
        LOGGER.info("RegionCleaner is stopped successfully.");
      }
    }
  }

  public Map<TConsensusGroupId, IRegionGroupCache> getRegionGroupCacheMap() {
    return regionGroupCacheMap;
  }

  public void removeRegionGroupCache(TConsensusGroupId consensusGroupId) {
    regionGroupCacheMap.remove(consensusGroupId);
  }

  /**
   * Get the leadership of each RegionGroup If a node is in unknown or removing status, this node
   * can't be leader
   *
   * @return Map<RegionGroupId, leader location>
   */
  public Map<TConsensusGroupId, Integer> getAllLeadership() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();
    if (ConfigNodeDescriptor.getInstance()
        .getConf()
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.MultiLeaderConsensus)) {
      regionGroupCacheMap.forEach(
          (consensusGroupId, regionGroupCache) -> {
            if (consensusGroupId.getType().equals(TConsensusGroupType.SchemaRegion)) {
              int leaderDataNodeId = regionGroupCache.getLeaderDataNodeId();
              if (configManager.getNodeManager().isNodeRemoving(leaderDataNodeId)) {
                result.put(consensusGroupId, -1);
              } else {
                result.put(consensusGroupId, leaderDataNodeId);
              }
            }
          });
      getLoadManager()
          .getRouteBalancer()
          .getRouteMap()
          .forEach(
              (consensusGroupId, regionReplicaSet) ->
                  result.put(
                      consensusGroupId,
                      regionReplicaSet.getDataNodeLocations().get(0).getDataNodeId()));
    } else {
      regionGroupCacheMap.forEach(
          (consensusGroupId, regionGroupCache) -> {
            int leaderDataNodeId = regionGroupCache.getLeaderDataNodeId();
            if (configManager.getNodeManager().isNodeRemoving(leaderDataNodeId)) {
              result.put(consensusGroupId, -1);
            } else {
              result.put(consensusGroupId, leaderDataNodeId);
            }
          });
    }
    return result;
  }

  public ScheduledExecutorService getRegionCleaner() {
    return regionCleaner;
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
