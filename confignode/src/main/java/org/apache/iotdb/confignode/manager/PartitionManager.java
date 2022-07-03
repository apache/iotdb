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
import org.apache.iotdb.confignode.client.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.exception.TimeoutException;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** The PartitionManager Manages cluster PartitionTable read and write requests. */
public class PartitionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  private final IManager configManager;
  private final PartitionInfo partitionInfo;
  private static final int REGION_CLEANER_WORK_INTERVAL = 300;
  private static final int REGION_CLEANER_WORK_INITIAL_DELAY = 10;

  private SeriesPartitionExecutor executor;
  private final ScheduledExecutorService regionCleaner;

  public PartitionManager(IManager configManager, PartitionInfo partitionInfo) {
    this.configManager = configManager;
    this.partitionInfo = partitionInfo;
    this.regionCleaner =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("IoTDB-Region-Cleaner");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        regionCleaner,
        this::clearDeletedRegions,
        REGION_CLEANER_WORK_INITIAL_DELAY,
        REGION_CLEANER_WORK_INTERVAL,
        TimeUnit.SECONDS);
    setSeriesPartitionExecutor();
  }

  /** Construct SeriesPartitionExecutor by iotdb-confignode.propertis */
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
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions. TIME_OUT
   *     if waiting other threads to create Regions for too long. STORAGE_GROUP_NOT_EXIST if some
   *     StorageGroup doesn't exist.
   */
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionPlan req) {
    // After all the SchemaPartitions are allocated,
    // all the read requests about SchemaPartitionTable are parallel.
    SchemaPartitionResp resp = (SchemaPartitionResp) getSchemaPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // Otherwise, fist ensure that each StorageGroup has at least one SchemaRegion.
    // This block of code is still parallel and concurrent safe.
    // Thus, we can prepare the SchemaRegions with maximum efficiency.
    TSStatus status =
        initializeRegionsIfNecessary(
            new ArrayList<>(req.getPartitionSlotsMap().keySet()), TConsensusGroupType.SchemaRegion);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setStatus(status);
      return resp;
    }

    // Next, we serialize the creation process of SchemaPartitions to
    // ensure that each SchemaPartition is created by a unique CreateSchemaPartitionReq.
    // Because the number of SchemaPartitions per storage group is limited by the number of
    // SeriesPartitionSlots,
    // the number of serialized CreateSchemaPartitionReqs is acceptable.
    synchronized (this) {
      // Filter unassigned SchemaPartitionSlots
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlots =
          partitionInfo.filterUnassignedSchemaPartitionSlots(req.getPartitionSlotsMap());
      if (unassignedSchemaPartitionSlots.size() > 0) {
        // Allocate SchemaPartitions
        Map<String, SchemaPartitionTable> assignedSchemaPartition =
            getLoadManager().allocateSchemaPartition(unassignedSchemaPartitionSlots);
        // Cache allocating result
        CreateSchemaPartitionPlan createPlan = new CreateSchemaPartitionPlan();
        createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
        getConsensusManager().write(createPlan);
      }
    }

    // Finally, if some StorageGroups own too many slots, extend SchemaRegion for them.
    extendRegionsIfNecessary(
        new ArrayList<>(req.getPartitionSlotsMap().keySet()), TConsensusGroupType.SchemaRegion);

    return getSchemaPartition(req);
  }

  /**
   * Get DataPartition and create a new one if it does not exist
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions. TIME_OUT
   *     if waiting other threads to create Regions for too long. STORAGE_GROUP_NOT_EXIST if some
   *     StorageGroup doesn't exist.
   */
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionPlan req) {
    // After all the SchemaPartitions are allocated,
    // all the read requests about SchemaPartitionTable are parallel.
    DataPartitionResp resp = (DataPartitionResp) getDataPartition(req);
    if (resp.isAllPartitionsExist()) {
      return resp;
    }

    // Otherwise, fist ensure that each StorageGroup has at least one DataRegion.
    // This block of code is still parallel and concurrent safe.
    // Thus, we can prepare the DataRegions with maximum efficiency.
    TSStatus status =
        initializeRegionsIfNecessary(
            new ArrayList<>(req.getPartitionSlotsMap().keySet()), TConsensusGroupType.DataRegion);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setStatus(status);
      return resp;
    }

    // Next, we serialize the creation process of DataPartitions to
    // ensure that each DataPartition is created by a unique CreateDataPartitionReq.
    // Because the number of DataPartitions per storage group per day is limited by the number of
    // SeriesPartitionSlots,
    // the number of serialized CreateDataPartitionReqs is acceptable.
    synchronized (this) {
      // Filter unassigned DataPartitionSlots
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
          unassignedDataPartitionSlots =
              partitionInfo.filterUnassignedDataPartitionSlots(req.getPartitionSlotsMap());
      if (unassignedDataPartitionSlots.size() > 0) {
        // Allocate DataPartitions
        Map<String, DataPartitionTable> assignedDataPartition =
            getLoadManager().allocateDataPartition(unassignedDataPartitionSlots);
        // Cache allocating result
        CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
        createPlan.setAssignedDataPartition(assignedDataPartition);
        getConsensusManager().write(createPlan);
      }
    }

    // Finally, if some StorageGroups own too many slots, extend DataRegion for them.
    extendRegionsIfNecessary(
        new ArrayList<>(req.getPartitionSlotsMap().keySet()), TConsensusGroupType.DataRegion);

    return getDataPartition(req);
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /** Handle the exceptions from initializeRegions */
  private TSStatus initializeRegionsIfNecessary(
      List<String> storageGroups, TConsensusGroupType consensusGroupType) {
    try {
      initializeRegions(storageGroups, consensusGroupType);
    } catch (NotEnoughDataNodeException e) {
      return new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode())
          .setMessage(
              "ConfigNode failed to allocate Partition because there are not enough DataNodes");
    } catch (TimeoutException e) {
      return new TSStatus(TSStatusCode.TIME_OUT.getStatusCode())
          .setMessage(
              "ConfigNode failed to allocate Partition because waiting for another thread's Region allocation timeout.");
    } catch (StorageGroupNotExistsException e) {
      return new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
          .setMessage(
              "ConfigNode failed to allocate DataPartition because some StorageGroup doesn't exist.");
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Initialize one Region for each StorageGroup who doesn't have any.
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @throws NotEnoughDataNodeException When the number of online DataNodes are too small to
   *     allocate Regions
   * @throws TimeoutException When waiting other threads to allocate Regions for too long
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   */
  private void initializeRegions(List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, TimeoutException, StorageGroupNotExistsException {

    int leastDataNode = 0;
    Map<String, Integer> unreadyStorageGroupMap = new HashMap<>();
    for (String storageGroup : storageGroups) {
      if (getRegionCount(storageGroup, consensusGroupType) == 0) {
        // Update leastDataNode
        TStorageGroupSchema storageGroupSchema =
            getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup);
        switch (consensusGroupType) {
          case SchemaRegion:
            leastDataNode =
                Math.max(leastDataNode, storageGroupSchema.getSchemaReplicationFactor());
            break;
          case DataRegion:
            leastDataNode = Math.max(leastDataNode, storageGroupSchema.getDataReplicationFactor());
        }

        // Recording StorageGroups without Region
        unreadyStorageGroupMap.put(storageGroup, 1);
      }
    }
    if (getNodeManager().getOnlineDataNodeCount() < leastDataNode) {
      // Make sure DataNodes enough
      throw new NotEnoughDataNodeException();
    }

    doOrWaitRegionCreation(unreadyStorageGroupMap, consensusGroupType);
  }

  /** Handle the exceptions from extendRegions */
  private void extendRegionsIfNecessary(
      List<String> storageGroups, TConsensusGroupType consensusGroupType) {
    try {
      extendRegions(storageGroups, consensusGroupType);
    } catch (NotEnoughDataNodeException e) {
      LOGGER.error("ConfigNode failed to extend Region because there are not enough DataNodes");
    } catch (TimeoutException e) {
      LOGGER.error(
          "ConfigNode failed to extend Region because waiting for another thread's Region allocation timeout.");
    } catch (StorageGroupNotExistsException e) {
      LOGGER.error("ConfigNode failed to extend Region because some StorageGroup doesn't exist.");
    }
  }

  /**
   * Allocate more Regions to StorageGroups who have too many slots.
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   * @throws NotEnoughDataNodeException When the number of online DataNodes are too small to
   *     allocate Regions
   * @throws TimeoutException When waiting other threads to allocate Regions for too long
   */
  private void extendRegions(List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws StorageGroupNotExistsException, NotEnoughDataNodeException, TimeoutException {
    // Map<StorageGroup, Region allotment>
    Map<String, Integer> filledStorageGroupMap = new HashMap<>();
    for (String storageGroup : storageGroups) {
      float regionCount = partitionInfo.getRegionCount(storageGroup, consensusGroupType);
      float slotCount = partitionInfo.getSlotCount(storageGroup);
      float maxRegionCount =
          getClusterSchemaManager().getMaxRegionGroupCount(storageGroup, consensusGroupType);
      float maxSlotCount = ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum();

      // Need extension
      if (regionCount < maxRegionCount && slotCount / regionCount > maxSlotCount / maxRegionCount) {
        // The delta is equal to the smallest integer solution that satisfies the inequality:
        // slotCount / (regionCount + delta) < maxSlotCount / maxRegionCount
        int delta =
            Math.min(
                (int) (maxRegionCount - regionCount),
                Math.max(
                    1, (int) Math.ceil(slotCount * maxRegionCount / maxSlotCount - regionCount)));
        filledStorageGroupMap.put(storageGroup, delta);
      }
    }

    doOrWaitRegionCreation(filledStorageGroupMap, consensusGroupType);
  }

  /**
   * Do Region creation for those StorageGroups who get the allocation particle, for those who
   * doesn't, waiting until other threads finished the creation process.
   *
   * @param allotmentMap Map<StorageGroup, Region allotment>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @throws NotEnoughDataNodeException When the number of online DataNodes are too small to *
   *     allocate Regions
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   * @throws TimeoutException When waiting other threads to allocate Regions for too long
   */
  private void doOrWaitRegionCreation(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, StorageGroupNotExistsException, TimeoutException {
    // StorageGroups who get the allocation particle
    Map<String, Integer> allocateMap = new HashMap<>();
    // StorageGroups who doesn't get the allocation particle
    List<String> waitingList = new ArrayList<>();
    for (String storageGroup : allotmentMap.keySet()) {
      // Try to get the allocation particle
      if (partitionInfo.contendRegionAllocationParticle(storageGroup, consensusGroupType)) {
        // Initialize one Region
        allocateMap.put(storageGroup, allotmentMap.get(storageGroup));
      } else {
        waitingList.add(storageGroup);
      }
    }

    // TODO: Use procedure to protect the following process
    // Do Region allocation and creation for those StorageGroups who get the particle
    getLoadManager().doRegionCreation(allocateMap, consensusGroupType);
    // Put back particles after that
    for (String storageGroup : allocateMap.keySet()) {
      partitionInfo.putBackRegionAllocationParticle(storageGroup, consensusGroupType);
    }

    // Waiting Region creation for those StorageGroups who don't get the particle
    waitRegionCreation(waitingList, consensusGroupType);
  }

  /** Waiting Region creation for those StorageGroups who don't get the particle */
  private void waitRegionCreation(List<String> waitingList, TConsensusGroupType consensusGroupType)
      throws TimeoutException {
    for (int retry = 0; retry < 100; retry++) {
      boolean allocationFinished = true;
      for (String storageGroup : waitingList) {
        if (!partitionInfo.getRegionAllocationParticle(storageGroup, consensusGroupType)) {
          // If a StorageGroup's Region allocation particle doesn't return,
          // the Region creation process is not complete
          allocationFinished = false;
          break;
        }
      }
      if (allocationFinished) {
        return;
      }

      try {
        // Sleep 200ms to wait Region allocation
        TimeUnit.MILLISECONDS.sleep(200);
      } catch (InterruptedException e) {
        LOGGER.warn("The PartitionManager is interrupted.", e);
      }
    }
    throw new TimeoutException("");
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
   *     SchemaPartition and matched child paths aboveMtree
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

  /**
   * Called by {@link PartitionManager#regionCleaner} Delete regions of logical deleted storage
   * groups periodically.
   */
  public void clearDeletedRegions() {
    if (getConsensusManager().isLeader()) {
      final Set<TRegionReplicaSet> deletedRegionSet = partitionInfo.getDeletedRegionSet();
      if (!deletedRegionSet.isEmpty()) {
        LOGGER.info(
            "DELETE REGIONS {} START",
            deletedRegionSet.stream()
                .map(TRegionReplicaSet::getRegionId)
                .collect(Collectors.toList()));
        SyncDataNodeClientPool.getInstance().deleteRegions(deletedRegionSet);
      }
    }
  }

  public void addMetrics() {
    partitionInfo.addMetrics();
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

  public ScheduledExecutorService getRegionCleaner() {
    return regionCleaner;
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
