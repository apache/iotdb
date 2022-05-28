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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.client.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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

  private final Manager configManager;
  private final PartitionInfo partitionInfo;
  private static final int REGION_CLEANER_WORK_INTERVAL = 300;
  private static final int REGION_CLEANER_WORK_INITIAL_DELAY = 10;

  private SeriesPartitionExecutor executor;
  private final ScheduledExecutorService regionCleaner;

  public PartitionManager(Manager configManager, PartitionInfo partitionInfo) {
    this.configManager = configManager;
    this.partitionInfo = partitionInfo;
    this.regionCleaner =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("IoTDB-Region-Cleaner");
    regionCleaner.scheduleAtFixedRate(
        this::clearDeletedRegions,
        REGION_CLEANER_WORK_INITIAL_DELAY,
        REGION_CLEANER_WORK_INTERVAL,
        TimeUnit.SECONDS);
    setSeriesPartitionExecutor();
  }

  public ScheduledExecutorService getRegionCleaner() {
    return regionCleaner;
  }

  /**
   * Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionReq physicalPlan) {
    SchemaPartitionResp schemaPartitionResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaPartitionResp = (SchemaPartitionResp) consensusReadResponse.getDataset();
    return schemaPartitionResp;
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   */
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionReq physicalPlan) {
    Map<String, List<TSeriesPartitionSlot>> noAssignedSchemaPartitionSlots =
        partitionInfo.filterNoAssignedSchemaPartitionSlots(physicalPlan.getPartitionSlotsMap());

    if (noAssignedSchemaPartitionSlots.size() > 0) {

      // Make sure each StorageGroup has at least one SchemaRegion
      try {
        checkAndAllocateRegionsIfNecessary(
            new ArrayList<>(noAssignedSchemaPartitionSlots.keySet()),
            TConsensusGroupType.SchemaRegion);
      } catch (NotEnoughDataNodeException e) {
        SchemaPartitionResp resp = new SchemaPartitionResp();
        resp.setStatus(new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode()));
        return resp;
      }

      // Allocate SchemaPartition
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
          allocateSchemaPartition(noAssignedSchemaPartitionSlots);

      // Persist SchemaPartition
      CreateSchemaPartitionReq createPlan = new CreateSchemaPartitionReq();
      createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
      getConsensusManager().write(createPlan);

      // TODO: Allocate more Regions if necessary
    }

    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO: allocate schema partition by LoadManager
   *
   * @param noAssignedSchemaPartitionSlotsMap Map<StorageGroupName, List<SeriesPartitionSlot>>
   * @return assign result, Map<StorageGroupName, Map<SeriesPartitionSlot, RegionReplicaSet>>
   */
  private Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> noAssignedSchemaPartitionSlotsMap) {

    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> result = new HashMap<>();
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaMap =
        partitionInfo.getRegionReplicaMap();

    for (String storageGroup : noAssignedSchemaPartitionSlotsMap.keySet()) {

      List<TSeriesPartitionSlot> noAssignedPartitionSlots =
          noAssignedSchemaPartitionSlotsMap.get(storageGroup);
      // List<Pair<allocatedSlotsNum, TConsensusGroupId>>
      List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          partitionInfo.getSortedRegionSlotsCounter(
              getClusterSchemaManager()
                  .getRegionGroupIds(storageGroup, TConsensusGroupType.SchemaRegion));

      Map<TSeriesPartitionSlot, TRegionReplicaSet> allocateResult = new HashMap<>();
      for (TSeriesPartitionSlot seriesPartitionSlot : noAssignedPartitionSlots) {
        // Do greedy allocation
        Pair<Long, TConsensusGroupId> bestRegion = regionSlotsCounter.get(0);
        allocateResult.put(seriesPartitionSlot, regionReplicaMap.get(bestRegion.getRight()));

        // Bubble sort
        int index = 0;
        regionSlotsCounter.set(0, new Pair<>(bestRegion.getLeft() + 1, bestRegion.getRight()));
        while (index < regionSlotsCounter.size() - 1
            && regionSlotsCounter.get(index).getLeft()
                > regionSlotsCounter.get(index + 1).getLeft()) {
          Collections.swap(regionSlotsCounter, index, index + 1);
          index += 1;
        }
      }

      result.put(storageGroup, allocateResult);
    }

    return result;
  }

  /**
   * Get DataPartition
   *
   * @param physicalPlan DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionReq physicalPlan) {
    DataPartitionResp dataPartitionResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    dataPartitionResp = (DataPartitionResp) consensusReadResponse.getDataset();
    return dataPartitionResp;
  }

  /**
   * Get DataPartition and create a new one if it does not exist
   *
   * @param physicalPlan DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   */
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionReq physicalPlan) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> noAssignedDataPartitionSlots =
        partitionInfo.filterNoAssignedDataPartitionSlots(physicalPlan.getPartitionSlotsMap());

    if (noAssignedDataPartitionSlots.size() > 0) {

      // Make sure each StorageGroup has at least one DataRegion
      try {
        checkAndAllocateRegionsIfNecessary(
            new ArrayList<>(noAssignedDataPartitionSlots.keySet()), TConsensusGroupType.DataRegion);
      } catch (NotEnoughDataNodeException e) {
        DataPartitionResp resp = new DataPartitionResp();
        resp.setStatus(new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode()));
        return resp;
      }

      // Allocate DataPartition
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          assignedDataPartition = allocateDataPartition(noAssignedDataPartitionSlots);

      // Persist DataPartition
      CreateDataPartitionReq createPlan = new CreateDataPartitionReq();
      createPlan.setAssignedDataPartition(assignedDataPartition);
      getConsensusManager().write(createPlan);

      // TODO: Allocate more Regions if necessary
    }

    return getDataPartition(physicalPlan);
  }

  /**
   * TODO: allocate by LoadManager
   *
   * @param noAssignedDataPartitionSlotsMap Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return assign result, Map<StorageGroupName, Map<SeriesPartitionSlot, Map<TimePartitionSlot,
   *     List<RegionReplicaSet>>>>
   */
  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      allocateDataPartition(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
              noAssignedDataPartitionSlotsMap) {

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        result = new HashMap<>();
    Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaMap =
        partitionInfo.getRegionReplicaMap();

    for (String storageGroup : noAssignedDataPartitionSlotsMap.keySet()) {

      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> noAssignedPartitionSlotsMap =
          noAssignedDataPartitionSlotsMap.get(storageGroup);
      // List<Pair<allocatedSlotsNum, TConsensusGroupId>>
      List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          partitionInfo.getSortedRegionSlotsCounter(
              getClusterSchemaManager()
                  .getRegionGroupIds(storageGroup, TConsensusGroupType.DataRegion));

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> allocateResult =
          new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesPartitionEntry :
          noAssignedPartitionSlotsMap.entrySet()) {
        allocateResult.put(seriesPartitionEntry.getKey(), new HashMap<>());
        for (TTimePartitionSlot timePartitionSlot : seriesPartitionEntry.getValue()) {

          // Do greedy allocation
          Pair<Long, TConsensusGroupId> bestRegion = regionSlotsCounter.get(0);
          allocateResult
              .get(seriesPartitionEntry.getKey())
              .computeIfAbsent(timePartitionSlot, key -> new ArrayList<>())
              .add(regionReplicaMap.get(bestRegion.getRight()));

          // Bubble sort
          int index = 0;
          regionSlotsCounter.set(0, new Pair<>(bestRegion.getLeft() + 1, bestRegion.getRight()));
          while (index < regionSlotsCounter.size() - 1
              && regionSlotsCounter.get(index).getLeft()
                  > regionSlotsCounter.get(index + 1).getLeft()) {
            Collections.swap(regionSlotsCounter, index, index + 1);
            index += 1;
          }
        }
      }

      result.put(storageGroup, allocateResult);
    }
    return result;
  }

  private void checkAndAllocateRegionsIfNecessary(
      List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException {
    List<String> storageGroupWithoutRegion = new ArrayList<>();
    for (String storageGroup : storageGroups) {
      List<TConsensusGroupId> groupIds =
          getClusterSchemaManager().getRegionGroupIds(storageGroup, consensusGroupType);
      if (groupIds.size() == 0) {
        storageGroupWithoutRegion.add(storageGroup);
      }
    }
    getLoadManager().initializeRegions(storageGroupWithoutRegion, consensusGroupType);
  }

  /** Get all allocated RegionReplicaSets */
  public List<TRegionReplicaSet> getAllocatedRegions() {
    return partitionInfo.getAllocatedRegions();
  }

  /** Construct SeriesPartitionExecutor by iotdb-confignode.propertis */
  private void setSeriesPartitionExecutor() {
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
    this.executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            conf.getSeriesPartitionExecutorClass(), conf.getSeriesPartitionSlotNum());
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

  /**
   * Only leader use this interface.
   *
   * @return the next RegionGroupId
   */
  public int generateNextRegionGroupId() {
    return partitionInfo.generateNextRegionGroupId();
  }

  /**
   * Only leader use this interface.
   *
   * @param groupIds List<TConsensusGroupId>
   * @return RegionReplicaSet by the specific TConsensusGroupIds
   */
  public List<TRegionReplicaSet> getRegionReplicaSets(List<TConsensusGroupId> groupIds) {
    return partitionInfo.getRegionReplicaSets(groupIds);
  }

  /**
   * GetNodePathsPartition
   *
   * @param physicalPlan GetNodesPathsPartitionReq
   * @return SchemaNodeManagementPartitionDataSet that contains only existing matched
   *     SchemaPartition and matched child paths aboveMtree
   */
  public DataSet getNodePathsPartition(GetNodePathsPartitionReq physicalPlan) {
    SchemaNodeManagementResp schemaNodeManagementResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaNodeManagementResp = (SchemaNodeManagementResp) consensusReadResponse.getDataset();
    return schemaNodeManagementResp;
  }

  public void preDeleteStorageGroup(
      String storageGroup, PreDeleteStorageGroupReq.PreDeleteType preDeleteType) {
    final PreDeleteStorageGroupReq preDeleteStorageGroupReq =
        new PreDeleteStorageGroupReq(storageGroup, preDeleteType);
    getConsensusManager().write(preDeleteStorageGroupReq);
  }

  /**
   * Called by {@link PartitionManager#regionCleaner} Delete regions of logical deleted storage
   * groups periodically.
   */
  private void clearDeletedRegions() {
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

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
