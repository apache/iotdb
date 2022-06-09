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
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.MetadataException;
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
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
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
  public DataSet getSchemaPartition(GetSchemaPartitionReq req) {
    return getConsensusManager().read(req).getDataset();
  }

  /**
   * Thread-safely get DataPartition
   *
   * @param req DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionReq req) {
    return getConsensusManager().read(req).getDataset();
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionResp with DataPartition and TSStatus. SUCCESS_STATUS if all process
   *     finish. NOT_ENOUGH_DATA_NODE if the DataNodes is not enough to create new Regions.
   */
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionReq req) {
    Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlots =
        partitionInfo.filterUnassignedSchemaPartitionSlots(req.getPartitionSlotsMap());

    if (unassignedSchemaPartitionSlots.size() > 0) {

      // Make sure each StorageGroup has at least one SchemaRegion
      try {
        checkAndAllocateRegionsIfNecessary(
            new ArrayList<>(unassignedSchemaPartitionSlots.keySet()),
            TConsensusGroupType.SchemaRegion);
      } catch (NotEnoughDataNodeException e) {
        SchemaPartitionResp resp = new SchemaPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because there are not enough DataNodes"));
        return resp;
      } catch (TimeoutException e) {
        SchemaPartitionResp resp = new SchemaPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.TIME_OUT.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because waiting for another thread's Region allocation timeout."));
        return resp;
      } catch (StorageGroupNotExistsException e) {
        SchemaPartitionResp resp = new SchemaPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because some StorageGroup doesn't exist."));
        return resp;
      }

      // Allocate SchemaPartition
      Map<String, SchemaPartitionTable> assignedSchemaPartition =
          getLoadManager().allocateSchemaPartition(unassignedSchemaPartitionSlots);

      // Persist SchemaPartition
      CreateSchemaPartitionReq createPlan = new CreateSchemaPartitionReq();
      createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
      getConsensusManager().write(createPlan);

      // TODO: Allocate more Regions if necessary
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
   */
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionReq req) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> unassignedDataPartitionSlots =
        partitionInfo.filterUnassignedDataPartitionSlots(req.getPartitionSlotsMap());

    if (unassignedDataPartitionSlots.size() > 0) {

      // Make sure each StorageGroup has at least one DataRegion
      try {
        checkAndAllocateRegionsIfNecessary(
            new ArrayList<>(unassignedDataPartitionSlots.keySet()), TConsensusGroupType.DataRegion);
      } catch (NotEnoughDataNodeException e) {
        DataPartitionResp resp = new DataPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because there are not enough DataNodes"));
        return resp;
      } catch (TimeoutException e) {
        DataPartitionResp resp = new DataPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.TIME_OUT.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because waiting for another thread's Region allocation timeout."));
        return resp;
      } catch (StorageGroupNotExistsException e) {
        SchemaPartitionResp resp = new SchemaPartitionResp();
        resp.setStatus(
            new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
                .setMessage(
                    "ConfigNode failed to allocate DataPartition because some StorageGroup doesn't exist."));
        return resp;
      }

      // Allocate DataPartition
      Map<String, DataPartitionTable> assignedDataPartition =
          getLoadManager().allocateDataPartition(unassignedDataPartitionSlots);

      // Persist DataPartition
      CreateDataPartitionReq createPlan = new CreateDataPartitionReq();
      createPlan.setAssignedDataPartition(assignedDataPartition);
      getConsensusManager().write(createPlan);

      // TODO: Allocate more Regions if necessary
    }

    return getDataPartition(req);
  }

  /**
   * Check whether each StorageGroup already has a Region of the specified type, and allocate one
   * Region for each StorageGroup who doesn't have any.
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType SchemaRegion or DataRegion
   * @throws NotEnoughDataNodeException When the number of online DataNodes are too small to
   *     allocate Regions
   * @throws TimeoutException When waiting other threads to allocate Regions for too long
   * @throws StorageGroupNotExistsException When some StorageGroups don't exist
   */
  private void checkAndAllocateRegionsIfNecessary(
      List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, TimeoutException, StorageGroupNotExistsException {

    int leastDataNode = 0;
    List<String> unreadyStorageGroups = new ArrayList<>();
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
        unreadyStorageGroups.add(storageGroup);
      }
    }
    if (getNodeManager().getOnlineDataNodeCount() < leastDataNode) {
      // Make sure DataNodes enough
      throw new NotEnoughDataNodeException();
    }

    List<String> storageGroupsNeedAllocation = new ArrayList<>();
    List<String> storageGroupsInAllocation = new ArrayList<>();
    for (String storageGroup : unreadyStorageGroups) {
      // Try to get the allocation particle
      if (partitionInfo.getRegionAllocationParticle(storageGroup, consensusGroupType)) {
        storageGroupsNeedAllocation.add(storageGroup);
      } else {
        storageGroupsInAllocation.add(storageGroup);
      }
    }

    // Calculate the number of Regions
    // TODO: This is a temporary code, delete it later
    int regionNum;
    if (consensusGroupType == TConsensusGroupType.SchemaRegion) {
      regionNum =
          Math.max(
              1,
              getNodeManager().getOnlineDataNodeCount()
                  / ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor());
    } else {
      regionNum =
          Math.max(
              2,
              (int)
                  (getNodeManager().getTotalCpuCoreCount()
                      * 0.3
                      / ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor()));
    }

    // Do Region allocation for those StorageGroups who get the particle
    // TODO: Use Procedure to put back Region allocation particles when creation process failed.
    getLoadManager().initializeRegions(storageGroupsNeedAllocation, consensusGroupType, regionNum);
    // TODO: Put back particles after creation

    // Waiting allocation for those StorageGroups who don't get the particle
    for (int retry = 0; retry < 10; retry++) {
      boolean allocationFinished = true;
      for (String storageGroup : storageGroupsInAllocation) {
        if (getRegionCount(storageGroup, consensusGroupType) == 0) {
          allocationFinished = false;
          break;
        }
      }
      if (allocationFinished) {
        return;
      }

      try {
        // Sleep 1s to wait Region allocation
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.warn("The PartitionManager is interrupted.", e);
      }
    }
    throw new TimeoutException("");
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

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

  /**
   * Get TSeriesPartitionSlot
   *
   * @param devicePath Full path ending with device name
   * @return SeriesPartitionSlot
   */
  public TSeriesPartitionSlot getSeriesPartitionSlot(String devicePath) {
    return executor.getSeriesPartitionSlot(devicePath);
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
