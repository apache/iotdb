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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class PartitionInfo {

  // Region read write lock
  private final ReentrantReadWriteLock regionReadWriteLock;
  // TODO: Serialize and Deserialize
  private AtomicInteger nextRegionGroupId = new AtomicInteger(0);
  // TODO: Serialize and Deserialize
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionMap;

  // SchemaPartition read write lock
  private final ReentrantReadWriteLock schemaPartitionReadWriteLock;
  // TODO: Serialize and Deserialize
  private final SchemaPartition schemaPartition;

  // DataPartition read write lock
  private final ReentrantReadWriteLock dataPartitionReadWriteLock;
  // TODO: Serialize and Deserialize
  private final DataPartition dataPartition;

  private PartitionInfo() {
    this.regionReadWriteLock = new ReentrantReadWriteLock();
    this.regionMap = new HashMap<>();

    this.schemaPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.schemaPartition =
        new SchemaPartition(
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    this.schemaPartition.setSchemaPartitionMap(new HashMap<>());

    this.dataPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.dataPartition =
        new DataPartition(
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
            ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    this.dataPartition.setDataPartitionMap(new HashMap<>());
  }

  public int generateNextRegionGroupId() {
    return nextRegionGroupId.getAndIncrement();
  }

  /**
   * Persistence allocation result of new Regions
   *
   * @param req CreateRegionsPlan
   * @return SUCCESS_STATUS
   */
  public TSStatus createRegions(CreateRegionsReq req) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      int maxRegionId = Integer.MIN_VALUE;

      for (TRegionReplicaSet regionReplicaSet : req.getRegionReplicaSets()) {
        regionMap.put(regionReplicaSet.getRegionId(), regionReplicaSet);
        maxRegionId = Math.max(maxRegionId, regionReplicaSet.getRegionId().getId());
      }

      if (nextRegionGroupId.get() < maxRegionId) {
        // In this case, at least one Region is created with the leader node,
        // so the nextRegionGroupID of the followers needs to be added
        nextRegionGroupId.getAndAdd(req.getRegionReplicaSets().size());
      }

      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Delete Regions
   *
   * @param req DeleteRegionsReq
   * @return SUCCESS_STATUS
   */
  public TSStatus deleteRegions(DeleteRegionsReq req) {
    TSStatus result;
    regionReadWriteLock.writeLock().lock();
    try {
      for (TConsensusGroupId consensusGroupId : req.getConsensusGroupIds()) {
        regionMap.remove(consensusGroupId);
      }
      result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      regionReadWriteLock.writeLock().unlock();
    }
    return result;
  }

  /**
   * Get SchemaPartition
   *
   * @param req SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetSchemaPartitionReq req) {
    SchemaPartitionResp schemaPartitionResp = new SchemaPartitionResp();
    schemaPartitionReadWriteLock.readLock().lock();

    try {
      schemaPartitionResp.setSchemaPartition(
          schemaPartition.getSchemaPartition(req.getPartitionSlotsMap()));
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
      schemaPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }

    return schemaPartitionResp;
  }

  /**
   * Create SchemaPartition
   *
   * @param req CreateSchemaPartitionPlan with SchemaPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
  public TSStatus createSchemaPartition(CreateSchemaPartitionReq req) {
    schemaPartitionReadWriteLock.writeLock().lock();

    try {
      // Allocate SchemaPartition by CreateSchemaPartitionPlan
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedResult =
          req.getAssignedSchemaPartition();
      assignedResult.forEach(
          (storageGroup, partitionSlots) ->
              partitionSlots.forEach(
                  (seriesPartitionSlot, regionReplicaSet) ->
                      schemaPartition.createSchemaPartition(
                          storageGroup, seriesPartitionSlot, regionReplicaSet)));
    } finally {
      schemaPartitionReadWriteLock.writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Filter no assigned SchemaPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, List<TSeriesPartitionSlot>>
   * @return Map<StorageGroupName, List<TSeriesPartitionSlot>>, SchemaPartitionSlots that is not
   *     assigned in partitionSlotsMap
   */
  public Map<String, List<TSeriesPartitionSlot>> filterNoAssignedSchemaPartitionSlots(
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<TSeriesPartitionSlot>> result;
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      result = schemaPartition.filterNoAssignedSchemaPartitionSlot(partitionSlotsMap);
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get DataPartition
   *
   * @param req DataPartitionPlan with partitionSlotsMap
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(GetDataPartitionReq req) {
    DataPartitionResp dataPartitionResp = new DataPartitionResp();
    dataPartitionReadWriteLock.readLock().lock();

    try {
      dataPartitionResp.setDataPartition(
          dataPartition.getDataPartition(
              req.getPartitionSlotsMap(),
              ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
              ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum()));
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
      dataPartitionResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }

    return dataPartitionResp;
  }

  /**
   * Create DataPartition
   *
   * @param req CreateDataPartitionPlan with DataPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
  public TSStatus createDataPartition(CreateDataPartitionReq req) {
    dataPartitionReadWriteLock.writeLock().lock();

    try {
      // Allocate DataPartition by CreateDataPartitionPlan
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          assignedResult = req.getAssignedDataPartition();
      assignedResult.forEach(
          (storageGroup, seriesPartitionTimePartitionSlots) ->
              seriesPartitionTimePartitionSlots.forEach(
                  ((seriesPartitionSlot, timePartitionSlotRegionReplicaSets) ->
                      timePartitionSlotRegionReplicaSets.forEach(
                          ((timePartitionSlot, regionReplicaSets) ->
                              regionReplicaSets.forEach(
                                  regionReplicaSet ->
                                      dataPartition.createDataPartition(
                                          storageGroup,
                                          seriesPartitionSlot,
                                          timePartitionSlot,
                                          regionReplicaSet)))))));
    } finally {
      dataPartitionReadWriteLock.writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /**
   * Filter no assigned DataPartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<TSeriesPartitionSlot,
   *     List<TTimePartitionSlot>>>
   * @return Map<StorageGroupName, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>,
   *     DataPartitionSlots that is not assigned in partitionSlotsMap
   */
  public Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      filterNoAssignedDataPartitionSlots(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result;
    dataPartitionReadWriteLock.readLock().lock();
    try {
      result = dataPartition.filterNoAssignedDataPartitionSlots(partitionSlotsMap);
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  /** Get RegionReplicaSet by the specific TConsensusGroupIds */
  public List<TRegionReplicaSet> getRegionReplicaSets(List<TConsensusGroupId> groupIds) {
    List<TRegionReplicaSet> result = new ArrayList<>();
    regionReadWriteLock.readLock().lock();
    try {
      for (TConsensusGroupId groupId : groupIds) {
        result.add(regionMap.get(groupId));
      }
    } finally {
      regionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  public void serialize(ByteBuffer buffer) {
    // TODO: Serialize PartitionInfo
  }

  public void deserialize(ByteBuffer buffer) {
    // TODO: Deserialize PartitionInfo
  }

  @TestOnly
  public void clear() {
    nextRegionGroupId = new AtomicInteger(0);
    regionMap.clear();

    if (schemaPartition.getSchemaPartitionMap() != null) {
      schemaPartition.getSchemaPartitionMap().clear();
    }

    if (dataPartition.getDataPartitionMap() != null) {
      dataPartition.getDataPartitionMap().clear();
    }
  }

  private static class PartitionInfoHolder {

    private static final PartitionInfo INSTANCE = new PartitionInfo();

    private PartitionInfoHolder() {
      // empty constructor
    }
  }

  public static PartitionInfo getInstance() {
    return PartitionInfoHolder.INSTANCE;
  }
}
