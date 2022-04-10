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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class PartitionInfoPersistence {

  /** schema partition read write lock */
  private final ReentrantReadWriteLock schemaPartitionReadWriteLock;

  /** data partition read write lock */
  private final ReentrantReadWriteLock dataPartitionReadWriteLock;

  // TODO: Serialize and Deserialize
  private final SchemaPartition schemaPartition;

  // TODO: Serialize and Deserialize
  private final DataPartition dataPartition;

  public PartitionInfoPersistence() {
    this.schemaPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.dataPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.schemaPartition = new SchemaPartition();
    this.schemaPartition.setSchemaPartitionMap(new HashMap<>());
    this.dataPartition = new DataPartition();
    this.dataPartition.setDataPartitionMap(new HashMap<>());
  }

  /**
   * TODO: Reconstruct this interface after PatterTree is moved to node-commons Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with PatternTree
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetOrCreateSchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet = new SchemaPartitionDataSet();
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      String storageGroup = physicalPlan.getStorageGroup();
      List<Integer> deviceGroupIDs = physicalPlan.getSeriesPartitionSlots();
      SchemaPartition schemaPartitionInfo = new SchemaPartition();
      schemaPartitionInfo.setSchemaPartitionMap(
          schemaPartition.getSchemaPartition(storageGroup, deviceGroupIDs));
      schemaPartitionDataSet.setSchemaPartition(schemaPartitionInfo);
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
      schemaPartitionDataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return schemaPartitionDataSet;
  }

  /**
   * TODO: Reconstruct this interface after PatterTree is moved to node-commons Get SchemaPartition
   * and create a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with PatternTree
   * @return SchemaPartitionDataSet
   */
  public TSStatus createSchemaPartition(GetOrCreateSchemaPartitionPlan physicalPlan) {
    schemaPartitionReadWriteLock.writeLock().lock();

    try {
      // Allocate SchemaPartition by SchemaPartitionPlan
      String storageGroup = physicalPlan.getStorageGroup();
      Map<Integer, RegionReplicaSet> schemaPartitionReplicaSets =
          physicalPlan.getSchemaPartitionReplicaSets();
      schemaPartitionReplicaSets.forEach(
          (key, value) -> schemaPartition.setSchemaRegionReplicaSet(storageGroup, key, value));
    } finally {
      schemaPartitionReadWriteLock.writeLock().unlock();
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /** TODO: Reconstruct this interface after PatterTree is moved to node-commons */
  public List<Integer> filterSchemaRegionNoAssignedPartitionSlots(
      String storageGroup, List<Integer> seriesPartitionSlots) {
    List<Integer> result;
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      result =
          schemaPartition.filterNoAssignedSeriesPartitionSlot(storageGroup, seriesPartitionSlots);
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
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
  public DataSet getDataPartition(GetOrCreateDataPartitionPlan physicalPlan) {
    DataPartitionDataSet dataPartitionDataSet = new DataPartitionDataSet();
    dataPartitionReadWriteLock.readLock().lock();
    try {
      dataPartitionDataSet.setDataPartition(
          dataPartition.getDataPartition(physicalPlan.getPartitionSlotsMap()));
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
      dataPartitionDataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return dataPartitionDataSet;
  }

  public TSStatus createDataPartition(CreateDataPartitionPlan physicalPlan) {
    dataPartitionReadWriteLock.writeLock().lock();

    try {
      // Allocate DataPartition by CreateDataPartitionPlan
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          assignedResult = physicalPlan.getAssignedDataPartition();
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

  public Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>>
      filterNoAssignedDataPartitionSlots(
          Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> result;
    dataPartitionReadWriteLock.readLock().lock();
    try {
      result = dataPartition.filterNoAssignedDataPartitionSlots(partitionSlotsMap);
    } finally {
      dataPartitionReadWriteLock.readLock().unlock();
    }
    return result;
  }

  @TestOnly
  public void clear() {
    if (schemaPartition.getSchemaPartitionMap() != null) {
      schemaPartition.getSchemaPartitionMap().clear();
    }

    if (dataPartition.getDataPartitionMap() != null) {
      dataPartition.getDataPartitionMap().clear();
    }
  }

  private static class PartitionInfoPersistenceHolder {

    private static final PartitionInfoPersistence INSTANCE = new PartitionInfoPersistence();

    private PartitionInfoPersistenceHolder() {
      // empty constructor
    }
  }

  public static PartitionInfoPersistence getInstance() {
    return PartitionInfoPersistence.PartitionInfoPersistenceHolder.INSTANCE;
  }
}
