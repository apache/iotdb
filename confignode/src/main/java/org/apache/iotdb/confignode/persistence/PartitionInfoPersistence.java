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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

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
   * Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetOrCreateSchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet = new SchemaPartitionDataSet();
    schemaPartitionReadWriteLock.readLock().lock();

    try {
      schemaPartitionDataSet.setSchemaPartition(
          schemaPartition.getSchemaPartition(physicalPlan.getPartitionSlotsMap()));
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
      schemaPartitionDataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }

    return schemaPartitionDataSet;
  }

  /**
   * Create SchemaPartition
   *
   * @param physicalPlan CreateSchemaPartitionPlan with SchemaPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
  public TSStatus createSchemaPartition(CreateSchemaPartitionPlan physicalPlan) {
    schemaPartitionReadWriteLock.writeLock().lock();

    try {
      // Allocate SchemaPartition by CreateSchemaPartitionPlan
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> assignedResult =
          physicalPlan.getAssignedSchemaPartition();
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

  public Map<String, List<SeriesPartitionSlot>> filterNoAssignedSchemaPartitionSlots(
      Map<String, List<SeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<SeriesPartitionSlot>> result;
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
   * @param physicalPlan DataPartitionPlan with partitionSlotsMap
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

  /**
   * Create DataPartition
   *
   * @param physicalPlan CreateDataPartitionPlan with DataPartition assigned result
   * @return TSStatusCode.SUCCESS_STATUS when creation successful
   */
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
