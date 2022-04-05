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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.physical.crud.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.SchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class PartitionInfoPersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionInfoPersistence.class);

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
    this.dataPartition = new DataPartition();
  }

  /**
   * Get schema partition
   *
   * @param physicalPlan storageGroup and deviceGroupIDs
   * @return Empty Data Set if does not exist
   */
  public DataSet getSchemaPartition(SchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet = new SchemaPartitionDataSet();
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      String storageGroup = physicalPlan.getStorageGroup();
      List<Integer> deviceGroupIDs = physicalPlan.getDeviceGroupIDs();
      SchemaPartition schemaPartitionInfo = new SchemaPartition();
      schemaPartitionInfo.setSchemaPartitionMap(
          schemaPartition.getSchemaPartition(storageGroup, deviceGroupIDs));
      schemaPartitionDataSet.setSchemaPartition(schemaPartitionInfo);
    } finally {
      schemaPartitionReadWriteLock.readLock().unlock();
    }
    return schemaPartitionDataSet;
  }

  /**
   * If does not exist, apply a new schema partition
   *
   * @param physicalPlan storage group and device group id
   * @return Schema Partition data set
   */
  public DataSet applySchemaPartition(SchemaPartitionPlan physicalPlan) {
    schemaPartitionReadWriteLock.writeLock().lock();

    String storageGroup = physicalPlan.getStorageGroup();
    List<Integer> deviceGroupIDs = physicalPlan.getDeviceGroupIDs();
    List<Integer> noAssignDeviceGroupId =
        schemaPartition.filterNoAssignDeviceGroupId(storageGroup, deviceGroupIDs);

    // allocate partition by storage group and device group id
    Map<Integer, RegionReplicaSet> deviceGroupIdReplicaSets =
        physicalPlan.getDeviceGroupIdReplicaSets();
    try {

      deviceGroupIdReplicaSets
              .forEach((key, value) -> schemaPartition.setSchemaRegionReplicaSet(
                      storageGroup, key, value));
    } finally {
      schemaPartitionReadWriteLock.writeLock().unlock();
    }

    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO:allocate schema partition by balancer
   *
   * @param physicalPlan physical plan
   * @return data set
   */
  public DataSet applyDataPartition(DataPartitionPlan physicalPlan) {
    return null;
  }

  public DataSet getDataPartition(DataPartitionPlan physicalPlan) {
    return null;
  }

  public List<Integer> filterSchemaRegionNoAssignDeviceGroupId(
      String storageGroup, List<Integer> deviceGroupIDs) {
    return schemaPartition.filterNoAssignDeviceGroupId(storageGroup, deviceGroupIDs);
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
