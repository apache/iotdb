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

import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.partition.DataPartitionInfo;
import org.apache.iotdb.confignode.partition.SchemaRegionReplicaSet;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.sys.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.SchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** manage data partition and schema partition */
public class PartitionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  /** schema partition read write lock */
  private final ReentrantReadWriteLock schemaPartitionReadWriteLock;

  /** data partition read write lock */
  private final ReentrantReadWriteLock dataPartitionReadWriteLock;

  // TODO: Serialize and Deserialize
  private final DataPartitionInfo dataPartition;

  private final Manager configNodeManager;

  public PartitionManager(Manager configNodeManager) {
    this.schemaPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.dataPartitionReadWriteLock = new ReentrantReadWriteLock();
    this.configNodeManager = configNodeManager;
    this.dataPartition = new DataPartitionInfo();
  }

  /**
   * Get schema partition
   *
   * @param physicalPlan storageGroup and deviceGroupIDs
   * @return Empty Data Set if does not exist
   */
  public DataSet getSchemaPartition(SchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet;
    schemaPartitionReadWriteLock.readLock().lock();
    try {
      ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
      schemaPartitionDataSet = (SchemaPartitionDataSet) consensusReadResponse.getDataset();
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
    String storageGroup = physicalPlan.getStorageGroup();
    List<Integer> deviceGroupIDs = physicalPlan.getDeviceGroupIDs();
    List<Integer> noAssignDeviceGroupId =
        PartitionInfoPersistence.getInstance()
            .filterSchemaRegionNoAssignDeviceGroupId(storageGroup, deviceGroupIDs);

    // allocate partition by storage group and device group id
    schemaPartitionReadWriteLock.writeLock().lock();
    try {
      Map<Integer, SchemaRegionReplicaSet> deviceGroupIdReplicaSets =
          allocateSchemaPartition(storageGroup, noAssignDeviceGroupId);
      physicalPlan.setDeviceGroupIdReplicaSet(deviceGroupIdReplicaSets);
      getConsensusManager().write(physicalPlan);
      LOGGER.info("Allocate schema partition to {}.", deviceGroupIdReplicaSets);
    } finally {
      schemaPartitionReadWriteLock.writeLock().unlock();
    }

    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO: allocate schema partition by balancer
   *
   * @param storageGroup storage group
   * @param deviceGroupIDs device group id list
   */
  private Map<Integer, SchemaRegionReplicaSet> allocateSchemaPartition(
      String storageGroup, List<Integer> deviceGroupIDs) {
    List<SchemaRegionReplicaSet> schemaRegionEndPoints =
        RegionInfoPersistence.getInstance().getSchemaRegionEndPoint();
    Random random = new Random();
    Map<Integer, SchemaRegionReplicaSet> deviceGroupIdReplicaSets = new HashMap<>();
    for (int i = 0; i < deviceGroupIDs.size(); i++) {
      SchemaRegionReplicaSet schemaRegionReplicaSet =
          schemaRegionEndPoints.get(random.nextInt(schemaRegionEndPoints.size()));
      deviceGroupIdReplicaSets.put(deviceGroupIDs.get(i), schemaRegionReplicaSet);
    }
    return deviceGroupIdReplicaSets;
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
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
}
