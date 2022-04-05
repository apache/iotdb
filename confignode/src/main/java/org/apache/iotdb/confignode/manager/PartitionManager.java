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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.crud.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.SchemaPartitionPlan;
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

  private final Manager configNodeManager;

  public PartitionManager(Manager configNodeManager) {
    this.configNodeManager = configNodeManager;
  }

  /**
   * Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with StorageGroup and DeviceGroupIds
   * @return Empty DataSet if the specific SchemaPartition does not exist
   */
  public DataSet getSchemaPartition(SchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaPartitionDataSet = (SchemaPartitionDataSet) consensusReadResponse.getDataset();
    return schemaPartitionDataSet;
  }

  /**
   * Get SchemaPartition and apply a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with StorageGroup and DeviceGroupIds
   * @return SchemaPartitionDataSet
   */
  public DataSet applySchemaPartition(SchemaPartitionPlan physicalPlan) {
    String storageGroup = physicalPlan.getStorageGroup();
    List<Integer> deviceGroupIDs = physicalPlan.getDeviceGroupIDs();
    List<Integer> noAssignDeviceGroupId =
        PartitionInfoPersistence.getInstance()
            .filterSchemaRegionNoAssignDeviceGroupId(storageGroup, deviceGroupIDs);

    if (noAssignDeviceGroupId.size() == 0 && !getConsensusManager().isLeader()) {
      // Reject apply if there exists not assigned SchemaPartition and the local ConfigNode is not the leader
      // TODO: Tell DataNode to re-transmit
      return new SchemaPartitionDataSet();
    }

    // allocate partition by storage group and device group id
      Map<Integer, RegionReplicaSet> deviceGroupIdReplicaSets =
          allocateSchemaPartition(storageGroup, noAssignDeviceGroupId);
      physicalPlan.setDeviceGroupIdReplicaSet(deviceGroupIdReplicaSets);
      getConsensusManager().write(physicalPlan);
      LOGGER.info("Allocate schema partition to {}.", deviceGroupIdReplicaSets);

    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO: allocate schema partition by balancer
   *
   * @param storageGroup storage group
   * @param deviceGroupIDs device group id list
   */
  private Map<Integer, RegionReplicaSet> allocateSchemaPartition(
      String storageGroup, List<Integer> deviceGroupIDs) {
    List<RegionReplicaSet> schemaRegionEndPoints =
        RegionInfoPersistence.getInstance().getSchemaRegionEndPoint();
    Random random = new Random();
    Map<Integer, RegionReplicaSet> deviceGroupIdReplicaSets = new HashMap<>();
    for (Integer deviceGroupID : deviceGroupIDs) {
      RegionReplicaSet schemaRegionReplicaSet =
              schemaRegionEndPoints.get(random.nextInt(schemaRegionEndPoints.size()));
      deviceGroupIdReplicaSets.put(deviceGroupID, schemaRegionReplicaSet);
    }
    return deviceGroupIdReplicaSets;
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
  }

  /**
   * Get DataPartition and apply a new one if it does not exist
   *
   * @param physicalPlan DataPartitionPlan with StorageGroup, DeviceGroupIds and StartTimes
   * @return DataPartitionDataSet
   */
  public DataSet applyDataPartition(DataPartitionPlan physicalPlan) {
    return null;
  }

  /**
   * Get DataPartition
   *
   * @param physicalPlan DataPartitionPlan with StorageGroup, DeviceGroupIds and StartTimes
   * @return Empty DataSet if the specific DataPartition does not exist
   */
  public DataSet getDataPartition(DataPartitionPlan physicalPlan) {
    DataPartitionDataSet dataPartitionDataSet;
      ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
      dataPartitionDataSet = (DataPartitionDataSet) consensusReadResponse.getDataset();

    return dataPartitionDataSet;
  }
}
