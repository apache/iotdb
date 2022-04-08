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

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.crud.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.SchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** manage data partition and schema partition */
public class PartitionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  private static final PartitionInfoPersistence partitionInfoPersistence =
      PartitionInfoPersistence.getInstance();

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
    List<Integer> seriesPartitionSlots = physicalPlan.getSeriesPartitionSlots();
    List<Integer> noAssignedSeriesPartitionSlots =
        partitionInfoPersistence.filterSchemaRegionNoAssignedPartitionSlots(
            storageGroup, seriesPartitionSlots);

    if (noAssignedSeriesPartitionSlots.size() > 0) {
      // allocate partition by storage group and device group id
      Map<Integer, RegionReplicaSet> schemaPartitionReplicaSets =
          allocateSchemaPartition(storageGroup, noAssignedSeriesPartitionSlots);
      physicalPlan.setSchemaPartitionReplicaSet(schemaPartitionReplicaSets);

      ConsensusWriteResponse consensusWriteResponse = getConsensusManager().write(physicalPlan);
      if (consensusWriteResponse.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info("Allocate schema partition to {}.", schemaPartitionReplicaSets);
      }
    }

    physicalPlan.setSchemaPartitionReplicaSet(new HashMap<>());
    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO: allocate schema partition by LoadManager
   *
   * @param storageGroup storage group
   * @param noAssignedSeriesPartitionSlots not assigned SeriesPartitionSlots
   * @return assign result
   */
  private Map<Integer, RegionReplicaSet> allocateSchemaPartition(
      String storageGroup, List<Integer> noAssignedSeriesPartitionSlots) {
    List<RegionReplicaSet> schemaRegionEndPoints =
        RegionInfoPersistence.getInstance().getSchemaRegionEndPoint(storageGroup);
    Random random = new Random();
    Map<Integer, RegionReplicaSet> schemaPartitionReplicaSets = new HashMap<>();
    for (Integer seriesPartitionSlot : noAssignedSeriesPartitionSlots) {
      RegionReplicaSet schemaRegionReplicaSet =
          schemaRegionEndPoints.get(random.nextInt(schemaRegionEndPoints.size()));
      schemaPartitionReplicaSets.put(seriesPartitionSlot, schemaRegionReplicaSet);
    }
    return schemaPartitionReplicaSets;
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
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

  /**
   * Get DataPartition and apply a new one if it does not exist
   *
   * @param physicalPlan DataPartitionPlan with StorageGroup, DeviceGroupIds and StartTimes
   * @return DataPartitionDataSet
   */
  public DataSet applyDataPartition(DataPartitionPlan physicalPlan) {

    String storageGroup = physicalPlan.getStorageGroup();
    Map<Integer, List<Long>> seriesPartitionSlots =
        physicalPlan.getSeriesPartitionTimePartitionSlots();
    Map<Integer, List<Long>> noAssignedSeriesPartitionSlots =
        partitionInfoPersistence.filterDataRegionNoAssignedPartitionSlots(
            storageGroup, seriesPartitionSlots);

    if (noAssignedSeriesPartitionSlots.size() > 0) {
      // allocate partition by storage group and device group id
      Map<Integer, Map<Long, List<RegionReplicaSet>>> dataPartitionReplicaSets =
          allocateDataPartition(storageGroup, noAssignedSeriesPartitionSlots);
      physicalPlan.setDataPartitionReplicaSets(dataPartitionReplicaSets);

      ConsensusWriteResponse consensusWriteResponse = getConsensusManager().write(physicalPlan);
      if (consensusWriteResponse.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info("Allocate data partition to {}.", dataPartitionReplicaSets);
      }
    }

    physicalPlan.setDataPartitionReplicaSets(new HashMap<>());
    return getDataPartition(physicalPlan);
  }

  /**
   * TODO: allocate by LoadManager
   *
   * @param storageGroup storage group name
   * @param noAssignedSeriesPartitionSlots not assigned DataPartitionSlots
   * @return assign result
   */
  private Map<Integer, Map<Long, List<RegionReplicaSet>>> allocateDataPartition(
      String storageGroup, Map<Integer, List<Long>> noAssignedSeriesPartitionSlots) {
    List<RegionReplicaSet> dataRegionEndPoints =
        RegionInfoPersistence.getInstance().getDataRegionEndPoint(storageGroup);
    Random random = new Random();
    Map<Integer, Map<Long, List<RegionReplicaSet>>> dataPartitionReplicaSets = new HashMap<>();
    for (Map.Entry<Integer, List<Long>> seriesPartitionEntry :
        noAssignedSeriesPartitionSlots.entrySet()) {
      dataPartitionReplicaSets.put(seriesPartitionEntry.getKey(), new HashMap<>());
      for (long timePartitionSlot : seriesPartitionEntry.getValue()) {
        dataPartitionReplicaSets
            .get(seriesPartitionEntry.getKey())
            .computeIfAbsent(timePartitionSlot, key -> new ArrayList<>())
            .add(dataRegionEndPoints.get(random.nextInt(dataRegionEndPoints.size())));
      }
    }
    return dataPartitionReplicaSets;
  }
}
