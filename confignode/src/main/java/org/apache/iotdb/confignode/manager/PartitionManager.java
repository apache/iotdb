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
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.QueryDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.QuerySchemaPartitionPlan;
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

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
  }

  /**
   * TODO: Reconstruct this interface after PatterTree is moved to node-commons Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with PatternTree
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(QuerySchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaPartitionDataSet = (SchemaPartitionDataSet) consensusReadResponse.getDataset();
    return schemaPartitionDataSet;
  }

  /**
   * TODO: Reconstruct this interface after PatterTree is moved to node-commons Get SchemaPartition
   * and create a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with PatternTree
   * @return SchemaPartitionDataSet
   */
  public DataSet getOrCreateSchemaPartition(QuerySchemaPartitionPlan physicalPlan) {
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
   * @param storageGroup StorageGroupName
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

  /**
   * Get DataPartition
   *
   * @param physicalPlan DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionDataSet that contains only existing DataPartition
   */
  public DataSet getDataPartition(QueryDataPartitionPlan physicalPlan) {
    DataPartitionDataSet dataPartitionDataSet;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    dataPartitionDataSet = (DataPartitionDataSet) consensusReadResponse.getDataset();
    return dataPartitionDataSet;
  }

  /**
   * Get DataPartition and create a new one if it does not exist
   *
   * @param physicalPlan DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionDataSet
   */
  public DataSet getOrCreateDataPartition(QueryDataPartitionPlan physicalPlan) {
    Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> noAssignedDataPartitionSlots =
        partitionInfoPersistence.filterNoAssignedDataPartitionSlots(
            physicalPlan.getPartitionSlotsMap());

    if (noAssignedDataPartitionSlots.size() > 0) {
      // Allocate DataPartition
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          assignedDataPartition = allocateDataPartition(noAssignedDataPartitionSlots);
      CreateDataPartitionPlan createPlan =
          new CreateDataPartitionPlan(PhysicalPlanType.CreateDataPartition);
      createPlan.setAssignedDataPartition(assignedDataPartition);

      // Persistence DataPartition
      ConsensusWriteResponse consensusWriteResponse = getConsensusManager().write(createPlan);
      if (consensusWriteResponse.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info("Allocate data partition to {}.", assignedDataPartition);
      }
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
  private Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      allocateDataPartition(
          Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>>
              noAssignedDataPartitionSlotsMap) {

    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>> result =
        new HashMap<>();

    for (String storageGroup : noAssignedDataPartitionSlotsMap.keySet()) {
      Map<SeriesPartitionSlot, List<TimePartitionSlot>> noAssignedPartitionSlotsMap =
          noAssignedDataPartitionSlotsMap.get(storageGroup);
      List<RegionReplicaSet> dataRegionEndPoints =
          RegionInfoPersistence.getInstance().getDataRegionEndPoint(storageGroup);
      Random random = new Random();

      Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> allocateResult =
          new HashMap<>();
      for (Map.Entry<SeriesPartitionSlot, List<TimePartitionSlot>> seriesPartitionEntry :
          noAssignedPartitionSlotsMap.entrySet()) {
        allocateResult.put(seriesPartitionEntry.getKey(), new HashMap<>());
        for (TimePartitionSlot timePartitionSlot : seriesPartitionEntry.getValue()) {
          allocateResult
              .get(seriesPartitionEntry.getKey())
              .computeIfAbsent(timePartitionSlot, key -> new ArrayList<>())
              .add(dataRegionEndPoints.get(random.nextInt(dataRegionEndPoints.size())));
        }
      }

      result.put(storageGroup, allocateResult);
    }
    return result;
  }
}
