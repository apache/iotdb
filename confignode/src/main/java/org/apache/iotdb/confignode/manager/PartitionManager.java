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
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

  private SeriesPartitionExecutor executor;

  public PartitionManager(Manager configNodeManager) {
    this.configNodeManager = configNodeManager;
    setSeriesPartitionExecutor();
  }

  private ConsensusManager getConsensusManager() {
    return configNodeManager.getConsensusManager();
  }

  /**
   * Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet that contains only existing SchemaPartition
   */
  public DataSet getSchemaPartition(GetOrCreateSchemaPartitionPlan physicalPlan) {
    SchemaPartitionDataSet schemaPartitionDataSet;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaPartitionDataSet = (SchemaPartitionDataSet) consensusReadResponse.getDataset();
    return schemaPartitionDataSet;
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet
   */
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionPlan physicalPlan) {
    Map<String, List<SeriesPartitionSlot>> noAssignedSchemaPartitionSlots =
        partitionInfoPersistence.filterNoAssignedSchemaPartitionSlots(
            physicalPlan.getPartitionSlotsMap());

    if (noAssignedSchemaPartitionSlots.size() > 0) {
      // Allocate SchemaPartition
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> assignedSchemaPartition =
          allocateSchemaPartition(noAssignedSchemaPartitionSlots);

      // Persist SchemaPartition
      CreateSchemaPartitionPlan createPlan = new CreateSchemaPartitionPlan();
      createPlan.setAssignedSchemaPartition(assignedSchemaPartition);
      getConsensusManager().write(createPlan);
    }

    return getSchemaPartition(physicalPlan);
  }

  /**
   * TODO: allocate schema partition by LoadManager
   *
   * @param noAssignedSchemaPartitionSlotsMap Map<StorageGroupName, List<SeriesPartitionSlot>>
   * @return assign result, Map<StorageGroupName, Map<SeriesPartitionSlot, RegionReplicaSet>>
   */
  private Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> allocateSchemaPartition(
      Map<String, List<SeriesPartitionSlot>> noAssignedSchemaPartitionSlotsMap) {
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> result = new HashMap<>();

    for (String storageGroup : noAssignedSchemaPartitionSlotsMap.keySet()) {
      List<SeriesPartitionSlot> noAssignedPartitionSlots =
          noAssignedSchemaPartitionSlotsMap.get(storageGroup);
      List<RegionReplicaSet> schemaRegionEndPoints =
          RegionInfoPersistence.getInstance().getSchemaRegionEndPoint(storageGroup);
      Random random = new Random();

      Map<SeriesPartitionSlot, RegionReplicaSet> allocateResult = new HashMap<>();
      noAssignedPartitionSlots.forEach(
          seriesPartitionSlot ->
              allocateResult.put(
                  seriesPartitionSlot,
                  schemaRegionEndPoints.get(random.nextInt(schemaRegionEndPoints.size()))));

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
  public DataSet getDataPartition(GetOrCreateDataPartitionPlan physicalPlan) {
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
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionPlan physicalPlan) {
    Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> noAssignedDataPartitionSlots =
        partitionInfoPersistence.filterNoAssignedDataPartitionSlots(
            physicalPlan.getPartitionSlotsMap());

    if (noAssignedDataPartitionSlots.size() > 0) {
      // Allocate DataPartition
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          assignedDataPartition = allocateDataPartition(noAssignedDataPartitionSlots);

      // Persist DataPartition
      CreateDataPartitionPlan createPlan = new CreateDataPartitionPlan();
      createPlan.setAssignedDataPartition(assignedDataPartition);
      getConsensusManager().write(createPlan);
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

  /** Construct SeriesPartitionExecutor by iotdb-confignode.propertis */
  private void setSeriesPartitionExecutor() {
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
    try {
      Class<?> executor = Class.forName(conf.getSeriesPartitionExecutorClass());
      Constructor<?> executorConstructor = executor.getConstructor(int.class);
      this.executor =
          (SeriesPartitionExecutor)
              executorConstructor.newInstance(conf.getSeriesPartitionSlotNum());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      LOGGER.error(
          "Couldn't Constructor SeriesPartitionExecutor class: {}",
          conf.getSeriesPartitionExecutorClass(),
          e);
      executor = null;
    }
  }

  /**
   * Get SeriesPartitionSlot
   *
   * @param devicePath Full path ending with device name
   * @return SeriesPartitionSlot
   */
  public SeriesPartitionSlot getSeriesPartitionSlot(String devicePath) {
    return executor.getSeriesPartitionSlot(devicePath);
  }
}
