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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;

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

  private static final ClusterSchemaInfo clusterSchemaInfo = ClusterSchemaInfo.getInstance();
  private static final PartitionInfo partitionInfo = PartitionInfo.getInstance();

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
  public DataSet getSchemaPartition(GetSchemaPartitionReq physicalPlan) {
    SchemaPartitionResp schemaPartitionResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    schemaPartitionResp = (SchemaPartitionResp) consensusReadResponse.getDataset();
    return schemaPartitionResp;
  }

  /**
   * Get SchemaPartition and create a new one if it does not exist
   *
   * @param physicalPlan SchemaPartitionPlan with partitionSlotsMap
   * @return SchemaPartitionDataSet
   */
  public DataSet getOrCreateSchemaPartition(GetOrCreateSchemaPartitionReq physicalPlan) {
    Map<String, List<TSeriesPartitionSlot>> noAssignedSchemaPartitionSlots =
        partitionInfo.filterNoAssignedSchemaPartitionSlots(physicalPlan.getPartitionSlotsMap());

    if (noAssignedSchemaPartitionSlots.size() > 0) {
      // Allocate SchemaPartition
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition =
          allocateSchemaPartition(noAssignedSchemaPartitionSlots);

      // Persist SchemaPartition
      CreateSchemaPartitionReq createPlan = new CreateSchemaPartitionReq();
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
  private Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> noAssignedSchemaPartitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> result = new HashMap<>();

    for (String storageGroup : noAssignedSchemaPartitionSlotsMap.keySet()) {
      List<TSeriesPartitionSlot> noAssignedPartitionSlots =
          noAssignedSchemaPartitionSlotsMap.get(storageGroup);
      List<TRegionReplicaSet> schemaRegionReplicaSets =
          partitionInfo.getRegionReplicaSets(
              clusterSchemaInfo.getRegionGroupIds(storageGroup, TConsensusGroupType.SchemaRegion));
      Random random = new Random();

      Map<TSeriesPartitionSlot, TRegionReplicaSet> allocateResult = new HashMap<>();
      noAssignedPartitionSlots.forEach(
          seriesPartitionSlot ->
              allocateResult.put(
                  seriesPartitionSlot,
                  schemaRegionReplicaSets.get(random.nextInt(schemaRegionReplicaSets.size()))));

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
  public DataSet getDataPartition(GetDataPartitionReq physicalPlan) {
    DataPartitionResp dataPartitionResp;
    ConsensusReadResponse consensusReadResponse = getConsensusManager().read(physicalPlan);
    dataPartitionResp = (DataPartitionResp) consensusReadResponse.getDataset();
    return dataPartitionResp;
  }

  /**
   * Get DataPartition and create a new one if it does not exist
   *
   * @param physicalPlan DataPartitionPlan with Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return DataPartitionDataSet
   */
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionReq physicalPlan) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> noAssignedDataPartitionSlots =
        partitionInfo.filterNoAssignedDataPartitionSlots(physicalPlan.getPartitionSlotsMap());

    if (noAssignedDataPartitionSlots.size() > 0) {
      // Allocate DataPartition
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          assignedDataPartition = allocateDataPartition(noAssignedDataPartitionSlots);

      // Persist DataPartition
      CreateDataPartitionReq createPlan = new CreateDataPartitionReq();
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
  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      allocateDataPartition(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
              noAssignedDataPartitionSlotsMap) {

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        result = new HashMap<>();

    for (String storageGroup : noAssignedDataPartitionSlotsMap.keySet()) {
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> noAssignedPartitionSlotsMap =
          noAssignedDataPartitionSlotsMap.get(storageGroup);
      List<TRegionReplicaSet> dataRegionEndPoints =
          partitionInfo.getRegionReplicaSets(
              clusterSchemaInfo.getRegionGroupIds(storageGroup, TConsensusGroupType.DataRegion));
      Random random = new Random();

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> allocateResult =
          new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesPartitionEntry :
          noAssignedPartitionSlotsMap.entrySet()) {
        allocateResult.put(seriesPartitionEntry.getKey(), new HashMap<>());
        for (TTimePartitionSlot timePartitionSlot : seriesPartitionEntry.getValue()) {
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
    this.executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            conf.getSeriesPartitionExecutorClass(), conf.getSeriesPartitionSlotNum());
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
}
