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

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationDataSet;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements Manager {

  private static final TSStatus ERROR_TSSTATUS =
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

  /** manage consensus, write or read consensus */
  private final ConsensusManager consensusManager;

  /** manage data node */
  private final DataNodeManager dataNodeManager;

  /** manage assign data partition and schema partition */
  private final PartitionManager partitionManager;

  /** manager assign schema region and data region */
  private final RegionManager regionManager;

  private final PermissionManager permissionManager;

  public ConfigManager() throws IOException {
    this.dataNodeManager = new DataNodeManager(this);
    this.partitionManager = new PartitionManager(this);
    this.regionManager = new RegionManager(this);
    this.consensusManager = new ConsensusManager();
    this.permissionManager = new PermissionManager(this);
  }

  public void close() throws IOException {
    consensusManager.close();
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public DataSet registerDataNode(PhysicalPlan physicalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return dataNodeManager.registerDataNode((RegisterDataNodePlan) physicalPlan);
    } else {
      DataNodeConfigurationDataSet dataSet = new DataNodeConfigurationDataSet();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getDataNodeInfo(PhysicalPlan physicalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return dataNodeManager.getDataNodeInfo((QueryDataNodeInfoPlan) physicalPlan);
    } else {
      DataNodesInfoDataSet dataSet = new DataNodesInfoDataSet();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getStorageGroupSchema() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return regionManager.getStorageGroupSchema();
    } else {
      StorageGroupSchemaDataSet dataSet = new StorageGroupSchemaDataSet();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setStorageGroup(PhysicalPlan physicalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return regionManager.setStorageGroup((SetStorageGroupPlan) physicalPlan);
    } else {
      return status;
    }
  }

  @Override
  public DataSet getSchemaPartition(PathPatternTree patternTree) {
    List<String> devicePaths = patternTree.findAllDevicePaths();
    List<String> storageGroups = getRegionManager().getStorageGroupNames();

    GetOrCreateSchemaPartitionPlan getSchemaPartitionPlan =
      new GetOrCreateSchemaPartitionPlan(PhysicalPlanType.GetSchemaPartition);
    Map<String, List<SeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

    boolean getAll = false;
    Set<String> getAllSet = new HashSet<>();
    for (String devicePath : devicePaths) {
      boolean matchStorageGroup = false;
      for (String storageGroup : storageGroups) {
        if (devicePath.contains(storageGroup)) {
          matchStorageGroup = true;
          if (devicePath.contains("*")) {
            getAllSet.add(storageGroup);
          } else {
            SeriesPartitionSlot seriesPartitionSlot = getPartitionManager().getSeriesPartitionSlot(devicePath);
            partitionSlotsMap.computeIfAbsent(storageGroup, key -> new ArrayList<>()).add(seriesPartitionSlot);
          }
          break;
        }
      }
      if (!matchStorageGroup && devicePath.contains("**")) {
        getAll = true;
      }
    }

    if (getAll) {
      partitionSlotsMap = new HashMap<>();
    } else {
      for (String storageGroup : getAllSet) {
        if (partitionSlotsMap.containsKey(storageGroup)) {
          partitionSlotsMap.replace(storageGroup, new ArrayList<>());
        } else {
          partitionSlotsMap.put(storageGroup, new ArrayList<>());
        }
      }
    }

    getSchemaPartitionPlan.setPartitionSlotsMap(partitionSlotsMap);
    return partitionManager.getSchemaPartition(getSchemaPartitionPlan);
  }

  @Override
  public DataSet getOrCreateSchemaPartition(PathPatternTree patternTree) {
    List<String> devicePaths = patternTree.findAllDevicePaths();
    List<String> storageGroups = getRegionManager().getStorageGroupNames();

    GetOrCreateSchemaPartitionPlan getOrCreateSchemaPartitionPlan =
      new GetOrCreateSchemaPartitionPlan(PhysicalPlanType.GetOrCreateSchemaPartition);
    Map<String, List<SeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

    for (String device : devicePaths) {
      if (!device.contains("*")) {
        for (String storageGroup : storageGroups) {
          if (device.contains(storageGroup)) {
            SeriesPartitionSlot seriesPartitionSlot = getPartitionManager().getSeriesPartitionSlot(device);
            partitionSlotsMap.computeIfAbsent(storageGroup, key -> new ArrayList<>()).add(seriesPartitionSlot);
            break;
          }
        }
      }
    }

    getOrCreateSchemaPartitionPlan.setPartitionSlotsMap(partitionSlotsMap);
    return partitionManager.getOrCreateSchemaPartition(getOrCreateSchemaPartitionPlan);
  }

  @Override
  public DataSet getDataPartition(PhysicalPlan physicalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getDataPartition((GetOrCreateDataPartitionPlan) physicalPlan);
    } else {
      DataPartitionDataSet dataSet = new DataPartitionDataSet();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getOrCreateDataPartition(PhysicalPlan physicalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getOrCreateDataPartition((GetOrCreateDataPartitionPlan) physicalPlan);
    } else {
      DataPartitionDataSet dataSet = new DataPartitionDataSet();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  private TSStatus confirmLeader() {
    if (getConsensusManager().isLeader()) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      Endpoint endpoint = getConsensusManager().getLeader();
      if (endpoint == null) {
        return new TSStatus(TSStatusCode.NEED_REDIRECTION.getStatusCode())
            .setMessage(
                "The current ConfigNode is not leader. And ConfigNodeGroup is in leader election. Please redirect with a random ConfigNode.");
      } else {
        return new TSStatus(TSStatusCode.NEED_REDIRECTION.getStatusCode())
            .setRedirectNode(new EndPoint(endpoint.getIp(), endpoint.getPort()))
            .setMessage("The current ConfigNode is not leader. Please redirect.");
      }
    }
  }

  @Override
  public DataNodeManager getDataNodeManager() {
    return dataNodeManager;
  }

  @Override
  public RegionManager getRegionManager() {
    return regionManager;
  }

  @Override
  public ConsensusManager getConsensusManager() {
    return consensusManager;
  }

  @Override
  public PartitionManager getPartitionManager() {
    return partitionManager;
  }

  @Override
  public TSStatus operatePermission(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof AuthorPlan) {
      return permissionManager.operatePermission((AuthorPlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }
}
