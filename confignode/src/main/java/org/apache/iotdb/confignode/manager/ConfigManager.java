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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.QueryDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeLocationsResp;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements Manager {

  private static final TSStatus ERROR_TSSTATUS =
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

  /** Manage PartitionTable read/write requests through the ConsensusLayer */
  private final ConsensusManager consensusManager;

  /** Manage cluster DataNode information */
  private final DataNodeManager dataNodeManager;

  /** Manage cluster schema */
  private final ClusterSchemaManager clusterSchemaManager;

  /** Manage cluster regions and partitions */
  private final PartitionManager partitionManager;

  /** Manage cluster authorization */
  private final PermissionManager permissionManager;

  public ConfigManager() throws IOException {
    this.dataNodeManager = new DataNodeManager(this);
    this.partitionManager = new PartitionManager(this);
    this.clusterSchemaManager = new ClusterSchemaManager(this);
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
  public DataSet registerDataNode(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return dataNodeManager.registerDataNode((RegisterDataNodeReq) configRequest);
    } else {
      DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getDataNodeInfo(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return dataNodeManager.getDataNodeInfo((QueryDataNodeInfoReq) configRequest);
    } else {
      DataNodeLocationsResp dataSet = new DataNodeLocationsResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getStorageGroupSchema() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getStorageGroupSchema();
    } else {
      StorageGroupSchemaResp dataSet = new StorageGroupSchemaResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setStorageGroup(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setStorageGroup((SetStorageGroupReq) configRequest);
    } else {
      return status;
    }
  }

  @Override
  public DataSet getSchemaPartition(PathPatternTree patternTree) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> devicePaths = patternTree.findAllDevicePaths();
      List<String> storageGroups = getClusterSchemaManager().getStorageGroupNames();

      GetOrCreateSchemaPartitionReq getSchemaPartitionPlan =
          new GetOrCreateSchemaPartitionReq(ConfigRequestType.GetSchemaPartition);
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

      boolean getAll = false;
      Set<String> getAllSet = new HashSet<>();
      for (String devicePath : devicePaths) {
        boolean matchStorageGroup = false;
        for (String storageGroup : storageGroups) {
          if (devicePath.contains(storageGroup)) {
            matchStorageGroup = true;
            if (devicePath.contains("*")) {
              // Get all SchemaPartitions of this StorageGroup if the devicePath contains "*"
              getAllSet.add(storageGroup);
            } else {
              // Get the specific SchemaPartition
              partitionSlotsMap
                  .computeIfAbsent(storageGroup, key -> new ArrayList<>())
                  .add(getPartitionManager().getSeriesPartitionSlot(devicePath));
            }
            break;
          }
        }
        if (!matchStorageGroup && devicePath.contains("**")) {
          // Get all SchemaPartitions if there exists one devicePath that contains "**"
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
    } else {
      SchemaPartitionResp dataSet = new SchemaPartitionResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getOrCreateSchemaPartition(PathPatternTree patternTree) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> devicePaths = patternTree.findAllDevicePaths();
      List<String> storageGroups = getClusterSchemaManager().getStorageGroupNames();

      GetOrCreateSchemaPartitionReq getOrCreateSchemaPartitionReq =
          new GetOrCreateSchemaPartitionReq(ConfigRequestType.GetOrCreateSchemaPartition);
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

      for (String devicePath : devicePaths) {
        if (!devicePath.contains("*")) {
          // Only check devicePaths that without "*"
          for (String storageGroup : storageGroups) {
            if (devicePath.contains(storageGroup)) {
              partitionSlotsMap
                  .computeIfAbsent(storageGroup, key -> new ArrayList<>())
                  .add(getPartitionManager().getSeriesPartitionSlot(devicePath));
              break;
            }
          }
        }
      }

      getOrCreateSchemaPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
      return partitionManager.getOrCreateSchemaPartition(getOrCreateSchemaPartitionReq);
    } else {
      SchemaPartitionResp dataSet = new SchemaPartitionResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getDataPartition(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getDataPartition((GetOrCreateDataPartitionReq) configRequest);
    } else {
      DataPartitionResp dataSet = new DataPartitionResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getOrCreateDataPartition(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getOrCreateDataPartition((GetOrCreateDataPartitionReq) configRequest);
    } else {
      DataPartitionResp dataSet = new DataPartitionResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  private TSStatus confirmLeader() {
    if (getConsensusManager().isLeader()) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return new TSStatus(TSStatusCode.NEED_REDIRECTION.getStatusCode())
          .setMessage(
              "The current ConfigNode is not leader. And ConfigNodeGroup is in leader election. Please redirect with a random ConfigNode.");
    }
  }

  @Override
  public DataNodeManager getDataNodeManager() {
    return dataNodeManager;
  }

  @Override
  public ClusterSchemaManager getClusterSchemaManager() {
    return clusterSchemaManager;
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
  public TSStatus operatePermission(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.operatePermission((AuthorReq) configRequest);
    } else {
      return status;
    }
  }

  @Override
  public DataSet queryPermission(ConfigRequest configRequest) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.queryPermission((AuthorReq) configRequest);
    } else {
      PermissionInfoResp dataSet = new PermissionInfoResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }
}
