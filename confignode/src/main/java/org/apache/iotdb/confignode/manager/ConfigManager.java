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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupsReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeLocationsResp;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements Manager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  /** Manage PartitionTable read/write requests through the ConsensusLayer */
  private final ConsensusManager consensusManager;

  /** Manage cluster node */
  private final NodeManager nodeManager;

  /** Manage cluster schema */
  private final ClusterSchemaManager clusterSchemaManager;

  /** Manage cluster regions and partitions */
  private final PartitionManager partitionManager;

  /** Manage cluster authorization */
  private final PermissionManager permissionManager;

  private final LoadManager loadManager;

  /** Manage procedure */
  private final ProcedureManager procedureManager;

  public ConfigManager() throws IOException {
    this.nodeManager = new NodeManager(this);
    this.partitionManager = new PartitionManager(this);
    this.clusterSchemaManager = new ClusterSchemaManager(this);
    this.permissionManager = new PermissionManager(this);
    this.loadManager = new LoadManager(this);
    this.procedureManager = new ProcedureManager(this);
    this.consensusManager = new ConsensusManager(this);
  }

  public void close() throws IOException {
    consensusManager.close();
    procedureManager.shiftExecutor(false);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public DataSet registerDataNode(RegisterDataNodeReq registerDataNodeReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.registerDataNode(registerDataNodeReq);
    } else {
      DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
      dataSet.setStatus(status);
      dataSet.setConfigNodeList(NodeInfo.getInstance().getOnlineConfigNodes());
      return dataSet;
    }
  }

  @Override
  public DataSet getDataNodeInfo(GetDataNodeInfoReq getDataNodeInfoReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.getDataNodeInfo(getDataNodeInfoReq);
    } else {
      DataNodeLocationsResp dataSet = new DataNodeLocationsResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setTTL(SetTTLReq setTTLReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTTL(setTTLReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorReq setSchemaReplicationFactorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setSchemaReplicationFactor(setSchemaReplicationFactorReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorReq setDataReplicationFactorReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setDataReplicationFactor(setDataReplicationFactorReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalReq setTimePartitionIntervalReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTimePartitionInterval(setTimePartitionIntervalReq);
    } else {
      return status;
    }
  }

  @Override
  public DataSet countMatchedStorageGroups(CountStorageGroupReq countStorageGroupReq) {
    TSStatus status = confirmLeader();
    CountStorageGroupResp result = new CountStorageGroupResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.countMatchedStorageGroups(countStorageGroupReq);
    } else {
      result.setStatus(status);
    }
    return result;
  }

  @Override
  public DataSet getMatchedStorageGroupSchemas(GetStorageGroupReq getStorageGroupReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getMatchedStorageGroupSchema(getStorageGroupReq);
    } else {
      StorageGroupSchemaResp dataSet = new StorageGroupSchemaResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setStorageGroup(setStorageGroupReq);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus deleteStorageGroups(DeleteStorageGroupsReq deleteStorageGroupsReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> rawPath = deleteStorageGroupsReq.getStorageGroups();
      // remove wild
      Map<String, TStorageGroupSchema> deleteStorageSchemaMap =
          ClusterSchemaInfo.getInstance().getDeleteStorageGroups(rawPath);
      for (Map.Entry<String, TStorageGroupSchema> storageGroupSchemaEntry :
          deleteStorageSchemaMap.entrySet()) {
        String sgName = storageGroupSchemaEntry.getKey();
        TStorageGroupSchema deleteStorageSchema = storageGroupSchemaEntry.getValue();
        deleteStorageSchema.setSchemaRegionGroupIds(
            getClusterSchemaManager().getRegionGroupIds(sgName, TConsensusGroupType.SchemaRegion));
        deleteStorageSchema.setDataRegionGroupIds(
            getClusterSchemaManager().getRegionGroupIds(sgName, TConsensusGroupType.DataRegion));
      }
      ArrayList<TStorageGroupSchema> parsedDeleteStorageGroups =
          new ArrayList<>(deleteStorageSchemaMap.values());
      return procedureManager.deleteStorageGroups(parsedDeleteStorageGroups);
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
      GetSchemaPartitionReq getSchemaPartitionReq = new GetSchemaPartitionReq();
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

      boolean getAll = false;
      Set<String> getAllSet = new HashSet<>();
      for (String devicePath : devicePaths) {
        boolean matchStorageGroup = false;
        for (String storageGroup : storageGroups) {
          if (devicePath.startsWith(storageGroup + ".")) {
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

      getSchemaPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
      return partitionManager.getSchemaPartition(getSchemaPartitionReq);
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
          new GetOrCreateSchemaPartitionReq();
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();

      for (String devicePath : devicePaths) {
        if (!devicePath.contains("*")) {
          // Only check devicePaths that without "*"
          for (String storageGroup : storageGroups) {
            if (devicePath.startsWith(storageGroup + ".")) {
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
  public DataSet getDataPartition(GetDataPartitionReq getDataPartitionReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getDataPartition(getDataPartitionReq);
    } else {
      DataPartitionResp dataSet = new DataPartitionResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public DataSet getOrCreateDataPartition(GetOrCreateDataPartitionReq getOrCreateDataPartitionReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);
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
  public NodeManager getNodeManager() {
    return nodeManager;
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
  public LoadManager getLoadManager() {
    return loadManager;
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

  @Override
  public TSStatus login(String username, String password) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.login(username, password);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus checkUserPrivileges(String username, List<String> paths, int permission) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.checkUserPrivileges(username, paths, permission);
    } else {
      return status;
    }
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    // Check global configuration
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
    TConfigNodeRegisterResp errorResp = new TConfigNodeRegisterResp();
    errorResp.setStatus(new TSStatus(TSStatusCode.ERROR_GLOBAL_CONFIG.getStatusCode()));
    if (!req.getDataNodeConsensusProtocolClass().equals(conf.getDataNodeConsensusProtocolClass())) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the data_node_consensus_protocol_class are consistent.");
      return errorResp;
    }
    if (req.getSeriesPartitionSlotNum() != conf.getSeriesPartitionSlotNum()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the series_partition_slot_num are consistent.");
      return errorResp;
    }
    if (!req.getSeriesPartitionExecutorClass().equals(conf.getSeriesPartitionExecutorClass())) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the series_partition_executor_class are consistent.");
      return errorResp;
    }
    if (req.getDefaultTTL() != CommonDescriptor.getInstance().getConfig().getDefaultTTL()) {
      errorResp
          .getStatus()
          .setMessage("Reject register, please ensure that the default_ttl are consistent.");
      return errorResp;
    }
    if (req.getTimePartitionInterval() != conf.getTimePartitionInterval()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the time_partition_interval are consistent.");
      return errorResp;
    }
    if (req.getSchemaReplicationFactor() != conf.getSchemaReplicationFactor()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the schema_replication_factor are consistent.");
      return errorResp;
    }
    if (req.getDataReplicationFactor() != conf.getDataReplicationFactor()) {
      errorResp
          .getStatus()
          .setMessage(
              "Reject register, please ensure that the data_replication_factor are consistent.");
      return errorResp;
    }

    return nodeManager.registerConfigNode(req);
  }

  @Override
  public TSStatus applyConfigNode(ApplyConfigNodeReq applyConfigNodeReq) {
    return nodeManager.applyConfigNode(applyConfigNodeReq);
  }

  public ProcedureManager getProcedureManager() {
    return procedureManager;
  }
}
