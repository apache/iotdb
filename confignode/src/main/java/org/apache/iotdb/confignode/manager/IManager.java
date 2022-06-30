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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.ActivateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.rpc.thrift.TClusterNodeInfos;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import java.util.List;

/**
 * a subset of services provided by {@ConfigManager}. For use internally only, passed to Managers,
 * services.
 */
public interface IManager {

  /**
   * if a service stop
   *
   * @return true if service stopped
   */
  boolean isStopped();

  /**
   * Get DataManager
   *
   * @return DataNodeManager instance
   */
  NodeManager getNodeManager();

  /**
   * Get ConsensusManager
   *
   * @return ConsensusManager instance
   */
  ConsensusManager getConsensusManager();

  /**
   * Get ClusterSchemaManager
   *
   * @return ClusterSchemaManager instance
   */
  ClusterSchemaManager getClusterSchemaManager();

  /**
   * Get PartitionManager
   *
   * @return PartitionManager instance
   */
  PartitionManager getPartitionManager();

  /**
   * Get LoadManager
   *
   * @return LoadManager instance
   */
  LoadManager getLoadManager();

  /**
   * Get UDFManager
   *
   * @return UDFManager instance
   */
  UDFManager getUDFManager();

  /**
   * Register DataNode
   *
   * @return DataNodeConfigurationDataSet
   */
  DataSet registerDataNode(RegisterDataNodePlan registerDataNodePlan);

  /**
   * activate DataNode
   *
   * @param activateDataNodePlan ActivateDataNodePlan
   * @return TSStatus
   */
  TSStatus activateDataNode(ActivateDataNodePlan activateDataNodePlan);

  /**
   * Get DataNode info
   *
   * @return DataNodesInfoDataSet
   */
  DataSet getDataNodeInfo(GetDataNodeInfoPlan getDataNodeInfoPlan);

  TClusterNodeInfos getAllClusterNodeInfos();

  TSStatus setTTL(SetTTLPlan configRequest);

  TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorPlan configPhysicalPlan);

  TSStatus setDataReplicationFactor(SetDataReplicationFactorPlan configPhysicalPlan);

  TSStatus setTimePartitionInterval(SetTimePartitionIntervalPlan configPhysicalPlan);

  /**
   * Count StorageGroups
   *
   * @return The number of matched StorageGroups
   */
  DataSet countMatchedStorageGroups(CountStorageGroupPlan countStorageGroupPlan);

  /**
   * Get StorageGroupSchemas
   *
   * @return StorageGroupSchemaDataSet
   */
  DataSet getMatchedStorageGroupSchemas(GetStorageGroupPlan getOrCountStorageGroupPlan);

  /**
   * Set StorageGroup
   *
   * @return status
   */
  TSStatus setStorageGroup(SetStorageGroupPlan setStorageGroupPlan);

  /**
   * Delete StorageGroups
   *
   * @param deletedPaths List<StringPattern>
   * @return status
   */
  TSStatus deleteStorageGroups(List<String> deletedPaths);

  /**
   * Get SchemaPartition
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionResp getSchemaPartition(PathPatternTree patternTree);

  /**
   * Get or create SchemaPartition
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionResp getOrCreateSchemaPartition(PathPatternTree patternTree);

  /**
   * create SchemaNodeManagementPartition for child paths node management
   *
   * @return TSchemaNodeManagementResp
   */
  TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level);

  /**
   * Get DataPartition
   *
   * @return TDataPartitionResp
   */
  TDataPartitionResp getDataPartition(GetDataPartitionPlan getDataPartitionPlan);

  /**
   * Get or create DataPartition
   *
   * @return TDataPartitionResp
   */
  TDataPartitionResp getOrCreateDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan);

  /**
   * Operate Permission
   *
   * @return status
   */
  TSStatus operatePermission(AuthorPlan authorPlan);

  /**
   * Query Permission
   *
   * @return PermissionInfoDataSet
   */
  DataSet queryPermission(AuthorPlan authorPlan);

  /** login */
  TPermissionInfoResp login(String username, String password);

  /** Check User Privileges */
  TPermissionInfoResp checkUserPrivileges(String username, List<String> paths, int permission);

  /**
   * Register ConfigNode when it is first startup
   *
   * @return TConfigNodeRegisterResp
   */
  TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req);

  /**
   * Add Consensus Group in new node.
   *
   * @return status
   */
  TSStatus addConsensusGroup(List<TConfigNodeLocation> configNodeLocations);

  /**
   * Remove ConfigNode
   *
   * @return status
   */
  TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan);

  TSStatus createFunction(String udfName, String className, List<String> uris);

  TSStatus dropFunction(String udfName);

  TSStatus flush(TFlushReq req);

  void addMetrics();

  /** Show (data/schema) regions */
  DataSet showRegion(GetRegionInfoListPlan getRegionInfoListPlan);
}
