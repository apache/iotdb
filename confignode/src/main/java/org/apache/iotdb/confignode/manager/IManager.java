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
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
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
   * Get ProcedureManager
   *
   * @return ProcedureManager instance
   */
  ProcedureManager getProcedureManager();

  /**
   * Register DataNode
   *
   * @return DataNodeConfigurationDataSet
   */
  DataSet registerDataNode(RegisterDataNodePlan registerDataNodePlan);

  /**
   * Remove DataNode
   *
   * @param removeDataNodePlan
   * @return DataNodeToStatusResp
   */
  DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan);

  /**
   * DataNode report region migrate result to ConfigNode when remove DataNode
   *
   * @param req TRegionMigrateResultReportReq
   * @return TSStatus
   */
  TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req);

  /**
   * Get DataNode info
   *
   * @return DataNodesConfigurationDataSet
   */
  DataSet getDataNodeConfiguration(GetDataNodeConfigurationPlan getDataNodeConfigurationPlan);

  TShowClusterResp showCluster();

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
   * @param isContainedReplicaSet The last map level of result will contain TRegionReplicaSet if
   *     true, otherwise the last level will be TConsensusGroupId
   * @return TSchemaPartitionResp
   */
  Object getSchemaPartition(PathPatternTree patternTree, boolean isContainedReplicaSet);

  /**
   * Get or create SchemaPartition
   *
   * @param isContainedReplicaSet The last map level of result will contain TRegionReplicaSet if
   *     true, otherwise the last level will be TConsensusGroupId
   * @return TSchemaPartitionResp
   */
  Object getOrCreateSchemaPartition(PathPatternTree patternTree, boolean isContainedReplicaSet);

  /**
   * create SchemaNodeManagementPartition for child paths node management
   *
   * @return TSchemaNodeManagementResp
   */
  TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level);

  /**
   * Get DataPartition
   *
   * @param isContainedReplicaSet The last map level of result will contain TRegionReplicaSet if
   *     true, otherwise the last level will be TConsensusGroupId
   * @return TDataPartitionResp
   */
  Object getDataPartition(GetDataPartitionPlan getDataPartitionPlan, boolean isContainedReplicaSet);

  /**
   * Get or create DataPartition
   *
   * @param isContainedReplicaSet The last map level of result will contain TRegionReplicaSet if
   *     true, otherwise the last level will be TConsensusGroupId
   * @return TDataPartitionResp
   */
  Object getOrCreateDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan, boolean isContainedReplicaSet);

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
   * @return TSStatus
   */
  TSStatus registerConfigNode(TConfigNodeRegisterReq req);

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

  /** Merge on all DataNodes */
  TSStatus merge();

  /** Flush on all DataNodes */
  TSStatus flush(TFlushReq req);

  /** Clear cache on all DataNodes */
  TSStatus clearCache();

  /** Load configuration on all DataNodes */
  TSStatus loadConfiguration();

  /**
   * Get the latest RegionRouteMap
   *
   * @return TRegionRouteMapResp
   */
  TRegionRouteMapResp getLatestRegionRouteMap();

  void addMetrics();

  /** Show (data/schema) regions */
  DataSet showRegion(GetRegionInfoListPlan getRegionInfoListPlan);

  /** Show DataNodes */
  TShowDataNodesResp showDataNodes();

  /** Show ConfigNodes */
  TShowConfigNodesResp showConfigNodes();

  /**
   * create schema template
   *
   * @param req TCreateSchemaTemplateReq
   * @return TSStatus
   */
  TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req);

  /**
   * show schema templates
   *
   * @return
   */
  TGetAllTemplatesResp getAllTemplates();

  /**
   * show nodes in schema template
   *
   * @param req String
   * @return
   */
  TGetTemplateResp getTemplate(String req);

  /**
   * set schema template xx to xx-path
   *
   * @param req TSetSchemaTemplateReq
   * @return TSStatus
   */
  TSStatus setSchemaTemplate(TSetSchemaTemplateReq req);

  /**
   * show paths set schema template xx
   *
   * @param req String
   * @return TGetPathsSetTemplatesResp
   */
  TGetPathsSetTemplatesResp getPathsSetTemplate(String req);
}
