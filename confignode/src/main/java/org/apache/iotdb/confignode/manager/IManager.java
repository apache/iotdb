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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRoutingPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRoutingResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStorageGroupResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;

/**
 * a subset of services provided by {@ConfigManager}. For use internally only, passed to Managers,
 * services.
 */
public interface IManager {

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
   * Get TriggerManager
   *
   * @return TriggerManager instance
   */
  TriggerManager getTriggerManager();

  /**
   * Get SyncManager
   *
   * @return SyncManager instance
   */
  SyncManager getSyncManager();

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
   * @param removeDataNodePlan RemoveDataNodePlan
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
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionTableResp getSchemaPartition(PathPatternTree patternTree);

  /**
   * Get or create SchemaPartition
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionTableResp getOrCreateSchemaPartition(PathPatternTree patternTree);

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
  TDataPartitionTableResp getDataPartition(GetDataPartitionPlan getDataPartitionPlan);

  /**
   * Get or create DataPartition
   *
   * @return TDataPartitionResp
   */
  TDataPartitionTableResp getOrCreateDataPartition(
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
   * @return TSStatus
   */
  TSStatus registerConfigNode(TConfigNodeRegisterReq req);

  /**
   * Create peer in new node to build consensus group.
   *
   * @return status
   */
  TSStatus createPeerForConsensusGroup(List<TConfigNodeLocation> configNodeLocations);

  /**
   * Remove ConfigNode
   *
   * @return status
   */
  TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan);

  TSStatus createFunction(String udfName, String className, List<String> uris);

  TSStatus dropFunction(String udfName);

  /** Create trigger */
  TSStatus createTrigger(TCreateTriggerReq req);

  /** Drop trigger */
  TSStatus dropTrigger(TDropTriggerReq req);

  /** Show trigger & DataNode start */
  TGetTriggerTableResp getTriggerTable();

  /** Merge on all DataNodes */
  TSStatus merge();

  /** Flush on all DataNodes */
  TSStatus flush(TFlushReq req);

  /** Clear cache on all DataNodes */
  TSStatus clearCache();

  /** Load configuration on all DataNodes */
  TSStatus loadConfiguration();

  /** Set system status on all DataNodes */
  TSStatus setSystemStatus(String status);

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
   * Show StorageGroup
   *
   * @param getStorageGroupPlan GetStorageGroupPlan, including path patterns about StorageGroups
   * @return TShowStorageGroupResp
   */
  TShowStorageGroupResp showStorageGroup(GetStorageGroupPlan getStorageGroupPlan);

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
   * @return TGetAllTemplatesResp
   */
  TGetAllTemplatesResp getAllTemplates();

  /**
   * show nodes in schema template
   *
   * @param req String
   * @return TGetTemplateResp
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

  /*
   * delete timeseries
   *
   */
  TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req);

  /**
   * Create PipeSink
   *
   * @param plan Info about PipeSink
   * @return TSStatus
   */
  TSStatus createPipeSink(CreatePipeSinkPlan plan);

  /**
   * Drop PipeSink
   *
   * @param plan Name of PipeSink
   * @return TSStatus
   */
  TSStatus dropPipeSink(DropPipeSinkPlan plan);

  /**
   * Get PipeSink by name. If pipeSinkName is empty, get all PipeSinks.
   *
   * @param req specify the pipeSinkName
   * @return TGetPipeSinkResp contains the PipeSink
   */
  TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req);

  /**
   * Create Pipe
   *
   * @param pipeInfo Info about Pipe
   * @return TSStatus
   */
  TSStatus createPipe(TPipeInfo pipeInfo);

  /**
   * Start Pipe
   *
   * @param pipeName name of Pipe
   * @return TSStatus
   */
  TSStatus startPipe(String pipeName);

  /**
   * Stop Pipe
   *
   * @param pipeName name of Pipe
   * @return TSStatus
   */
  TSStatus stopPipe(String pipeName);

  /**
   * Drop Pipe
   *
   * @param pipeName name of Pipe
   * @return TSStatus
   */
  TSStatus dropPipe(String pipeName);

  /**
   * Get Pipe by name. If pipeName is empty, get all Pipe.
   *
   * @param req specify the pipeName
   * @return TShowPipeResp contains the TShowPipeInfo
   */
  TShowPipeResp showPipe(TShowPipeReq req);

  TGetRoutingResp getRouting(GetRoutingPlan plan);

  TGetTimeSlotListResp getTimeSlotList(GetTimeSlotListPlan plan);

  TGetSeriesSlotListResp getSeriesSlotList(GetSeriesSlotListPlan plan);
}
