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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.pipe.PipeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;

/**
 * A subset of services provided by {@link ConfigManager}. For use internally only, passed to
 * Managers, services.
 */
public interface IManager {

  /**
   * Get DataManager.
   *
   * @return DataNodeManager instance
   */
  NodeManager getNodeManager();

  /**
   * Get ConsensusManager.
   *
   * @return ConsensusManager instance
   */
  ConsensusManager getConsensusManager();

  /**
   * Get ClusterSchemaManager.
   *
   * @return ClusterSchemaManager instance
   */
  ClusterSchemaManager getClusterSchemaManager();

  /**
   * Get PartitionManager.
   *
   * @return PartitionManager instance
   */
  PartitionManager getPartitionManager();

  /**
   * Get LoadManager.
   *
   * @return LoadManager instance
   */
  LoadManager getLoadManager();

  /**
   * Get UDFManager.
   *
   * @return UDFManager instance
   */
  UDFManager getUDFManager();

  /**
   * Get TriggerManager.
   *
   * @return TriggerManager instance
   */
  TriggerManager getTriggerManager();

  /**
   * Get ProcedureManager.
   *
   * @return ProcedureManager instance
   */
  ProcedureManager getProcedureManager();

  /**
   * Get CQManager.
   *
   * @return CQManager instance
   */
  CQManager getCQManager();

  /**
   * Get ModelManager.
   *
   * @return ModelManager instance
   */
  ModelManager getModelManager();

  /**
   * Get PipeManager.
   *
   * @return PipeManager instance
   */
  PipeManager getPipeManager();

  /**
   * Get ClusterQuotaManager.
   *
   * @return ClusterQuotaManager instance
   */
  ClusterQuotaManager getClusterQuotaManager();

  /**
   * Get RetryFailedTasksThread.
   *
   * @return RetryFailedTasksThread instance
   */
  RetryFailedTasksThread getRetryFailedTasksThread();

  /**
   * Get system configurations that is not associated with the DataNodeId.
   *
   * @return SystemConfigurationResp
   */
  DataSet getSystemConfiguration();

  /**
   * Register DataNode.
   *
   * @return DataNodeConfigurationDataSet
   */
  DataSet registerDataNode(TDataNodeRegisterReq req);

  /**
   * Restart DataNode.
   *
   * @param req TDataNodeRestartReq
   * @return SUCCESS_STATUS if allow DataNode to restart, REJECT_START otherwise
   */
  TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req);

  /**
   * Remove DataNode.
   *
   * @param removeDataNodePlan RemoveDataNodePlan
   * @return DataNodeToStatusResp
   */
  DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan);

  /**
   * Report that the specified DataNode will be shutdown.
   *
   * <p>The ConfigNode-leader will mark it as Unknown
   *
   * @return SUCCESS_STATUS if reporting successfully
   */
  TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation);

  /**
   * DataNode report region migrate result to ConfigNode when remove DataNode.
   *
   * @param req TRegionMigrateResultReportReq
   * @return TSStatus
   */
  TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req);

  /**
   * Get DataNode info.
   *
   * @return DataNodesConfigurationDataSet
   */
  DataSet getDataNodeConfiguration(GetDataNodeConfigurationPlan getDataNodeConfigurationPlan);

  /**
   * Get Cluster Nodes' information.
   *
   * @return TShowClusterResp
   */
  TShowClusterResp showCluster();

  /**
   * Get variables.
   *
   * @return TShowVariablesResp
   */
  TShowVariablesResp showVariables();

  TSStatus setTTL(SetTTLPlan configRequest);

  TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorPlan configPhysicalPlan);

  TSStatus setDataReplicationFactor(SetDataReplicationFactorPlan configPhysicalPlan);

  TSStatus setTimePartitionInterval(SetTimePartitionIntervalPlan configPhysicalPlan);

  /**
   * Count StorageGroups.
   *
   * @return The number of matched StorageGroups
   */
  DataSet countMatchedDatabases(CountDatabasePlan countDatabasePlan);

  /**
   * Get StorageGroupSchemas.
   *
   * @return StorageGroupSchemaDataSet
   */
  DataSet getMatchedDatabaseSchemas(GetDatabasePlan getOrCountStorageGroupPlan);

  /**
   * Set Database.
   *
   * @return TSStatus
   */
  TSStatus setDatabase(DatabaseSchemaPlan databaseSchemaPlan);

  /**
   * Alter Database.
   *
   * @return TSStatus
   */
  TSStatus alterDatabase(DatabaseSchemaPlan databaseSchemaPlan);

  /**
   * Delete StorageGroups.
   *
   * @param deletedPaths List{@literal <}String{@literal >}
   * @return status
   */
  TSStatus deleteDatabases(List<String> deletedPaths);

  /**
   * Get SchemaPartition.
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionTableResp getSchemaPartition(PathPatternTree patternTree);

  /**
   * Get or create SchemaPartition.
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionTableResp getOrCreateSchemaPartition(PathPatternTree patternTree);

  /**
   * Create SchemaNodeManagementPartition for child paths node management.
   *
   * @return TSchemaNodeManagementResp
   */
  TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level);

  /**
   * Get DataPartition.
   *
   * @return TDataPartitionResp
   */
  TDataPartitionTableResp getDataPartition(GetDataPartitionPlan getDataPartitionPlan);

  /**
   * Get or create DataPartition.
   *
   * @return TDataPartitionResp
   */
  TDataPartitionTableResp getOrCreateDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan);

  /**
   * Operate Permission.
   *
   * @return status
   */
  TSStatus operatePermission(AuthorPlan authorPlan);

  /**
   * Query Permission.
   *
   * @return PermissionInfoDataSet
   */
  DataSet queryPermission(AuthorPlan authorPlan);

  /** login. */
  TPermissionInfoResp login(String username, String password);

  /** Check User Privileges. */
  TPermissionInfoResp checkUserPrivileges(String username, List<PartialPath> paths, int permission);

  /**
   * Register ConfigNode when it is first startup.
   *
   * @return TConfigNodeRegisterResp
   */
  TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req);

  TSStatus restartConfigNode(TConfigNodeRestartReq req);

  /**
   * Create peer in new node to build consensus group.
   *
   * @return status
   */
  TSStatus createPeerForConsensusGroup(List<TConfigNodeLocation> configNodeLocations);

  /**
   * Remove ConfigNode.
   *
   * @return status
   */
  TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan);

  /**
   * Report that the specified ConfigNode will be shutdown. The ConfigNode-leader will mark it as
   * Unknown.
   *
   * @return SUCCESS_STATUS if reporting successfully
   */
  TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation);

  TSStatus createFunction(TCreateFunctionReq req);

  TSStatus dropFunction(String udfName);

  TGetUDFTableResp getUDFTable();

  TGetJarInListResp getUDFJar(TGetJarInListReq req);

  /** Create trigger. */
  TSStatus createTrigger(TCreateTriggerReq req);

  /** Drop trigger. */
  TSStatus dropTrigger(TDropTriggerReq req);

  /** Show trigger & DataNode start. */
  TGetTriggerTableResp getTriggerTable();

  /** DataNode refresh stateful trigger cache. */
  TGetTriggerTableResp getStatefulTriggerTable();

  /** Get DataNodeLocation of stateful trigger. */
  TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName);

  /** Get Trigger jar. */
  TGetJarInListResp getTriggerJar(TGetJarInListReq req);

  /** Create pipe plugin. */
  TSStatus createPipePlugin(TCreatePipePluginReq req);

  /** Drop pipe plugin. */
  TSStatus dropPipePlugin(String pluginName);

  /** Show pipe plugins. */
  TGetPipePluginTableResp getPipePluginTable();

  /** Get pipe plugin jar. */
  TGetJarInListResp getPipePluginJar(TGetJarInListReq req);

  /** Merge on all DataNodes. */
  TSStatus merge();

  /** Flush on all DataNodes. */
  TSStatus flush(TFlushReq req);

  /** Clear cache on all DataNodes. */
  TSStatus clearCache();

  /** Load configuration on all DataNodes. */
  TSStatus loadConfiguration();

  /** Set system status on all DataNodes. */
  TSStatus setSystemStatus(String status);

  /** TestOnly. Set the target DataNode to the specified status */
  TSStatus setDataNodeStatus(TSetDataNodeStatusReq req);

  TSStatus killQuery(String queryId, int dataNodeId);

  TGetDataNodeLocationsResp getRunningDataNodeLocations();

  /**
   * Get the latest RegionRouteMap.
   *
   * @return TRegionRouteMapResp
   */
  TRegionRouteMapResp getLatestRegionRouteMap();

  void addMetrics();

  /** Show (data/schemaengine) regions. */
  DataSet showRegion(GetRegionInfoListPlan getRegionInfoListPlan);

  /** Show DataNodes. */
  TShowDataNodesResp showDataNodes();

  /** Show ConfigNodes. */
  TShowConfigNodesResp showConfigNodes();

  /**
   * Show StorageGroup.
   *
   * @param getStorageGroupPlan GetStorageGroupPlan, including path patterns about StorageGroups
   * @return TShowStorageGroupResp
   */
  TShowDatabaseResp showDatabase(GetDatabasePlan getStorageGroupPlan);

  /**
   * Create schemaengine template.
   *
   * @param req TCreateSchemaTemplateReq
   * @return TSStatus
   */
  TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req);

  /**
   * Show schemaengine templates.
   *
   * @return TGetAllTemplatesResp
   */
  TGetAllTemplatesResp getAllTemplates();

  /**
   * Show nodes in schemaengine template.
   *
   * @param req String
   * @return TGetTemplateResp
   */
  TGetTemplateResp getTemplate(String req);

  /**
   * Set schemaengine template xx to xx-path.
   *
   * @param req TSetSchemaTemplateReq
   * @return TSStatus
   */
  TSStatus setSchemaTemplate(TSetSchemaTemplateReq req);

  /**
   * show paths set schemaengine template xx.
   *
   * @param req String
   * @return TGetPathsSetTemplatesResp
   */
  TGetPathsSetTemplatesResp getPathsSetTemplate(String req);

  /** Deactivate schemaengine template. */
  TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req);

  /** Unset schemaengine template. */
  TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req);

  /** Drop schemaengine template. */
  TSStatus dropSchemaTemplate(String templateName);

  TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req);

  /** Delete timeseries. */
  TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req);

  TSStatus deleteLogicalView(TDeleteLogicalViewReq req);

  TSStatus alterLogicalView(TAlterLogicalViewReq req);

  /**
   * Create Pipe.
   *
   * @param req Info about Pipe
   * @return TSStatus
   */
  TSStatus createPipe(TCreatePipeReq req);

  /**
   * Start Pipe.
   *
   * @param pipeName name of Pipe
   * @return TSStatus
   */
  TSStatus startPipe(String pipeName);

  /**
   * Stop Pipe.
   *
   * @param pipeName name of Pipe
   * @return TSStatus
   */
  TSStatus stopPipe(String pipeName);

  /**
   * Drop Pipe.
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

  /**
   * Get all pipe information. It is used for DataNode registration and restart.
   *
   * @return All pipe information.
   */
  TGetAllPipeInfoResp getAllPipeInfo();

  /**
   * Get RegionId。used for Show cluster slots information in
   * docs/zh/UserGuide/Cluster/Cluster-Maintenance.md.
   *
   * @return TGetRegionIdResp.
   */
  TGetRegionIdResp getRegionId(TGetRegionIdReq req);

  /**
   * Get timeSlot(timePartition)。used for Show cluster slots information in
   * docs/zh/UserGuide/Cluster/Cluster-Maintenance.md.
   *
   * @return TGetTimeSlotListResp.
   */
  TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req);

  /**
   * Count timeSlot(timePartition)。used for Show cluster slots information in
   * docs/zh/UserGuide/Cluster/Cluster-Maintenance.md.
   *
   * @return TCountTimeSlotListResp.
   */
  TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req);

  /**
   * Get seriesSlot。used for Show cluster slots information in
   * docs/zh/UserGuide/Cluster/Cluster-Maintenance.md.
   *
   * @return TGetSeriesSlotListResp.
   */
  TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req);

  TSStatus migrateRegion(TMigrateRegionReq req);

  TSStatus createCQ(TCreateCQReq req);

  TSStatus dropCQ(TDropCQReq req);

  TShowCQResp showCQ();

  TSStatus checkConfigNodeGlobalConfig(TConfigNodeRegisterReq req);

  TSStatus transfer(List<TDataNodeLocation> newUnknownDataList);

  /** Create a model. */
  TSStatus createModel(TCreateModelReq req);

  /** Drop a model. */
  TSStatus dropModel(TDropModelReq req);

  /** Return the model table. */
  TShowModelResp showModel(TShowModelReq req);

  /** Return the trail table. */
  TShowTrailResp showTrail(TShowTrailReq req);

  /** Update the model info. */
  TSStatus updateModelInfo(TUpdateModelInfoReq req);

  /** Update the model state. */
  TSStatus updateModelState(TUpdateModelStateReq req);

  /** Set space quota. */
  TSStatus setSpaceQuota(TSetSpaceQuotaReq req);
}
