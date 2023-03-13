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

package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.datanode.ConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRecordPipeMessageReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.db.mpp.plan.statement.AuthorType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServiceProcessor implements IConfigNodeRPCService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServiceProcessor.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONFIG =
      ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  public ConfigNodeRPCServiceProcessor(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @TestOnly
  public void close() throws IOException {
    configManager.close();
  }

  @TestOnly
  public ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  @Override
  public TSystemConfigurationResp getSystemConfiguration() {
    TSystemConfigurationResp resp =
        ((ConfigurationResp) configManager.getSystemConfiguration())
            .convertToRpcSystemConfigurationResp();

    // Print log to record the ConfigNode that performs the GetConfigurationRequest
    LOGGER.info("Execute GetSystemConfiguration with result {}", resp);
    return resp;
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) {
    TDataNodeRegisterResp resp =
        ((DataNodeRegisterResp) configManager.registerDataNode(req))
            .convertToRpcDataNodeRegisterResp();

    // Print log to record the ConfigNode that performs the RegisterDatanodeRequest
    LOGGER.info("Execute RegisterDataNodeRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) {
    TDataNodeRestartResp resp = configManager.restartDataNode(req);

    // Print log to record the ConfigNode that performs the RestartDatanodeRequest
    LOGGER.info("Execute RestartDataNodeRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req) {
    LOGGER.info("ConfigNode RPC Service start to remove DataNode, req: {}", req);
    RemoveDataNodePlan removeDataNodePlan = new RemoveDataNodePlan(req.getDataNodeLocations());
    DataNodeToStatusResp removeResp =
        (DataNodeToStatusResp) configManager.removeDataNode(removeDataNodePlan);
    TDataNodeRemoveResp resp = removeResp.convertToRpCDataNodeRemoveResp();
    LOGGER.info(
        "ConfigNode RPC Service finished to remove DataNode, req: {}, result: {}", req, resp);
    return resp;
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) {
    return configManager.reportDataNodeShutdown(dataNodeLocation);
  }

  @Override
  public TDataNodeConfigurationResp getDataNodeConfiguration(int dataNodeID) {
    GetDataNodeConfigurationPlan queryReq = new GetDataNodeConfigurationPlan(dataNodeID);
    DataNodeConfigurationResp queryResp =
        (DataNodeConfigurationResp) configManager.getDataNodeConfiguration(queryReq);

    TDataNodeConfigurationResp resp = new TDataNodeConfigurationResp();
    queryResp.convertToRpcDataNodeLocationResp(resp);
    return resp;
  }

  @Override
  public TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req) {
    return configManager.reportRegionMigrateResult(req);
  }

  @Override
  public TShowClusterResp showCluster() {
    return configManager.showCluster();
  }

  @Override
  public TShowVariablesResp showVariables() {
    return configManager.showVariables();
  }

  @Override
  public TSStatus setDatabase(TDatabaseSchema databaseSchema) {
    TSStatus errorResp = null;

    // Set default configurations if necessary
    if (!databaseSchema.isSetTTL()) {
      databaseSchema.setTTL(CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs());
    } else if (databaseSchema.getTTL() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage("Failed to create database. The TTL should be positive.");
    }

    if (!databaseSchema.isSetSchemaReplicationFactor()) {
      databaseSchema.setSchemaReplicationFactor(CONFIG_NODE_CONFIG.getSchemaReplicationFactor());
    } else if (databaseSchema.getSchemaReplicationFactor() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to create database. The schemaReplicationFactor should be positive.");
    }

    if (!databaseSchema.isSetDataReplicationFactor()) {
      databaseSchema.setDataReplicationFactor(CONFIG_NODE_CONFIG.getDataReplicationFactor());
    } else if (databaseSchema.getDataReplicationFactor() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to create database. The dataReplicationFactor should be positive.");
    }

    if (!databaseSchema.isSetTimePartitionInterval()) {
      databaseSchema.setTimePartitionInterval(CONFIG_NODE_CONFIG.getTimePartitionInterval());
    } else if (databaseSchema.getTimePartitionInterval() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to create database. The timePartitionInterval should be positive.");
    }

    if (!databaseSchema.isSetMinSchemaRegionGroupNum()) {
      databaseSchema.setMinSchemaRegionGroupNum(
          CONFIG_NODE_CONFIG.getDefaultSchemaRegionGroupNumPerDatabase());
    } else if (databaseSchema.getMinSchemaRegionGroupNum() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to create database. The schemaRegionGroupNum should be positive.");
    }

    if (!databaseSchema.isSetMinDataRegionGroupNum()) {
      databaseSchema.setMinDataRegionGroupNum(
          CONFIG_NODE_CONFIG.getDefaultDataRegionGroupNumPerDatabase());
    } else if (databaseSchema.getMinDataRegionGroupNum() <= 0) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage("Failed to create database. The dataRegionGroupNum should be positive.");
    }

    if (errorResp != null) {
      LOGGER.warn("Execute SetDatabase: {} with result: {}", databaseSchema, errorResp);
      return errorResp;
    }

    // The maxRegionGroupNum is equal to the minRegionGroupNum when initialize
    databaseSchema.setMaxSchemaRegionGroupNum(databaseSchema.getMinSchemaRegionGroupNum());
    databaseSchema.setMaxDataRegionGroupNum(databaseSchema.getMinDataRegionGroupNum());

    DatabaseSchemaPlan setPlan =
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, databaseSchema);
    TSStatus resp = configManager.setDatabase(setPlan);

    // Print log to record the ConfigNode that performs the set SetDatabaseRequest
    LOGGER.info("Execute SetDatabase: {} with result: {}", databaseSchema, resp);

    return resp;
  }

  @Override
  public TSStatus alterDatabase(TDatabaseSchema databaseSchema) {
    TSStatus errorResp = null;

    // TODO: Support alter the following fields
    if (databaseSchema.isSetTTL()) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage("Failed to alter database. Doesn't support ALTER TTL yet.");
    }
    if (databaseSchema.isSetSchemaReplicationFactor()) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to alter database. Doesn't support ALTER SchemaReplicationFactor yet.");
    }
    if (databaseSchema.isSetDataReplicationFactor()) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to alter database. Doesn't support ALTER DataReplicationFactor yet.");
    }
    if (databaseSchema.isSetTimePartitionInterval()) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to alter database. Doesn't support ALTER TimePartitionInterval yet.");
    }

    if (errorResp != null) {
      LOGGER.warn("Execute AlterDatabase: {} with result: {}", databaseSchema, errorResp);
      return errorResp;
    }

    DatabaseSchemaPlan alterPlan =
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, databaseSchema);
    TSStatus resp = configManager.alterDatabase(alterPlan);

    // Print log to record the ConfigNode that performs the set SetDatabaseRequest
    LOGGER.info("Execute AlterDatabase: {} with result: {}", databaseSchema, resp);

    return resp;
  }

  @Override
  public TSStatus deleteDatabase(TDeleteDatabaseReq tDeleteReq) {
    String prefixPath = tDeleteReq.getPrefixPath();
    return configManager.deleteStorageGroups(Collections.singletonList(prefixPath));
  }

  @Override
  public TSStatus deleteDatabases(TDeleteDatabasesReq tDeleteReq) {
    List<String> prefixList = tDeleteReq.getPrefixPathList();
    return configManager.deleteStorageGroups(prefixList);
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return configManager.setTTL(new SetTTLPlan(req.getStorageGroupPathPattern(), req.getTTL()));
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return configManager.setSchemaReplicationFactor(
        new SetSchemaReplicationFactorPlan(req.getDatabase(), req.getSchemaReplicationFactor()));
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return configManager.setDataReplicationFactor(
        new SetDataReplicationFactorPlan(req.getDatabase(), req.getDataReplicationFactor()));
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return configManager.setTimePartitionInterval(
        new SetTimePartitionIntervalPlan(req.getDatabase(), req.getTimePartitionInterval()));
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(List<String> storageGroupPathPattern) {
    CountDatabaseResp countDatabaseResp =
        (CountDatabaseResp)
            configManager.countMatchedStorageGroups(new CountDatabasePlan(storageGroupPathPattern));

    TCountDatabaseResp resp = new TCountDatabaseResp();
    countDatabaseResp.convertToRPCCountStorageGroupResp(resp);
    return resp;
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(List<String> storageGroupPathPattern) {
    DatabaseSchemaResp databaseSchemaResp =
        (DatabaseSchemaResp)
            configManager.getMatchedStorageGroupSchemas(
                new GetDatabasePlan(storageGroupPathPattern));

    return databaseSchemaResp.convertToRPCStorageGroupSchemaResp();
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return configManager.getSchemaPartition(patternTree);
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return configManager.getOrCreateSchemaPartition(patternTree);
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    PartialPath partialPath = patternTree.getAllPathPatterns().get(0);
    return configManager.getNodePathsPartition(partialPath, req.getLevel());
  }

  @Override
  public TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req) {
    GetDataPartitionPlan getDataPartitionPlan =
        GetDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    return configManager.getDataPartition(getDataPartitionPlan);
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req) {
    GetOrCreateDataPartitionPlan getOrCreateDataPartitionReq =
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    return configManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) {
    if (req.getAuthorType() < 0 || req.getAuthorType() >= AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorPlan plan = null;
    try {
      plan =
          new AuthorPlan(
              ConfigPhysicalPlanType.values()[
                  req.getAuthorType() + ConfigPhysicalPlanType.Author.ordinal() + 1],
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeNameList());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    return configManager.operatePermission(plan);
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) {
    if (req.getAuthorType() < 0 || req.getAuthorType() >= AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorPlan plan = null;
    try {
      plan =
          new AuthorPlan(
              ConfigPhysicalPlanType.values()[
                  req.getAuthorType() + ConfigPhysicalPlanType.Author.ordinal() + 1],
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeNameList());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    PermissionInfoResp dataSet = (PermissionInfoResp) configManager.queryPermission(plan);
    TAuthorizerResp resp = new TAuthorizerResp(dataSet.getStatus());
    resp.setAuthorizerInfo(dataSet.getPermissionInfo());
    return resp;
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) {
    return configManager.login(req.getUserrname(), req.getPassword());
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) {
    return configManager.checkUserPrivileges(
        req.getUsername(), req.getPaths(), req.getPermission());
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    TConfigNodeRegisterResp resp = configManager.registerConfigNode(req);

    // Print log to record the ConfigNode that performs the RegisterConfigNodeRequest
    LOGGER.info("Execute RegisterConfigNodeRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TSStatus addConsensusGroup(TAddConsensusGroupReq registerResp) {
    return configManager.createPeerForConsensusGroup(registerResp.getConfigNodeList());
  }

  @Override
  public TSStatus notifyRegisterSuccess() {
    try {
      SystemPropertiesUtils.storeSystemParameters();
    } catch (IOException e) {
      LOGGER.error("Write confignode-system.properties failed", e);
      return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
    }

    // The initial startup of Non-Seed-ConfigNode finished
    LOGGER.info(
        "{} has successfully started and joined the cluster: {}.",
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeDescriptor.getInstance().getConf().getClusterName());
    return StatusUtils.OK;
  }

  @Override
  public TSStatus restartConfigNode(TConfigNodeRestartReq req) {
    TSStatus status = configManager.restartConfigNode(req);

    // Print log to record the ConfigNode that performs the RegisterConfigNodeRequest
    LOGGER.info("Execute RestartConfigNodeRequest {} with result {}", req, status);

    return status;
  }

  /** For leader to remove ConfigNode configuration in consensus layer */
  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    RemoveConfigNodePlan removeConfigNodePlan = new RemoveConfigNodePlan(configNodeLocation);
    TSStatus status = configManager.removeConfigNode(removeConfigNodePlan);
    // Print log to record the ConfigNode that performs the RemoveConfigNodeRequest
    LOGGER.info("Execute RemoveConfigNodeRequest {} with result {}", configNodeLocation, status);

    return status;
  }

  @Override
  public TSStatus deleteConfigNodePeer(TConfigNodeLocation configNodeLocation) {
    if (!configManager.getNodeManager().getRegisteredConfigNodes().contains(configNodeLocation)) {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
          .setMessage(
              "remove ConsensusGroup failed because the ConfigNode not in current Cluster.");
    }

    ConsensusGroupId groupId = configManager.getConsensusManager().getConsensusGroupId();
    ConsensusGenericResponse resp =
        configManager.getConsensusManager().getConsensusImpl().deletePeer(groupId);
    if (!resp.isSuccess()) {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_ERROR.getStatusCode())
          .setMessage(
              "remove ConsensusGroup failed because internal failure. See other logs for more details");
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage("remove ConsensusGroup success.");
  }

  @Override
  public TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation) {
    return configManager.reportConfigNodeShutdown(configNodeLocation);
  }

  /** Stop ConfigNode */
  @Override
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) {
    new Thread(
            () -> {
              try {
                // Sleep 1s before stop itself
                TimeUnit.SECONDS.sleep(1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn(e.getMessage());
              } finally {
                ConfigNode.getInstance().stop();
              }
            })
        .start();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage("Stop ConfigNode success.");
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) {
    return configManager.createFunction(req);
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) {
    return configManager.dropFunction(req.getUdfName());
  }

  @Override
  public TGetUDFTableResp getUDFTable() {
    return configManager.getUDFTable();
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) {
    return configManager.getUDFJar(req);
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) {
    return configManager.createTrigger(req);
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) {
    return configManager.dropTrigger(req);
  }

  @Override
  public TGetTriggerTableResp getTriggerTable() {
    return configManager.getTriggerTable();
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() {
    return configManager.getStatefulTriggerTable();
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName) {
    return configManager.getLocationOfStatefulTrigger(triggerName);
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) {
    return configManager.getTriggerJar(req);
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) {
    return configManager.createPipePlugin(req);
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginReq req) {
    return configManager.dropPipePlugin(req.getPluginName());
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() {
    return configManager.getPipePluginTable();
  }

  @Override
  public TSStatus merge() throws TException {
    return configManager.merge();
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    if (req.storageGroups != null) {
      List<PartialPath> noExistSg =
          configManager.checkStorageGroupExist(PartialPath.fromStringList(req.storageGroups));
      if (!noExistSg.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        noExistSg.forEach(storageGroup -> sb.append(storageGroup.getFullPath()).append(","));
        return RpcUtils.getStatus(
            TSStatusCode.DATABASE_NOT_EXIST,
            "storageGroup " + sb.subSequence(0, sb.length() - 1) + " does not exist");
      }
    }
    return configManager.flush(req);
  }

  @Override
  public TSStatus clearCache() {
    return configManager.clearCache();
  }

  @Override
  public TSStatus loadConfiguration() {
    return configManager.loadConfiguration();
  }

  @Override
  public TSStatus setSystemStatus(String status) {
    return configManager.setSystemStatus(status);
  }

  @TestOnly
  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) {
    return configManager.setDataNodeStatus(req);
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId) {
    return configManager.killQuery(queryId, dataNodeId);
  }

  @Override
  public TGetDataNodeLocationsResp getRunningDataNodeLocations() {
    return configManager.getRunningDataNodeLocations();
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq showRegionReq) {
    GetRegionInfoListPlan getRegionInfoListPlan = new GetRegionInfoListPlan(showRegionReq);
    RegionInfoListResp dataSet = configManager.showRegion(getRegionInfoListPlan);
    TShowRegionResp showRegionResp = new TShowRegionResp();
    showRegionResp.setStatus(dataSet.getStatus());
    showRegionResp.setRegionInfoList(dataSet.getRegionInfoList());
    return showRegionResp;
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() {
    return configManager.getLatestRegionRouteMap();
  }

  @Override
  public long getConfigNodeHeartBeat(long timestamp) {
    return timestamp;
  }

  @Override
  public TShowDataNodesResp showDataNodes() {
    return configManager.showDataNodes();
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() {
    return configManager.showConfigNodes();
  }

  @Override
  public TShowDatabaseResp showDatabase(List<String> storageGroupPathPattern) {
    return configManager.showStorageGroup(new GetDatabasePlan(storageGroupPathPattern));
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) {
    return configManager.createSchemaTemplate(req);
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() {
    return configManager.getAllTemplates();
  }

  @Override
  public TGetTemplateResp getTemplate(String req) {
    return configManager.getTemplate(req);
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) {
    return configManager.setSchemaTemplate(req);
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String req) {
    return configManager.getPathsSetTemplate(req);
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) {
    return configManager.deactivateSchemaTemplate(req);
  }

  @Override
  public TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) {
    return configManager.unsetSchemaTemplate(req);
  }

  @Override
  public TSStatus dropSchemaTemplate(String req) {
    return configManager.dropSchemaTemplate(req);
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) {
    return configManager.deleteTimeSeries(req);
  }

  @Override
  public TSStatus createPipeSink(TPipeSinkInfo req) {
    return configManager.createPipeSink(new CreatePipeSinkPlan(req));
  }

  @Override
  public TSStatus dropPipeSink(TDropPipeSinkReq req) {
    return configManager.dropPipeSink(new DropPipeSinkPlan(req.getPipeSinkName()));
  }

  @Override
  public TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req) {
    return configManager.getPipeSink(req);
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) {
    return configManager.createPipe(req);
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    return configManager.startPipe(pipeName);
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    return configManager.stopPipe(pipeName);
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    return configManager.dropPipe(pipeName);
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) {
    return configManager.showPipe(req);
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() {
    return configManager.getAllPipeInfo();
  }

  @Override
  public TSStatus recordPipeMessage(TRecordPipeMessageReq req) {
    return configManager.recordPipeMessage(req);
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) {
    if (req.isSetTimeSlotId() && req.getType() != TConsensusGroupType.DataRegion) {
      return new TGetRegionIdResp(new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode()));
    }
    return configManager.getRegionId(req);
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) {
    long startTime = req.isSetStartTime() ? req.getStartTime() : Long.MIN_VALUE;
    long endTime = req.isSetEndTime() ? req.getEndTime() : Long.MAX_VALUE;
    GetTimeSlotListPlan plan =
        new GetTimeSlotListPlan(req.getDatabase(), req.getSeriesSlotId(), startTime, endTime);
    return configManager.getTimeSlotList(plan);
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) {
    TConsensusGroupType type = req.isSetType() ? req.getType() : TConsensusGroupType.ConfigRegion;
    GetSeriesSlotListPlan plan = new GetSeriesSlotListPlan(req.getDatabase(), type);
    return configManager.getSeriesSlotList(plan);
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) {
    return configManager.migrateRegion(req);
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) {
    return configManager.createCQ(req);
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) {
    return configManager.dropCQ(req);
  }

  @Override
  public TShowCQResp showCQ() {
    return configManager.showCQ();
  }

  @Override
  public TSStatus createModel(TCreateModelReq req) throws TException {
    return configManager.createModel(req);
  }

  @Override
  public TSStatus dropModel(TDropModelReq req) throws TException {
    return configManager.dropModel(req);
  }

  @Override
  public TShowModelResp showModel(TShowModelReq req) throws TException {
    return configManager.showModel(req);
  }

  @Override
  public TShowTrailResp showTrail(TShowTrailReq req) throws TException {
    return configManager.showTrail(req);
  }

  @Override
  public TSStatus updateModelInfo(TUpdateModelInfoReq req) throws TException {
    return configManager.updateModelInfo(req);
  }

  @Override
  public TSStatus updateModelState(TUpdateModelStateReq req) throws TException {
    return configManager.updateModelState(req);
  }
}
