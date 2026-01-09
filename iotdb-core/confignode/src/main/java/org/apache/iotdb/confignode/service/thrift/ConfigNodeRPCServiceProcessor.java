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

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TShowAppliedConfigurationsResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowTTLReq;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeSystemPropertiesHandler;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ainode.GetAINodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.ttl.ShowTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.ainode.AINodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.ainode.AINodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.datanode.ConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.auth.AuthorInfo;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterEncodingCompressorReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTableViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAINodeLocationResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TGetUdfTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferResp;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowAINodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTTLResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TTestOperation;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServiceProcessor implements IConfigNodeRPCService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServiceProcessor.class);

  protected final CommonConfig commonConfig;
  protected final ConfigNodeConfig configNodeConfig;
  protected final ConfigNode configNode;
  protected final ConfigManager configManager;

  public ConfigNodeRPCServiceProcessor(ConfigManager configManager) {
    this.commonConfig = CommonDescriptor.getInstance().getConfig();
    this.configNodeConfig = ConfigNodeDescriptor.getInstance().getConf();
    this.configNode = ConfigNode.getInstance();
    this.configManager = configManager;
  }

  public ConfigNodeRPCServiceProcessor(
      CommonConfig commonConfig,
      ConfigNodeConfig configNodeConfig,
      ConfigNode configNode,
      ConfigManager configManager) {
    this.commonConfig = commonConfig;
    this.configNodeConfig = configNodeConfig;
    this.configNode = configNode;
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
  public TGetClusterIdResp getClusterId() {
    TGetClusterIdResp resp = new TGetClusterIdResp();
    String clusterId = configManager.getClusterManager().getClusterId();
    if (clusterId == null) {
      LOGGER.error("clusterId not generated yet, should never happen.");
      return resp.setClusterId("")
          .setStatus(new TSStatus(TSStatusCode.GET_CLUSTER_ID_ERROR.getStatusCode()));
    }
    resp.setClusterId(clusterId).setStatus(RpcUtils.SUCCESS_STATUS);
    LOGGER.info("Execute getClusterId with result {}", resp);
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
  public TAINodeRegisterResp registerAINode(TAINodeRegisterReq req) {
    TAINodeRegisterResp resp =
        ((AINodeRegisterResp) configManager.registerAINode(req)).convertToAINodeRegisterResp();
    LOGGER.info("Execute RegisterAINodeRequest {} with result {}", req, resp);
    return resp;
  }

  @Override
  public TAINodeRestartResp restartAINode(TAINodeRestartReq req) {
    TAINodeRestartResp resp = configManager.restartAINode(req);
    LOGGER.info("Execute RestartAINodeRequest {} with result {}", req, resp);
    return resp;
  }

  @Override
  public TSStatus removeAINode(TAINodeRemoveReq req) {
    LOGGER.info("ConfigNode RPC Service start to remove AINode");
    TSStatus status = configManager.removeAINode();
    LOGGER.info("ConfigNode RPC Service finished to remove AINode, result: {}", status);
    return status;
  }

  @Override
  public TShowAINodesResp showAINodes() throws TException {
    return configManager.showAINodes();
  }

  @Override
  public TAINodeConfigurationResp getAINodeConfiguration(int aiNodeId) throws TException {
    GetAINodeConfigurationPlan getAINodeConfigurationPlan =
        new GetAINodeConfigurationPlan(aiNodeId);
    AINodeConfigurationResp aiNodeConfigurationResp =
        (AINodeConfigurationResp) configManager.getAINodeConfiguration(getAINodeConfigurationPlan);
    TAINodeConfigurationResp resp = new TAINodeConfigurationResp();
    aiNodeConfigurationResp.convertToRpcAINodeLocationResp(resp);
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
  public TShowClusterResp showCluster() {
    return configManager.showCluster();
  }

  @Override
  public TShowVariablesResp showVariables() {
    return configManager.showVariables();
  }

  @Override
  public TSStatus setDatabase(final TDatabaseSchema databaseSchema) {
    final TSStatus setDefaultStatus =
        ClusterSchemaManager.enrichDatabaseSchemaWithDefaultProperties(databaseSchema);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != setDefaultStatus.getCode()) {
      return setDefaultStatus;
    }

    final DatabaseSchemaPlan setPlan =
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, databaseSchema);
    final TSStatus resp = configManager.setDatabase(setPlan);

    // Print log to record the ConfigNode that performs the set SetDatabaseRequest
    LOGGER.info("Execute SetDatabase: {} with result: {}", databaseSchema, resp);

    return resp;
  }

  @Override
  public TSStatus alterDatabase(final TDatabaseSchema databaseSchema) {
    TSStatus errorResp = null;

    // TODO: Support alter the following fields
    if (databaseSchema.isSetTTL() && !databaseSchema.isIsTableModel()) {
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

    if (databaseSchema.isSetTimePartitionOrigin()) {
      errorResp =
          new TSStatus(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode())
              .setMessage(
                  "Failed to alter database. Doesn't support ALTER TimePartitionOrigin yet.");
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

    final DatabaseSchemaPlan alterPlan =
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, databaseSchema);
    final TSStatus resp = configManager.alterDatabase(alterPlan);

    // Print log to record the ConfigNode that performs the set SetDatabaseRequest
    LOGGER.info("Execute AlterDatabase: {} with result: {}", databaseSchema, resp);

    return resp;
  }

  @Override
  public TSStatus deleteDatabase(final TDeleteDatabaseReq tDeleteReq) {
    return configManager.deleteDatabases(
        new TDeleteDatabasesReq(Collections.singletonList(tDeleteReq.getPrefixPath()))
            .setIsGeneratedByPipe(tDeleteReq.isIsGeneratedByPipe()));
  }

  @Override
  public TSStatus deleteDatabases(final TDeleteDatabasesReq tDeleteReq) {
    return configManager.deleteDatabases(tDeleteReq);
  }

  @Override
  public TSStatus setTTL(final TSetTTLReq req) throws TException {
    return configManager.setTTL(new SetTTLPlan(req.getPathPattern(), req.getTTL()));
  }

  @Override
  public TSStatus setSchemaReplicationFactor(final TSetSchemaReplicationFactorReq req)
      throws TException {
    return configManager.setSchemaReplicationFactor(
        new SetSchemaReplicationFactorPlan(req.getDatabase(), req.getSchemaReplicationFactor()));
  }

  @Override
  public TSStatus setDataReplicationFactor(final TSetDataReplicationFactorReq req)
      throws TException {
    return configManager.setDataReplicationFactor(
        new SetDataReplicationFactorPlan(req.getDatabase(), req.getDataReplicationFactor()));
  }

  @Override
  public TSStatus setTimePartitionInterval(final TSetTimePartitionIntervalReq req)
      throws TException {
    return configManager.setTimePartitionInterval(
        new SetTimePartitionIntervalPlan(req.getDatabase(), req.getTimePartitionInterval()));
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(final TGetDatabaseReq req) {
    final PathPatternTree scope =
        req.getScopePatternTree() == null
            ? SchemaConstant.ALL_MATCH_SCOPE
            : PathPatternTree.deserialize(ByteBuffer.wrap(req.getScopePatternTree()));
    final CountDatabasePlan plan =
        new CountDatabasePlan(
            req.getDatabasePathPattern(),
            scope,
            req.isSetIsTableModel() && req.isIsTableModel(),
            !req.isSetCanSeeAuditDB() || req.isCanSeeAuditDB());
    final CountDatabaseResp countDatabaseResp =
        (CountDatabaseResp) configManager.countMatchedDatabases(plan);

    final TCountDatabaseResp resp = new TCountDatabaseResp();
    countDatabaseResp.convertToRPCCountStorageGroupResp(resp);
    return resp;
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(final TGetDatabaseReq req) {
    final PathPatternTree scope =
        req.getScopePatternTree() == null
            ? SchemaConstant.ALL_MATCH_SCOPE
            : PathPatternTree.deserialize(ByteBuffer.wrap(req.getScopePatternTree()));
    final GetDatabasePlan plan =
        new GetDatabasePlan(
            req.getDatabasePathPattern(),
            scope,
            req.isSetIsTableModel() && req.isIsTableModel(),
            false,
            !req.isSetCanSeeAuditDB() || req.isCanSeeAuditDB());
    final DatabaseSchemaResp databaseSchemaResp =
        (DatabaseSchemaResp) configManager.getMatchedDatabaseSchemas(plan);

    return databaseSchemaResp.convertToRPCStorageGroupSchemaResp();
  }

  @Override
  public TShowTTLResp showTTL(TShowTTLReq req) {
    ShowTTLResp showTTLResp =
        (ShowTTLResp)
            configManager.showTTL(new ShowTTLPlan(req.getPathPattern().toArray(new String[0])));
    return showTTLResp.convertToRPCTShowTTLResp();
  }

  public TSStatus callSpecialProcedure(TTestOperation operation) {
    switch (operation) {
      case TEST_PROCEDURE_RECOVER:
        return configManager.getProcedureManager().createManyDatabases();
      case TEST_SUB_PROCEDURE:
        return configManager.getProcedureManager().testSubProcedure();
      default:
        String msg = String.format("operation %s is not supported", operation);
        LOGGER.error(msg);
        throw new UnsupportedOperationException(msg);
    }
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(final TSchemaPartitionReq req) {
    return configManager.getSchemaPartition(
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree())), true);
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTableWithSlots(
      final Map<String, List<TSeriesPartitionSlot>> dbSlotMap) {
    return configManager.getSchemaPartition(dbSlotMap);
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(final TSchemaPartitionReq req) {
    return configManager.getOrCreateSchemaPartition(
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree())));
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTableWithSlots(
      final Map<String, List<TSeriesPartitionSlot>> dbSlotMap) {
    return configManager.getOrCreateSchemaPartition(dbSlotMap);
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req) {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    PathPatternTree scope =
        req.getScopePatternTree() == null
            ? SchemaConstant.ALL_MATCH_SCOPE
            : PathPatternTree.deserialize(ByteBuffer.wrap(req.getScopePatternTree()));
    PartialPath partialPath = patternTree.getAllPathPatterns().get(0);
    return configManager.getNodePathsPartition(
        partialPath, scope, req.getLevel(), !req.isSetNeedAuditDB() || req.isNeedAuditDB());
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
  public TSStatus operatePermission(final TAuthorizerReq req) {
    ConfigPhysicalPlanType configPhysicalPlanType =
        AuthorInfo.getConfigPhysicalPlanTypeFromAuthorType(req.getAuthorType());
    return configManager.operatePermission(
        new AuthorTreePlan(
            configPhysicalPlanType,
            req.getUserName(),
            req.getRoleName(),
            req.getPassword(),
            req.getNewPassword(),
            req.getPermissions(),
            req.isGrantOpt(),
            AuthUtils.deserializePartialPathList(ByteBuffer.wrap(req.getNodeNameList())),
            req.getExecutedByUserID(),
            req.getNewUsername()));
  }

  @Override
  public TGetAINodeLocationResp getAINodeLocation() throws TException {
    final TGetAINodeLocationResp resp = new TGetAINodeLocationResp();
    final TSStatus status = new TSStatus();
    try {
      final List<TAINodeConfiguration> registeredAINodes =
          configManager.getNodeManager().getRegisteredAINodes();

      if (registeredAINodes == null || registeredAINodes.isEmpty()) {
        status.setCode(TSStatusCode.NO_REGISTERED_AI_NODE_ERROR.getStatusCode());
        status.setMessage("No registered AINode found");
        resp.setStatus(status);
        return resp;
      }

      final TAINodeLocation loc = registeredAINodes.get(0).getLocation();
      resp.setAiNodeLocation(loc);
      status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.setMessage("AINode location resolved");

    } catch (Exception e) {
      status.setCode(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage("getAINodeLocation failed: " + e.getMessage());
    }
    resp.setStatus(status);
    return resp;
  }

  @Override
  public TAuthorizerResp queryPermission(final TAuthorizerReq req) {
    final PermissionInfoResp dataSet =
        (PermissionInfoResp)
            configManager.queryPermission(
                new AuthorTreePlan(
                    AuthorInfo.getConfigPhysicalPlanTypeFromAuthorType(req.getAuthorType()),
                    req.getUserName(),
                    req.getRoleName(),
                    req.getPassword(),
                    req.getNewPassword(),
                    req.getPermissions(),
                    req.isGrantOpt(),
                    AuthUtils.deserializePartialPathList(ByteBuffer.wrap(req.getNodeNameList())),
                    req.getExecutedByUserID(),
                    req.getNewUsername()));
    final TAuthorizerResp resp = new TAuthorizerResp(dataSet.getStatus());
    resp.setMemberInfo(dataSet.getMemberList());
    resp.setPermissionInfo(dataSet.getPermissionInfoResp());
    resp.setTag(dataSet.getTag());
    resp.setUsersInfo(dataSet.getUsersInfo());
    return resp;
  }

  @Override
  public TSStatus operateRPermission(final TAuthorizerRelationalReq req) {
    if (req.getAuthorType() < 0 || req.getAuthorType() >= AuthorRType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    ConfigPhysicalPlanType configPhysicalPlanType =
        AuthorInfo.getConfigPhysicalPlanTypeFromAuthorRType(req.getAuthorType());
    return configManager.operatePermission(
        new AuthorRelationalPlan(
            configPhysicalPlanType,
            req.getUserName(),
            req.getRoleName(),
            req.getDatabase(),
            req.getTable(),
            req.getPermissions(),
            req.isGrantOpt(),
            req.getPassword(),
            req.getExecutedByUserID(),
            req.getNewUsername()));
  }

  @Override
  public TAuthorizerResp queryRPermission(final TAuthorizerRelationalReq req) {
    if (req.getAuthorType() < 0 || req.getAuthorType() >= AuthorRType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    final PermissionInfoResp dataSet =
        (PermissionInfoResp)
            configManager.queryPermission(
                new AuthorRelationalPlan(
                    AuthorInfo.getConfigPhysicalPlanTypeFromAuthorRType(req.getAuthorType()),
                    req.getUserName(),
                    req.getRoleName(),
                    req.getDatabase(),
                    req.getTable(),
                    req.getPermissions(),
                    req.isGrantOpt(),
                    req.getPassword(),
                    req.getExecutedByUserID(),
                    ""));
    final TAuthorizerResp resp = new TAuthorizerResp(dataSet.getStatus());
    resp.setMemberInfo(dataSet.getMemberList());
    resp.setPermissionInfo(dataSet.getPermissionInfoResp());
    resp.setUsersInfo(dataSet.getUsersInfo());
    resp.setTag(dataSet.getTag());
    return resp;
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) {
    return configManager.login(req.getUserrname(), req.getPassword());
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) {
    PrivilegeModelType reqType = PrivilegeModelType.values()[req.getReqtype()];
    PrivilegeType permission;
    // permission = -1. check object's visible.
    if (req.getPermission() == -1) {
      permission = null;
    } else {
      permission = PrivilegeType.values()[req.getPermission()];
    }
    switch (reqType) {
      case TREE:
        List<PartialPath> partialPaths =
            AuthUtils.deserializePartialPathList(ByteBuffer.wrap(req.getPaths()));
        return configManager.checkUserPrivileges(
            req.getUsername(), new PrivilegeUnion(partialPaths, permission, req.isGrantOpt()));
      case SYSTEM:
        return configManager.checkUserPrivileges(
            req.getUsername(), new PrivilegeUnion(permission, req.isGrantOpt()));
      case RELATIONAL:
        PrivilegeUnion union;
        if (!req.isSetDatabase() && !req.isSetTable()) {
          union = new PrivilegeUnion(permission, req.isGrantOpt(), true);
        } else if (req.isSetTable()) {
          union =
              new PrivilegeUnion(req.getDatabase(), req.getTable(), permission, req.isGrantOpt());
        } else {
          union = new PrivilegeUnion(req.getDatabase(), permission, req.isGrantOpt());
        }
        return configManager.checkUserPrivileges(req.getUsername(), union);
      default:
        return AuthUtils.generateEmptyPermissionInfoResp();
    }
  }

  @Override
  public TAuthizedPatternTreeResp fetchAuthizedPatternTree(TCheckUserPrivilegesReq req) {
    return configManager.fetchAuthizedPatternTree(req.getUsername(), req.getPermission());
  }

  @Override
  public TPermissionInfoResp checkRoleOfUser(TAuthorizerReq req) {
    return configManager.checkRoleOfUser(req.getUserName(), req.getRoleName());
  }

  @Override
  public TPermissionInfoResp getUser(String userName) {
    return configManager.getUser(userName);
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
        configNodeConfig.getClusterName());
    return StatusUtils.OK;
  }

  /** For leader to remove ConfigNode configuration in consensus layer */
  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    RemoveConfigNodePlan removeConfigNodePlan = new RemoveConfigNodePlan(configNodeLocation);
    TSStatus status = configManager.removeConfigNode(removeConfigNodePlan);
    // Print log to record the ConfigNode that performs the RemoveConfigNodeRequest
    LOGGER.info(
        "The result of submitting RemoveConfigNode job is {}. RemoveConfigNodeRequest: {}",
        status,
        configNodeLocation);

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
    try {
      configManager.getConsensusManager().getConsensusImpl().deleteLocalPeer(groupId);
    } catch (ConsensusException e) {
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
  public TSStatus stopAndClearConfigNode(TConfigNodeLocation configNodeLocation) {
    new Thread(
            // TODO: Perhaps we should find some other way of shutting down the config node, adding
            // a hard dependency
            //  in order to do this feels a bit odd. Dispatching a shutdown event which is processed
            // where the
            //  instance is created feels cleaner.
            () -> {
              try {
                // Sleep 5s before stop itself
                TimeUnit.SECONDS.sleep(5);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn(e.getMessage());
              } finally {
                ConfigNodeSystemPropertiesHandler.getInstance().delete();
                configNode.stop();
              }
            })
        .start();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage("Stop And Clear ConfigNode Success.");
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) {
    return configManager.createFunction(req);
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) {
    return configManager.dropFunction(req);
  }

  @Override
  public TGetUDFTableResp getUDFTable(TGetUdfTableReq req) {
    return configManager.getUDFTable(req);
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
    return configManager.dropPipePlugin(req);
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() {
    return configManager.getPipePluginTable();
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTableExtended(TShowPipePluginReq req) {
    return configManager.getPipePluginTableExtended(req);
  }

  @Override
  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) {
    return configManager.getPipePluginJar(req);
  }

  @Override
  public TSStatus merge() throws TException {
    return configManager.merge();
  }

  @Override
  public TSStatus flush(final TFlushReq req) throws TException {
    if (req.storageGroups != null) {
      final List<String> noExistSg =
          configManager.getPartitionManager().filterUnExistDatabases(req.storageGroups);
      if (!noExistSg.isEmpty()) {
        final StringBuilder sb = new StringBuilder();
        noExistSg.forEach(storageGroup -> sb.append(storageGroup).append(","));
        return RpcUtils.getStatus(
            TSStatusCode.DATABASE_NOT_EXIST,
            "Database " + sb.subSequence(0, sb.length() - 1) + " does not exist");
      }
    }
    return configManager.flush(req);
  }

  @Override
  public TSStatus clearCache(final Set<Integer> clearCacheOptions) {
    return configManager.clearCache(clearCacheOptions);
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) {
    return configManager.setConfiguration(req);
  }

  @Override
  public TSStatus startRepairData() {
    return configManager.startRepairData();
  }

  @Override
  public TSStatus stopRepairData() throws TException {
    return configManager.stopRepairData();
  }

  @Override
  public TSStatus submitLoadConfigurationTask() throws TException {
    return configManager.submitLoadConfigurationTask();
  }

  @Override
  public TSStatus loadConfiguration() {
    return configManager.loadConfiguration();
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId) throws TException {
    return configManager.showConfiguration(nodeId);
  }

  @Override
  public TShowAppliedConfigurationsResp showAppliedConfigurations(int nodeId) throws TException {
    return configManager.showAppliedConfigurations(nodeId);
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
  public TSStatus killQuery(String queryId, int dataNodeId, String allowedUsername) {
    return configManager.killQuery(queryId, dataNodeId, allowedUsername);
  }

  @Override
  public TGetDataNodeLocationsResp getReadableDataNodeLocations() {
    return configManager.getReadableDataNodeLocations();
  }

  @Override
  public TShowRegionResp showRegion(final TShowRegionReq showRegionReq) {
    final GetRegionInfoListPlan getRegionInfoListPlan = new GetRegionInfoListPlan(showRegionReq);
    final RegionInfoListResp dataSet = configManager.showRegion(getRegionInfoListPlan);
    final TShowRegionResp showRegionResp = new TShowRegionResp();
    showRegionResp.setStatus(dataSet.getStatus());
    showRegionResp.setRegionInfoList(dataSet.getRegionInfoList());
    return showRegionResp;
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() {
    return configManager.getLatestRegionRouteMap();
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq heartbeatReq) {
    TConfigNodeHeartbeatResp resp = new TConfigNodeHeartbeatResp();
    resp.setTimestamp(heartbeatReq.getTimestamp());
    return resp;
  }

  @Override
  public TShowDataNodesResp showDataNodes() {
    return configManager.showDataNodes();
  }

  @Override
  public TShowDataNodes4InformationSchemaResp showDataNodes4InformationSchema() {
    return configManager.showDataNodes4InformationSchema();
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() {
    return configManager.showConfigNodes();
  }

  @Override
  public TShowConfigNodes4InformationSchemaResp showConfigNodes4InformationSchema() {
    return configManager.showConfigNodes4InformationSchema();
  }

  @Override
  public TShowDatabaseResp showDatabase(TGetDatabaseReq req) {
    return configManager.showDatabase(req);
  }

  /** Call by ConfigNode leader */
  @Override
  public TTestConnectionResp submitTestConnectionTask(TNodeLocations nodeLocations)
      throws TException {
    return configManager.getClusterManager().doConnectionTest(nodeLocations);
  }

  /** Call by client connected DataNode */
  @Override
  public TTestConnectionResp submitTestConnectionTaskToLeader() throws TException {
    return configManager.getClusterManager().submitTestConnectionTaskToEveryNode();
  }

  /** Call by every other nodes */
  @Override
  public TSStatus testConnectionEmptyRPC() throws TException {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
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
  public TGetPathsSetTemplatesResp getPathsSetTemplate(TGetPathsSetTemplatesReq req) {
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
  public TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req) {
    return configManager.alterSchemaTemplate(req);
  }

  @Override
  public TSStatus alterEncodingCompressor(final TAlterEncodingCompressorReq req) throws TException {
    return configManager.alterEncodingCompressor(req);
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) {
    return configManager.deleteTimeSeries(req);
  }

  @Override
  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) {
    return configManager.deleteLogicalView(req);
  }

  @Override
  public TSStatus alterLogicalView(TAlterLogicalViewReq req) {
    return configManager.alterLogicalView(req);
  }

  @Override
  public TSStatus alterTimeSeriesDataType(final TAlterTimeSeriesReq req) {
    return configManager.alterTimeSeriesDataType(req);
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) {
    return configManager.createPipe(req);
  }

  @Override
  public TSStatus alterPipe(TAlterPipeReq req) {
    return configManager.alterPipe(req);
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    return configManager.startPipe(new TStartPipeReq().setPipeName(pipeName));
  }

  @Override
  public TSStatus startPipeExtended(TStartPipeReq req) {
    return configManager.startPipe(req);
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    return configManager.stopPipe(new TStopPipeReq().setPipeName(pipeName));
  }

  @Override
  public TSStatus stopPipeExtended(TStopPipeReq req) {
    return configManager.stopPipe(req);
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    return configManager.dropPipe(
        new TDropPipeReq().setPipeName(pipeName).setIfExistsCondition(false));
  }

  @Override
  public TSStatus dropPipeExtended(TDropPipeReq req) {
    return configManager.dropPipe(req);
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
  public TPipeConfigTransferResp handleTransferConfigPlan(TPipeConfigTransferReq req) {
    return configManager.handleTransferConfigPlan(req);
  }

  @Override
  public TSStatus handlePipeConfigClientExit(String clientId) {
    return configManager.handleClientExit(clientId);
  }

  @Override
  public TSStatus createTopic(TCreateTopicReq req) {
    return configManager.createTopic(req);
  }

  @Override
  public TSStatus dropTopic(String topicName) {
    return configManager.dropTopic(
        new TDropTopicReq().setTopicName(topicName).setIfExistsCondition(false));
  }

  @Override
  public TSStatus dropTopicExtended(TDropTopicReq req) throws TException {
    return configManager.dropTopic(req);
  }

  @Override
  public TShowTopicResp showTopic(TShowTopicReq req) {
    return configManager.showTopic(req);
  }

  @Override
  public TGetAllTopicInfoResp getAllTopicInfo() {
    return configManager.getAllTopicInfo();
  }

  @Override
  public TSStatus createConsumer(TCreateConsumerReq req) {
    return configManager.createConsumer(req);
  }

  @Override
  public TSStatus closeConsumer(TCloseConsumerReq req) {
    return configManager.closeConsumer(req);
  }

  @Override
  public TSStatus createSubscription(TSubscribeReq req) {
    return configManager.createSubscription(req);
  }

  @Override
  public TSStatus dropSubscription(TUnsubscribeReq req) {
    return configManager.dropSubscription(req);
  }

  @Override
  public TSStatus dropSubscriptionById(TDropSubscriptionReq req) {
    return configManager.dropSubscriptionById(req);
  }

  @Override
  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) {
    return configManager.showSubscription(req);
  }

  @Override
  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() {
    return configManager.getAllSubscriptionInfo();
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) {
    return configManager.getRegionId(req);
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) {
    return configManager.getTimeSlotList(req);
  }

  @Override
  public TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) {
    return configManager.countTimeSlotList(req);
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) {
    return configManager.getSeriesSlotList(req);
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) {
    return configManager.migrateRegion(req);
  }

  @Override
  public TSStatus reconstructRegion(TReconstructRegionReq req) {
    return configManager.reconstructRegion(req);
  }

  @Override
  public TSStatus extendRegion(TExtendRegionReq req) throws TException {
    return configManager.extendRegion(req);
  }

  @Override
  public TSStatus removeRegion(TRemoveRegionReq req) throws TException {
    return configManager.removeRegion(req);
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
  public TSStatus createExternalService(TCreateExternalServiceReq req) {
    return configManager.createExternalService(req);
  }

  @Override
  public TSStatus startExternalService(int dataNodeId, String serviceName) {
    return configManager.startExternalService(dataNodeId, serviceName);
  }

  @Override
  public TSStatus stopExternalService(int dataNodeId, String serviceName) {
    return configManager.stopExternalService(dataNodeId, serviceName);
  }

  @Override
  public TSStatus dropExternalService(int dataNodeId, String serviceName) {
    return configManager.dropExternalService(dataNodeId, serviceName);
  }

  @Override
  public TExternalServiceListResp showExternalService(int dataNodeId) {
    return configManager.showExternalService(dataNodeId);
  }

  @Override
  public TSStatus setSpaceQuota(final TSetSpaceQuotaReq req) throws TException {
    return configManager.setSpaceQuota(req);
  }

  @Override
  public TSpaceQuotaResp showSpaceQuota(List<String> databases) {
    return configManager.showSpaceQuota(databases);
  }

  @Override
  public TSpaceQuotaResp getSpaceQuota() {
    return configManager.getSpaceQuota();
  }

  @Override
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) throws TException {
    return configManager.setThrottleQuota(req);
  }

  @Override
  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) {
    return configManager.showThrottleQuota(req);
  }

  @Override
  public TThrottleQuotaResp getThrottleQuota() {
    return configManager.getThrottleQuota();
  }

  @Override
  public TSStatus createTable(final ByteBuffer tableInfo) {
    return configManager.createTable(tableInfo);
  }

  @Override
  public TSStatus alterOrDropTable(final TAlterOrDropTableReq req) {
    return configManager.alterOrDropTable(req);
  }

  @Override
  public TShowTableResp showTables(final String database, final boolean isDetails) {
    return configManager.showTables(database, isDetails);
  }

  @Override
  public TShowTable4InformationSchemaResp showTables4InformationSchema() {
    return configManager.showTables4InformationSchema();
  }

  @Override
  public TDescTableResp describeTable(
      final String database, final String tableName, final boolean isDetails) {
    return configManager.describeTable(database, tableName, isDetails);
  }

  @Override
  public TDescTable4InformationSchemaResp descTables4InformationSchema() {
    return configManager.describeTable4InformationSchema();
  }

  @Override
  public TFetchTableResp fetchTables(final Map<String, Set<String>> fetchTableMap) {
    return configManager.fetchTables(fetchTableMap);
  }

  @Override
  public TDeleteTableDeviceResp deleteDevice(final TDeleteTableDeviceReq req) {
    return configManager.deleteDevice(req);
  }

  @Override
  public TSStatus createTableView(final TCreateTableViewReq req) {
    return configManager.createTableView(req);
  }

  @Override
  public TSStatus pushHeartbeat(final int dataNodeId, final TPipeHeartbeatResp resp) {
    return configManager.pushHeartbeat(dataNodeId, resp);
  }
}
