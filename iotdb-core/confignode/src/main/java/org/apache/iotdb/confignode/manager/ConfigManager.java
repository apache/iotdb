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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.database.CountDatabaseResp;
import org.apache.iotdb.confignode.consensus.response.database.DatabaseSchemaResp;
import org.apache.iotdb.confignode.consensus.response.datanode.ConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.consensus.response.partition.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.template.TemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.statemachine.ConfigRegionStateMachine;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.ClusterNodeStartUtils;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.node.NodeMetrics;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.manager.pipe.PipeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaQuotaStatistics;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TClusterParameters;
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
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateAlterOperationType;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateAlterOperationUtil;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/** Entry of all management, AssignPartitionManager, AssignRegionManager. */
public class ConfigManager implements IManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONF = CommonDescriptor.getInstance().getConfig();

  /** Manage PartitionTable read/write requests through the ConsensusLayer. */
  private final AtomicReference<ConsensusManager> consensusManager = new AtomicReference<>();

  /** Manage cluster node. */
  private final NodeManager nodeManager;

  /** Manage cluster schemaengine. */
  private final ClusterSchemaManager clusterSchemaManager;

  /** Manage cluster regions and partitions. */
  private final PartitionManager partitionManager;

  /** Manage cluster authorization. */
  private final PermissionManager permissionManager;

  private final LoadManager loadManager;

  /** Manage procedure. */
  private final ProcedureManager procedureManager;

  /** UDF. */
  private final UDFManager udfManager;

  /** Manage Trigger. */
  private final TriggerManager triggerManager;

  /** CQ. */
  private final CQManager cqManager;

  /** ML Model. */
  private final ModelManager modelManager;

  /** Pipe */
  private final PipeManager pipeManager;

  /** Manage quotas */
  private final ClusterQuotaManager clusterQuotaManager;

  private final ConfigRegionStateMachine stateMachine;

  private final RetryFailedTasksThread retryFailedTasksThread;

  private static final String DATABASE = "\tDatabase=";

  public ConfigManager() throws IOException {
    // Build the persistence module
    NodeInfo nodeInfo = new NodeInfo();
    ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    PartitionInfo partitionInfo = new PartitionInfo();
    AuthorInfo authorInfo = new AuthorInfo();
    ProcedureInfo procedureInfo = new ProcedureInfo();
    UDFInfo udfInfo = new UDFInfo();
    TriggerInfo triggerInfo = new TriggerInfo();
    CQInfo cqInfo = new CQInfo();
    ModelInfo modelInfo = new ModelInfo();
    PipeInfo pipeInfo = new PipeInfo();
    QuotaInfo quotaInfo = new QuotaInfo();

    // Build state machine and executor
    ConfigPlanExecutor executor =
        new ConfigPlanExecutor(
            nodeInfo,
            clusterSchemaInfo,
            partitionInfo,
            authorInfo,
            procedureInfo,
            udfInfo,
            triggerInfo,
            cqInfo,
            modelInfo,
            pipeInfo,
            quotaInfo);
    this.stateMachine = new ConfigRegionStateMachine(this, executor);

    // Build the manager module
    this.nodeManager = new NodeManager(this, nodeInfo);
    this.clusterSchemaManager =
        new ClusterSchemaManager(this, clusterSchemaInfo, new ClusterSchemaQuotaStatistics());
    this.partitionManager = new PartitionManager(this, partitionInfo);
    this.permissionManager = new PermissionManager(this, authorInfo);
    this.procedureManager = new ProcedureManager(this, procedureInfo);
    this.udfManager = new UDFManager(this, udfInfo);
    this.triggerManager = new TriggerManager(this, triggerInfo);
    this.cqManager = new CQManager(this);
    this.modelManager = new ModelManager(this, modelInfo);
    this.pipeManager = new PipeManager(this, pipeInfo);

    // 1. keep PipeManager initialization before LoadManager initialization, because
    // LoadManager will register PipeManager as a listener.
    // 2. keep RetryFailedTasksThread initialization after LoadManager initialization,
    // because RetryFailedTasksThread will keep a reference of LoadManager.
    this.loadManager = new LoadManager(this);

    this.retryFailedTasksThread = new RetryFailedTasksThread(this);
    this.clusterQuotaManager = new ClusterQuotaManager(this, quotaInfo);
  }

  public void initConsensusManager() throws IOException {
    this.consensusManager.set(new ConsensusManager(this, this.stateMachine));
  }

  public void close() throws IOException {
    if (consensusManager.get() != null) {
      consensusManager.get().close();
    }
    if (partitionManager != null) {
      partitionManager.getRegionMaintainer().shutdown();
    }
    if (procedureManager != null) {
      procedureManager.shiftExecutor(false);
    }
  }

  @Override
  public DataSet getSystemConfiguration() {
    TSStatus status = confirmLeader();
    ConfigurationResp dataSet;
    // Notice: The Seed-ConfigNode must also have the privilege to give system configuration.
    // Otherwise, the IoTDB-cluster will not have the ability to restart from scratch.
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || ConfigNodeDescriptor.getInstance().isSeedConfigNode()
        || SystemPropertiesUtils.isSeedConfigNode()) {
      dataSet = (ConfigurationResp) nodeManager.getSystemConfiguration();
    } else {
      dataSet = new ConfigurationResp();
      dataSet.setStatus(status);
    }
    return dataSet;
  }

  @Override
  public DataSet registerDataNode(TDataNodeRegisterReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status =
          ClusterNodeStartUtils.confirmNodeRegistration(
              NodeType.DataNode,
              req.getClusterName(),
              req.getDataNodeConfiguration().getLocation(),
              this);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return nodeManager.registerDataNode(req);
      }
    }
    DataNodeRegisterResp resp = new DataNodeRegisterResp();
    resp.setStatus(status);
    resp.setConfigNodeList(getNodeManager().getRegisteredConfigNodes());
    return resp;
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status =
          ClusterNodeStartUtils.confirmNodeRestart(
              NodeType.DataNode,
              req.getClusterName(),
              req.getDataNodeConfiguration().getLocation().getDataNodeId(),
              req.getDataNodeConfiguration().getLocation(),
              this);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return nodeManager.updateDataNodeIfNecessary(req);
      }
    }
    return new TDataNodeRestartResp()
        .setStatus(status)
        .setConfigNodeList(getNodeManager().getRegisteredConfigNodes());
  }

  @Override
  public DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.removeDataNode(removeDataNodePlan);
    } else {
      DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Force updating the target DataNode's status to Unknown
      getLoadManager()
          .forceUpdateNodeCache(
              NodeType.DataNode,
              dataNodeLocation.getDataNodeId(),
              NodeHeartbeatSample.generateDefaultSample(NodeStatus.Unknown));
      LOGGER.info(
          "[ShutdownHook] The DataNode-{} will be shutdown soon, mark it as Unknown",
          dataNodeLocation.getDataNodeId());
    }
    return status;
  }

  @Override
  public TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      procedureManager.reportRegionMigrateResult(req);
    }
    return status;
  }

  @Override
  public DataSet getDataNodeConfiguration(
      GetDataNodeConfigurationPlan getDataNodeConfigurationPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.getDataNodeConfiguration(getDataNodeConfigurationPlan);
    } else {
      DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TShowClusterResp showCluster() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<TConfigNodeLocation> configNodeLocations = getNodeManager().getRegisteredConfigNodes();
      configNodeLocations.sort(Comparator.comparingInt(TConfigNodeLocation::getConfigNodeId));
      List<TDataNodeLocation> dataNodeInfoLocations =
          getNodeManager().getRegisteredDataNodes().stream()
              .map(TDataNodeConfiguration::getLocation)
              .sorted(Comparator.comparingInt(TDataNodeLocation::getDataNodeId))
              .collect(Collectors.toList());
      Map<Integer, String> nodeStatus = getLoadManager().getNodeStatusWithReason();
      Map<Integer, TNodeVersionInfo> nodeVersionInfo = getNodeManager().getNodeVersionInfo();
      return new TShowClusterResp(
          status, configNodeLocations, dataNodeInfoLocations, nodeStatus, nodeVersionInfo);
    } else {
      return new TShowClusterResp(
          status, new ArrayList<>(), new ArrayList<>(), new HashMap<>(), new HashMap<>());
    }
  }

  @Override
  public TShowVariablesResp showVariables() {
    TSStatus status = confirmLeader();
    TShowVariablesResp resp = new TShowVariablesResp();
    resp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setClusterParameters(getClusterParameters());
    }
    return resp;
  }

  public TClusterParameters getClusterParameters() {
    TClusterParameters clusterParameters = new TClusterParameters();
    clusterParameters.setClusterName(CONF.getClusterName());
    clusterParameters.setConfigNodeConsensusProtocolClass(
        CONF.getConfigNodeConsensusProtocolClass());
    clusterParameters.setDataRegionConsensusProtocolClass(
        CONF.getDataRegionConsensusProtocolClass());
    clusterParameters.setSchemaRegionConsensusProtocolClass(
        CONF.getSchemaRegionConsensusProtocolClass());
    clusterParameters.setSeriesPartitionSlotNum(CONF.getSeriesSlotNum());
    clusterParameters.setSeriesPartitionExecutorClass(CONF.getSeriesPartitionExecutorClass());
    clusterParameters.setDefaultTTL(COMMON_CONF.getDefaultTTLInMs());
    clusterParameters.setTimePartitionInterval(COMMON_CONF.getTimePartitionInterval());
    clusterParameters.setDataReplicationFactor(CONF.getDataReplicationFactor());
    clusterParameters.setSchemaReplicationFactor(CONF.getSchemaReplicationFactor());
    clusterParameters.setDataRegionPerDataNode(CONF.getDataRegionPerDataNode());
    clusterParameters.setSchemaRegionPerDataNode(CONF.getSchemaRegionPerDataNode());
    clusterParameters.setDiskSpaceWarningThreshold(COMMON_CONF.getDiskSpaceWarningThreshold());
    clusterParameters.setReadConsistencyLevel(CONF.getReadConsistencyLevel());
    clusterParameters.setTimestampPrecision(COMMON_CONF.getTimestampPrecision());
    clusterParameters.setSchemaEngineMode(COMMON_CONF.getSchemaEngineMode());
    clusterParameters.setTagAttributeTotalSize(COMMON_CONF.getTagAttributeTotalSize());
    clusterParameters.setDatabaseLimitThreshold(COMMON_CONF.getDatabaseLimitThreshold());
    return clusterParameters;
  }

  @Override
  public TSStatus setTTL(SetTTLPlan setTTLPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTTL(setTTLPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorPlan setSchemaReplicationFactorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setSchemaReplicationFactor(setSchemaReplicationFactorPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorPlan setDataReplicationFactorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setDataReplicationFactor(setDataReplicationFactorPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalPlan setTimePartitionIntervalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTimePartitionInterval(setTimePartitionIntervalPlan);
    } else {
      return status;
    }
  }

  @Override
  public DataSet countMatchedDatabases(CountDatabasePlan countDatabasePlan) {
    TSStatus status = confirmLeader();
    CountDatabaseResp result = new CountDatabaseResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.countMatchedDatabases(countDatabasePlan);
    } else {
      result.setStatus(status);
    }
    return result;
  }

  @Override
  public DataSet getMatchedDatabaseSchemas(GetDatabasePlan getDatabaseReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getMatchedDatabaseSchema(getDatabaseReq);
    } else {
      DatabaseSchemaResp dataSet = new DatabaseSchemaResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public synchronized TSStatus setDatabase(DatabaseSchemaPlan databaseSchemaPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setDatabase(databaseSchemaPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus alterDatabase(DatabaseSchemaPlan databaseSchemaPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.alterDatabase(databaseSchemaPlan);
    } else {
      return status;
    }
  }

  @Override
  public synchronized TSStatus deleteDatabases(List<String> deletedPaths) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // remove wild
      Map<String, TDatabaseSchema> deleteDatabaseSchemaMap =
          getClusterSchemaManager().getMatchedDatabaseSchemasByName(deletedPaths);
      if (deleteDatabaseSchemaMap.isEmpty()) {
        return RpcUtils.getStatus(
            TSStatusCode.PATH_NOT_EXIST.getStatusCode(),
            String.format("Path %s does not exist", Arrays.toString(deletedPaths.toArray())));
      }
      ArrayList<TDatabaseSchema> parsedDeleteDatabases =
          new ArrayList<>(deleteDatabaseSchemaMap.values());
      return procedureManager.deleteDatabases(parsedDeleteDatabases);
    } else {
      return status;
    }
  }

  private List<TSeriesPartitionSlot> calculateRelatedSlot(PartialPath path, PartialPath database) {
    // The path contains `**`
    if (path.getFullPath().contains(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    List<PartialPath> innerPathList = path.alterPrefixPath(database);
    if (innerPathList.size() == 0) {
      return new ArrayList<>();
    }
    PartialPath innerPath = innerPathList.get(0);
    // The innerPath contains `*` and the only `*` is not in last level
    if (innerPath.getDevice().contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    return Collections.singletonList(
        getPartitionManager().getSeriesPartitionSlot(innerPath.getDevice()));
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartition(PathPatternTree patternTree) {
    // Construct empty response
    TSchemaPartitionTableResp resp = new TSchemaPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    // Build GetSchemaPartitionPlan
    Map<String, Set<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    List<PartialPath> relatedPaths = patternTree.getAllPathPatterns();
    List<String> allDatabases = getClusterSchemaManager().getDatabaseNames();
    List<PartialPath> allDatabasePaths = new ArrayList<>();
    for (String database : allDatabases) {
      try {
        allDatabasePaths.add(new PartialPath(database));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
    Map<String, Boolean> scanAllRegions = new HashMap<>();
    for (PartialPath path : relatedPaths) {
      for (int i = 0; i < allDatabases.size(); i++) {
        String database = allDatabases.get(i);
        PartialPath databasePath = allDatabasePaths.get(i);
        if (path.overlapWithFullPathPrefix(databasePath) && !scanAllRegions.containsKey(database)) {
          List<TSeriesPartitionSlot> relatedSlot = calculateRelatedSlot(path, databasePath);
          if (relatedSlot.isEmpty()) {
            scanAllRegions.put(database, true);
            partitionSlotsMap.put(database, new HashSet<>());
          } else {
            partitionSlotsMap.computeIfAbsent(database, k -> new HashSet<>()).addAll(relatedSlot);
          }
        }
      }
    }

    // Return empty resp if the partitionSlotsMap is empty
    if (partitionSlotsMap.isEmpty()) {
      return resp.setStatus(StatusUtils.OK).setSchemaPartitionTable(new HashMap<>());
    }

    GetSchemaPartitionPlan getSchemaPartitionPlan =
        new GetSchemaPartitionPlan(
            partitionSlotsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))));
    SchemaPartitionResp queryResult = partitionManager.getSchemaPartition(getSchemaPartitionPlan);
    resp = queryResult.convertToRpcSchemaPartitionTableResp();

    LOGGER.debug("GetSchemaPartition receive paths: {}, return: {}", relatedPaths, resp);

    return resp;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartition(PathPatternTree patternTree) {
    // Construct empty response
    TSchemaPartitionTableResp resp = new TSchemaPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    List<String> devicePaths = patternTree.getAllDevicePatterns();
    List<String> databases = getClusterSchemaManager().getDatabaseNames();

    // Build GetOrCreateSchemaPartitionPlan
    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    for (String devicePath : devicePaths) {
      if (!devicePath.contains("*")) {
        // Only check devicePaths that without "*"
        for (String database : databases) {
          if (PathUtils.isStartWith(devicePath, database)) {
            partitionSlotsMap
                .computeIfAbsent(database, key -> new ArrayList<>())
                .add(getPartitionManager().getSeriesPartitionSlot(devicePath));
            break;
          }
        }
      }
    }
    GetOrCreateSchemaPartitionPlan getOrCreateSchemaPartitionPlan =
        new GetOrCreateSchemaPartitionPlan(partitionSlotsMap);

    SchemaPartitionResp queryResult =
        partitionManager.getOrCreateSchemaPartition(getOrCreateSchemaPartitionPlan);
    resp = queryResult.convertToRpcSchemaPartitionTableResp();

    if (CONF.isEnablePrintingNewlyCreatedPartition()) {
      printNewCreatedSchemaPartition(devicePaths, resp);
    }

    return resp;
  }

  private void printNewCreatedSchemaPartition(
      List<String> devicePaths, TSchemaPartitionTableResp resp) {
    final String lineSeparator = System.lineSeparator();
    StringBuilder devicePathString = new StringBuilder("{");
    for (String devicePath : devicePaths) {
      devicePathString.append(lineSeparator).append("\t").append(devicePath).append(",");
    }
    devicePathString.append(lineSeparator).append("}");

    StringBuilder schemaPartitionRespString = new StringBuilder("{");
    schemaPartitionRespString
        .append(lineSeparator)
        .append("\tTSStatus=")
        .append(resp.getStatus().getCode())
        .append(",");
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        resp.getSchemaPartitionTable();
    for (Map.Entry<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> databaseEntry :
        schemaPartitionTable.entrySet()) {
      String database = databaseEntry.getKey();
      schemaPartitionRespString
          .append(lineSeparator)
          .append(DATABASE)
          .append(database)
          .append(": {");
      for (Map.Entry<TSeriesPartitionSlot, TConsensusGroupId> slotEntry :
          databaseEntry.getValue().entrySet()) {
        schemaPartitionRespString
            .append(lineSeparator)
            .append("\t\t")
            .append(slotEntry.getKey())
            .append(", ")
            .append(slotEntry.getValue())
            .append(",");
      }
      schemaPartitionRespString.append(lineSeparator).append("\t},");
    }
    schemaPartitionRespString.append(lineSeparator).append("}");
    LOGGER.info(
        "[GetOrCreateSchemaPartition]:{} Receive PathPatternTree: {}, Return TSchemaPartitionTableResp: {}",
        lineSeparator,
        devicePathString,
        schemaPartitionRespString);
  }

  @Override
  public TSchemaNodeManagementResp getNodePathsPartition(
      PartialPath partialPath, PathPatternTree scope, Integer level) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      GetNodePathsPartitionPlan getNodePathsPartitionPlan = new GetNodePathsPartitionPlan();
      getNodePathsPartitionPlan.setPartialPath(partialPath);
      getNodePathsPartitionPlan.setScope(scope);
      if (null != level) {
        getNodePathsPartitionPlan.setLevel(level);
      }
      SchemaNodeManagementResp resp =
          partitionManager.getNodePathsPartition(getNodePathsPartitionPlan);
      TSchemaNodeManagementResp result =
          resp.convertToRpcSchemaNodeManagementPartitionResp(
              getLoadManager().getRegionPriorityMap());

      LOGGER.info(
          "getNodePathsPartition receive devicePaths: {}, level: {}, return TSchemaNodeManagementResp: {}",
          partialPath,
          level,
          result);

      return result;
    } else {
      return new TSchemaNodeManagementResp().setStatus(status);
    }
  }

  @Override
  public TDataPartitionTableResp getDataPartition(GetDataPartitionPlan getDataPartitionPlan) {
    // Construct empty response
    TDataPartitionTableResp resp = new TDataPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }
    DataPartitionResp queryResult = partitionManager.getDataPartition(getDataPartitionPlan);

    resp = queryResult.convertToTDataPartitionTableResp();

    LOGGER.debug(
        "GetDataPartition interface receive PartitionSlotsMap: {}, return: {}",
        getDataPartitionPlan.getPartitionSlotsMap(),
        resp);

    return resp;
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan) {
    // Construct empty response
    TDataPartitionTableResp resp = new TDataPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    DataPartitionResp queryResult =
        partitionManager.getOrCreateDataPartition(getOrCreateDataPartitionPlan);
    resp = queryResult.convertToTDataPartitionTableResp();

    if (CONF.isEnablePrintingNewlyCreatedPartition()) {
      printNewCreatedDataPartition(getOrCreateDataPartitionPlan, resp);
    }

    return resp;
  }

  private void printNewCreatedDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan, TDataPartitionTableResp resp) {
    final String lineSeparator = System.lineSeparator();
    StringBuilder partitionSlotsMapString = new StringBuilder("{");
    for (Map.Entry<String, Map<TSeriesPartitionSlot, TTimeSlotList>> databaseEntry :
        getOrCreateDataPartitionPlan.getPartitionSlotsMap().entrySet()) {
      String database = databaseEntry.getKey();
      partitionSlotsMapString.append(lineSeparator).append(DATABASE).append(database).append(": {");
      for (Map.Entry<TSeriesPartitionSlot, TTimeSlotList> slotEntry :
          databaseEntry.getValue().entrySet()) {
        partitionSlotsMapString
            .append(lineSeparator)
            .append("\t\t")
            .append(slotEntry.getKey())
            .append(",")
            .append(slotEntry.getValue());
      }
      partitionSlotsMapString.append(lineSeparator).append("\t},");
    }
    partitionSlotsMapString.append(lineSeparator).append("}");

    StringBuilder dataPartitionRespString = new StringBuilder("{");
    dataPartitionRespString
        .append(lineSeparator)
        .append("\tTSStatus=")
        .append(resp.getStatus().getCode())
        .append(",");
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        dataPartitionTable = resp.getDataPartitionTable();
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        databaseEntry : dataPartitionTable.entrySet()) {
      String database = databaseEntry.getKey();
      dataPartitionRespString.append(lineSeparator).append(DATABASE).append(database).append(": {");
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
          seriesSlotEntry : databaseEntry.getValue().entrySet()) {
        dataPartitionRespString
            .append(lineSeparator)
            .append("\t\t")
            .append(seriesSlotEntry.getKey())
            .append(": {");
        for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> timeSlotEntry :
            seriesSlotEntry.getValue().entrySet()) {
          dataPartitionRespString
              .append(lineSeparator)
              .append("\t\t\t")
              .append(timeSlotEntry.getKey())
              .append(", ")
              .append(timeSlotEntry.getValue())
              .append(",");
        }
        dataPartitionRespString.append(lineSeparator).append("\t\t},");
      }
      dataPartitionRespString.append(lineSeparator).append("\t}");
    }
    dataPartitionRespString.append(lineSeparator).append("}");

    LOGGER.info(
        "[GetOrCreateDataPartition]:{} Receive PartitionSlotsMap: {}, Return TDataPartitionTableResp: {}",
        lineSeparator,
        partitionSlotsMapString,
        dataPartitionRespString);
  }

  private TSStatus confirmLeader() {
    // Make sure the consensus layer has been initialized
    if (getConsensusManager() == null) {
      return new TSStatus(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode())
          .setMessage(
              "ConsensusManager of target-ConfigNode is not initialized, "
                  + "please make sure the target-ConfigNode has been started successfully.");
    }
    return getConsensusManager().confirmLeader();
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
    return consensusManager.get();
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
  public TriggerManager getTriggerManager() {
    return triggerManager;
  }

  @Override
  public ModelManager getModelManager() {
    return modelManager;
  }

  @Override
  public PipeManager getPipeManager() {
    return pipeManager;
  }

  @Override
  public TSStatus operatePermission(AuthorPlan authorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.operatePermission(authorPlan);
    } else {
      return status;
    }
  }

  @Override
  public DataSet queryPermission(AuthorPlan authorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.queryPermission(authorPlan);
    } else {
      PermissionInfoResp dataSet = new PermissionInfoResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TPermissionInfoResp login(String username, String password) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.login(username, password);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(
      String username, List<PartialPath> paths, int permission) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.checkUserPrivileges(username, paths, permission);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  public TAuthizedPatternTreeResp fetchAuthizedPatternTree(String username, int permission) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      try {
        return permissionManager.fetchAuthizedPTree(username, permission);
      } catch (AuthException e) {
        TAuthizedPatternTreeResp resp = new TAuthizedPatternTreeResp();
        status
            .setCode(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(e.getMessage());
        resp.setStatus(status);
        return resp;
      }
    } else {
      TAuthizedPatternTreeResp resp = new TAuthizedPatternTreeResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    final int ERROR_STATUS_NODE_ID = -1;

    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Make sure the global configurations are consist
      status = checkConfigNodeGlobalConfig(req);
      if (status == null) {
        status =
            ClusterNodeStartUtils.confirmNodeRegistration(
                NodeType.ConfigNode,
                req.getClusterParameters().getClusterName(),
                req.getConfigNodeLocation(),
                this);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return nodeManager.registerConfigNode(req);
        }
      }
    }

    return new TConfigNodeRegisterResp().setStatus(status).setConfigNodeId(ERROR_STATUS_NODE_ID);
  }

  @Override
  public TSStatus restartConfigNode(TConfigNodeRestartReq req) {
    TSStatus status = confirmLeader();
    // Notice: The Seed-ConfigNode must also have the privilege to do Node restart check.
    // Otherwise, the IoTDB-cluster will not have the ability to restart from scratch.
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || ConfigNodeDescriptor.getInstance().isSeedConfigNode()
        || SystemPropertiesUtils.isSeedConfigNode()) {
      status =
          ClusterNodeStartUtils.confirmNodeRestart(
              NodeType.ConfigNode,
              req.getClusterName(),
              req.getConfigNodeLocation().getConfigNodeId(),
              req.getConfigNodeLocation(),
              this);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return ClusterNodeStartUtils.ACCEPT_NODE_RESTART;
      }
    }
    return status;
  }

  public TSStatus checkConfigNodeGlobalConfig(TConfigNodeRegisterReq req) {
    final String errorPrefix = "Reject register, please ensure that the parameter ";
    final String errorSuffix = " is consistent with the Seed-ConfigNode.";
    TSStatus errorStatus = new TSStatus(TSStatusCode.CONFIGURATION_ERROR.getStatusCode());
    TClusterParameters clusterParameters = req.getClusterParameters();

    if (!clusterParameters
        .getConfigNodeConsensusProtocolClass()
        .equals(CONF.getConfigNodeConsensusProtocolClass())) {
      return errorStatus.setMessage(
          errorPrefix + "config_node_consensus_protocol_class" + errorSuffix);
    }
    if (!clusterParameters
        .getDataRegionConsensusProtocolClass()
        .equals(CONF.getDataRegionConsensusProtocolClass())) {
      return errorStatus.setMessage(
          errorPrefix + "data_region_consensus_protocol_class" + errorSuffix);
    }
    if (!clusterParameters
        .getSchemaRegionConsensusProtocolClass()
        .equals(CONF.getSchemaRegionConsensusProtocolClass())) {
      return errorStatus.setMessage(
          errorPrefix + "schema_region_consensus_protocol_class" + errorSuffix);
    }

    if (clusterParameters.getSeriesPartitionSlotNum() != CONF.getSeriesSlotNum()) {
      return errorStatus.setMessage(errorPrefix + "series_partition_slot_num" + errorSuffix);
    }
    if (!clusterParameters
        .getSeriesPartitionExecutorClass()
        .equals(CONF.getSeriesPartitionExecutorClass())) {
      return errorStatus.setMessage(errorPrefix + "series_partition_executor_class" + errorSuffix);
    }

    if (clusterParameters.getDefaultTTL()
        != CommonDescriptor.getInstance().getConfig().getDefaultTTLInMs()) {
      return errorStatus.setMessage(errorPrefix + "default_ttl" + errorSuffix);
    }
    if (clusterParameters.getTimePartitionInterval() != COMMON_CONF.getTimePartitionInterval()) {
      return errorStatus.setMessage(errorPrefix + "time_partition_interval" + errorSuffix);
    }

    if (clusterParameters.getSchemaReplicationFactor() != CONF.getSchemaReplicationFactor()) {
      return errorStatus.setMessage(errorPrefix + "schema_replication_factor" + errorSuffix);
    }
    if (clusterParameters.getDataReplicationFactor() != CONF.getDataReplicationFactor()) {
      return errorStatus.setMessage(errorPrefix + "data_replication_factor" + errorSuffix);
    }

    if (clusterParameters.getSchemaRegionPerDataNode() != CONF.getSchemaRegionPerDataNode()) {
      return errorStatus.setMessage(errorPrefix + "schema_region_per_data_node" + errorSuffix);
    }
    if (clusterParameters.getDataRegionPerDataNode() != CONF.getDataRegionPerDataNode()) {
      return errorStatus.setMessage(errorPrefix + "data_region_per_data_node" + errorSuffix);
    }

    if (!clusterParameters.getReadConsistencyLevel().equals(CONF.getReadConsistencyLevel())) {
      return errorStatus.setMessage(errorPrefix + "read_consistency_level" + errorSuffix);
    }

    if (clusterParameters.getDiskSpaceWarningThreshold()
        != COMMON_CONF.getDiskSpaceWarningThreshold()) {
      return errorStatus.setMessage(errorPrefix + "disk_space_warning_threshold" + errorSuffix);
    }

    if (!clusterParameters.getTimestampPrecision().equals(COMMON_CONF.getTimestampPrecision())) {
      return errorStatus.setMessage(errorPrefix + "timestamp_precision" + errorSuffix);
    }

    if (!clusterParameters.getSchemaEngineMode().equals(COMMON_CONF.getSchemaEngineMode())) {
      return errorStatus.setMessage(errorPrefix + "schema_engine_mode" + errorSuffix);
    }

    if (clusterParameters.getTagAttributeTotalSize() != COMMON_CONF.getTagAttributeTotalSize()) {
      return errorStatus.setMessage(errorPrefix + "tag_attribute_total_size" + errorSuffix);
    }

    if (clusterParameters.getDatabaseLimitThreshold() != COMMON_CONF.getDatabaseLimitThreshold()) {
      return errorStatus.setMessage(errorPrefix + "database_limit_threshold" + errorSuffix);
    }

    return null;
  }

  @Override
  public TSStatus createPeerForConsensusGroup(List<TConfigNodeLocation> configNodeLocations) {
    for (int i = 0; i < 30; i++) {
      try {
        if (consensusManager.get() == null) {
          Thread.sleep(1000);
        } else {
          // When add non Seed-ConfigNode to the ConfigNodeGroup, the parameter should be emptyList
          consensusManager.get().createPeerForConsensusGroup(Collections.emptyList());
          return StatusUtils.OK;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Unexpected interruption during retry creating peer for consensus group");
      } catch (ConsensusException e) {
        LOGGER.error("Failed to create peer for consensus group", e);
        break;
      }
    }
    return StatusUtils.INTERNAL_ERROR;
  }

  @Override
  public TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    TSStatus status = confirmLeader();

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status = nodeManager.checkConfigNodeBeforeRemove(removeConfigNodePlan);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        procedureManager.removeConfigNode(removeConfigNodePlan);
      }
    }

    return status;
  }

  @Override
  public TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Force updating the target ConfigNode's status to Unknown
      getLoadManager()
          .forceUpdateNodeCache(
              NodeType.ConfigNode,
              configNodeLocation.getConfigNodeId(),
              NodeHeartbeatSample.generateDefaultSample(NodeStatus.Unknown));
      LOGGER.info(
          "[ShutdownHook] The ConfigNode-{} will be shutdown soon, mark it as Unknown",
          configNodeLocation.getConfigNodeId());
    }
    return status;
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.createFunction(req)
        : status;
  }

  @Override
  public TSStatus dropFunction(String udfName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.dropFunction(udfName)
        : status;
  }

  @Override
  public TGetUDFTableResp getUDFTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.getUDFTable()
        : new TGetUDFTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.getUDFJar(req)
        : new TGetJarInListResp(status, Collections.emptyList());
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.createTrigger(req)
        : status;
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.dropTrigger(req)
        : status;
  }

  @Override
  public TGetTriggerTableResp getTriggerTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerTable(false)
        : new TGetTriggerTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerTable(true)
        : new TGetTriggerTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getLocationOfStatefulTrigger(triggerName)
        : new TGetLocationForTriggerResp(status);
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerJar(req)
        : new TGetJarInListResp(status, Collections.emptyList());
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipePluginCoordinator().createPipePlugin(req)
        : status;
  }

  @Override
  public TSStatus dropPipePlugin(String pipePluginName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipePluginCoordinator().dropPipePlugin(pipePluginName)
        : status;
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipePluginCoordinator().getPipePluginTable()
        : new TGetPipePluginTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipePluginCoordinator().getPipePluginJar(req)
        : new TGetJarInListResp(status, Collections.emptyList());
  }

  @Override
  public TSStatus merge() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.merge())
        : status;
  }

  @Override
  public TSStatus flush(TFlushReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.flush(req))
        : status;
  }

  @Override
  public TSStatus clearCache() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.clearCache())
        : status;
  }

  @Override
  public TSStatus loadConfiguration() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.loadConfiguration())
        : status;
  }

  @Override
  public TSStatus setSystemStatus(String systemStatus) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.setSystemStatus(systemStatus))
        : status;
  }

  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? nodeManager.setDataNodeStatus(req)
        : status;
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? nodeManager.killQuery(queryId, dataNodeId)
        : status;
  }

  @Override
  public TGetDataNodeLocationsResp getRunningDataNodeLocations() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? new TGetDataNodeLocationsResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            nodeManager.filterDataNodeThroughStatus(NodeStatus.Running).stream()
                .map(TDataNodeConfiguration::getLocation)
                .collect(Collectors.toList()))
        : new TGetDataNodeLocationsResp(status, Collections.emptyList());
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() {
    TSStatus status = confirmLeader();
    TRegionRouteMapResp resp = new TRegionRouteMapResp(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setTimestamp(System.currentTimeMillis());
      resp.setRegionRouteMap(getLoadManager().getRegionPriorityMap());
    }

    return resp;
  }

  @Override
  public UDFManager getUDFManager() {
    return udfManager;
  }

  @Override
  public RegionInfoListResp showRegion(GetRegionInfoListPlan getRegionInfoListPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getRegionInfoList(getRegionInfoListPlan);
    } else {
      RegionInfoListResp regionResp = new RegionInfoListResp();
      regionResp.setStatus(status);
      return regionResp;
    }
  }

  @Override
  public TShowDataNodesResp showDataNodes() {
    TSStatus status = confirmLeader();
    TShowDataNodesResp resp = new TShowDataNodesResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setDataNodesInfoList(nodeManager.getRegisteredDataNodeInfoList())
          .setStatus(StatusUtils.OK);
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() {
    TSStatus status = confirmLeader();
    TShowConfigNodesResp resp = new TShowConfigNodesResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setConfigNodesInfoList(nodeManager.getRegisteredConfigNodeInfoList())
          .setStatus(StatusUtils.OK);
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TShowDatabaseResp showDatabase(TGetDatabaseReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      PathPatternTree scope =
          req.getScopePatternTree() == null
              ? SchemaConstant.ALL_MATCH_SCOPE
              : PathPatternTree.deserialize(ByteBuffer.wrap(req.getScopePatternTree()));
      GetDatabasePlan getDatabasePlan = new GetDatabasePlan(req.getDatabasePathPattern(), scope);
      return getClusterSchemaManager().showDatabase(getDatabasePlan);
    } else {
      return new TShowDatabaseResp().setStatus(status);
    }
  }

  @Override
  public ProcedureManager getProcedureManager() {
    return procedureManager;
  }

  @Override
  public CQManager getCQManager() {
    return cqManager;
  }

  @Override
  public ClusterQuotaManager getClusterQuotaManager() {
    return clusterQuotaManager;
  }

  @Override
  public RetryFailedTasksThread getRetryFailedTasksThread() {
    return retryFailedTasksThread;
  }

  @Override
  public void addMetrics() {
    MetricService.getInstance().addMetricSet(new NodeMetrics(getNodeManager()));
    MetricService.getInstance().addMetricSet(new PartitionMetrics(this));
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      CreateSchemaTemplatePlan createSchemaTemplatePlan =
          new CreateSchemaTemplatePlan(req.getSerializedTemplate());
      return clusterSchemaManager.createTemplate(createSchemaTemplatePlan);
    } else {
      return status;
    }
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getAllTemplates();
    } else {
      return new TGetAllTemplatesResp().setStatus(status);
    }
  }

  @Override
  public TGetTemplateResp getTemplate(String req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getTemplate(req);
    } else {
      return new TGetTemplateResp().setStatus(status);
    }
  }

  @Override
  public synchronized TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.setSchemaTemplate(req.getQueryId(), req.getName(), req.getPath());
    } else {
      return status;
    }
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(TGetPathsSetTemplatesReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      PathPatternTree scope =
          req.getScopePatternTree() == null
              ? SchemaConstant.ALL_MATCH_SCOPE
              : PathPatternTree.deserialize(ByteBuffer.wrap(req.getScopePatternTree()));
      return clusterSchemaManager.getPathsSetTemplate(req.getTemplateName(), scope);
    } else {
      return new TGetPathsSetTemplatesResp(status);
    }
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));

    List<PartialPath> patternList = patternTree.getAllPathPatterns();
    TemplateSetInfoResp templateSetInfoResp = clusterSchemaManager.getTemplateSetInfo(patternList);
    if (templateSetInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return templateSetInfoResp.getStatus();
    }

    Map<PartialPath, List<Template>> templateSetInfo = templateSetInfoResp.getPatternTemplateMap();
    if (templateSetInfo.isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.TEMPLATE_NOT_SET,
          String.format(
              "Schema Template %s is not set on any prefix path of %s",
              req.getTemplateName(), patternList));
    }

    if (!req.getTemplateName().equals(ONE_LEVEL_PATH_WILDCARD)) {
      Map<PartialPath, List<Template>> filteredTemplateSetInfo = new HashMap<>();
      for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
        for (Template template : entry.getValue()) {
          if (template.getName().equals(req.getTemplateName())) {
            filteredTemplateSetInfo.put(entry.getKey(), Collections.singletonList(template));
            break;
          }
        }
      }

      if (filteredTemplateSetInfo.isEmpty()) {
        return RpcUtils.getStatus(
            TSStatusCode.TEMPLATE_NOT_SET,
            String.format(
                "Schema Template %s is not set on any prefix path of %s",
                req.getTemplateName(), patternList));
      }

      templateSetInfo = filteredTemplateSetInfo;
    }

    return procedureManager.deactivateTemplate(req.getQueryId(), templateSetInfo);
  }

  @Override
  public synchronized TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    Pair<TSStatus, Template> checkResult =
        clusterSchemaManager.checkIsTemplateSetOnPath(req.getTemplateName(), req.getPath());
    if (checkResult.left.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      try {
        return procedureManager.unsetSchemaTemplate(
            req.getQueryId(), checkResult.right, new PartialPath(req.getPath()));
      } catch (IllegalPathException e) {
        return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
      }
    } else {
      return checkResult.left;
    }
  }

  @Override
  public TSStatus dropSchemaTemplate(String templateName) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.dropSchemaTemplate(templateName);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      ByteBuffer buffer = ByteBuffer.wrap(req.getTemplateAlterInfo());
      TemplateAlterOperationType operationType =
          TemplateAlterOperationUtil.parseOperationType(buffer);
      if (operationType.equals(TemplateAlterOperationType.EXTEND_TEMPLATE)) {
        return clusterSchemaManager.extendSchemaTemplate(
            TemplateAlterOperationUtil.parseTemplateExtendInfo(buffer));
      }
      return RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.deleteTimeSeries(req);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.deleteLogicalView(req);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus alterLogicalView(TAlterLogicalViewReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.alterLogicalView(req);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().createPipe(req)
        : status;
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().startPipe(pipeName)
        : status;
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().stopPipe(pipeName)
        : status;
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().dropPipe(pipeName)
        : status;
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().showPipes(req)
        : new TShowPipeResp().setStatus(status);
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? pipeManager.getPipeTaskCoordinator().getAllPipeInfo()
        : new TGetAllPipeInfoResp(status, Collections.emptyList());
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getRegionId(req).convertToRpcGetRegionIdResp()
        : new TGetRegionIdResp(status);
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getTimeSlotList(req).convertToRpcGetTimeSlotListResp()
        : new TGetTimeSlotListResp(status);
  }

  @Override
  public TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.countTimeSlotList(req).convertToRpcCountTimeSlotListResp()
        : new TCountTimeSlotListResp(status);
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getSeriesSlotList(req).convertToRpcGetSeriesSlotListResp()
        : new TGetSeriesSlotListResp(status);
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? procedureManager.migrateRegion(req)
        : status;
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? cqManager.createCQ(req)
        : status;
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? cqManager.dropCQ(req)
        : status;
  }

  @Override
  public TShowCQResp showCQ() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? cqManager.showCQ()
        : new TShowCQResp(status, Collections.emptyList());
  }

  /**
   * Get all related schemaRegion which may contains the timeseries matched by given patternTree.
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
      PathPatternTree patternTree) {
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        getSchemaPartition(patternTree).getSchemaPartitionTable();

    List<TRegionReplicaSet> allRegionReplicaSets = getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        schemaPartitionTable.values().stream()
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toSet());
    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  /**
   * Get all related dataRegion which may contains the data of specific timeseries matched by given
   * patternTree
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRelatedDataRegionGroup(
      PathPatternTree patternTree) {
    // Get all databases and slots by getting schemaengine partition
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        getSchemaPartition(patternTree).getSchemaPartitionTable();

    // Construct request for getting data partition
    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap = new HashMap<>();
    schemaPartitionTable.forEach(
        (key, value) -> {
          Map<TSeriesPartitionSlot, TTimeSlotList> slotListMap = new HashMap<>();
          value
              .keySet()
              .forEach(
                  slot ->
                      slotListMap.put(
                          slot, new TTimeSlotList(Collections.emptyList(), true, true)));
          partitionSlotsMap.put(key, slotListMap);
        });

    // Get all data partitions
    GetDataPartitionPlan getDataPartitionPlan = new GetDataPartitionPlan(partitionSlotsMap);
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        dataPartitionTable = getDataPartition(getDataPartitionPlan).getDataPartitionTable();

    // Get all region replicaset of target data partitions
    List<TRegionReplicaSet> allRegionReplicaSets = getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        dataPartitionTable.values().stream()
            .flatMap(
                tSeriesPartitionSlotMapMap ->
                    tSeriesPartitionSlotMapMap.values().stream()
                        .flatMap(
                            tTimePartitionSlotListMap ->
                                tTimePartitionSlotListMap.values().stream()
                                    .flatMap(Collection::stream)))
            .collect(Collectors.toSet());
    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  public TSStatus transfer(List<TDataNodeLocation> newUnknownDataList) {
    Map<Integer, TDataNodeLocation> runningDataNodeLocationMap = new HashMap<>();
    nodeManager
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .forEach(
            dataNodeConfiguration ->
                runningDataNodeLocationMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    if (runningDataNodeLocationMap.isEmpty()) {
      // No running DataNode, will not transfer and print log
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }

    newUnknownDataList.forEach(
        dataNodeLocation -> runningDataNodeLocationMap.remove(dataNodeLocation.getDataNodeId()));

    LOGGER.info("Start transfer of {}", newUnknownDataList);
    // Transfer trigger
    TSStatus transferResult =
        triggerManager.transferTrigger(newUnknownDataList, runningDataNodeLocationMap);
    if (transferResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Fail to transfer because {}, will retry", transferResult.getMessage());
    }

    return transferResult;
  }

  @Override
  public TSStatus createModel(TCreateModelReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.createModel(req)
        : status;
  }

  @Override
  public TSStatus dropModel(TDropModelReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.dropModel(req)
        : status;
  }

  @Override
  public TShowModelResp showModel(TShowModelReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.showModel(req)
        : new TShowModelResp(status, Collections.emptyList());
  }

  @Override
  public TShowTrailResp showTrail(TShowTrailReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.showTrail(req)
        : new TShowTrailResp(status, Collections.emptyList());
  }

  @Override
  public TSStatus updateModelInfo(TUpdateModelInfoReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.updateModelInfo(req)
        : status;
  }

  @Override
  public TSStatus updateModelState(TUpdateModelStateReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? modelManager.updateModelState(req)
        : status;
  }

  @Override
  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.setSpaceQuota(req)
        : status;
  }

  public TSpaceQuotaResp showSpaceQuota(List<String> databases) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.showSpaceQuota(databases)
        : new TSpaceQuotaResp(status);
  }

  public TSpaceQuotaResp getSpaceQuota() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.getSpaceQuota()
        : new TSpaceQuotaResp(status);
  }

  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.setThrottleQuota(req)
        : status;
  }

  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.showThrottleQuota(req)
        : new TThrottleQuotaResp(status);
  }

  public TThrottleQuotaResp getThrottleQuota() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? clusterQuotaManager.getThrottleQuota()
        : new TThrottleQuotaResp(status);
  }
}
