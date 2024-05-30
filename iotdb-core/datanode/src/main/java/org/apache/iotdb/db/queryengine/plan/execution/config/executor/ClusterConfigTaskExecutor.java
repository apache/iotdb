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

package org.apache.iotdb.db.queryengine.plan.execution.config.executor;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.executable.ExecutableResource;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DatabaseSchemaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetRegionIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetSeriesSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowContinuousQueriesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTriggersTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowVariablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowNodesInSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowPathSetTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.ShowPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowSpaceQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowThrottleQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowSubscriptionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowTopicsTask;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.TransformToViewExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.rescon.DataNodeSchemaQuotaManager;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateAlterOperationType;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateAlterOperationUtil;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairTaskStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.trigger.service.TriggerClassLoader;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.udf.api.UDF;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.protocol.client.ConfigNodeClient.MSG_RECONNECTION_FAIL;

public class ClusterConfigTaskExecutor implements IConfigTaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigTaskExecutor.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  /** FIXME Consolidate this clientManager with the upper one. */
  private static final IClientManager<ConfigRegionId, ConfigNodeClient>
      CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<ConfigRegionId, ConfigNodeClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.ClusterDeletionConfigNodeClientPoolFactory());

  private static final class ClusterConfigTaskExecutorHolder {

    private static final ClusterConfigTaskExecutor INSTANCE = new ClusterConfigTaskExecutor();

    private ClusterConfigTaskExecutorHolder() {}
  }

  public static ClusterConfigTaskExecutor getInstance() {
    return ClusterConfigTaskExecutor.ClusterConfigTaskExecutorHolder.INSTANCE;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setDatabase(
      DatabaseSchemaStatement databaseSchemaStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    TDatabaseSchema databaseSchema =
        DatabaseSchemaTask.constructDatabaseSchema(databaseSchemaStatement);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setDatabase(databaseSchema);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        // If database already exists when loading, we do not throw exceptions to avoid printing too
        // many logs
        if (TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode() == tsStatus.getCode()
            && !databaseSchemaStatement.getEnablePrintExceptionLog()) {
          future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
        } else {
          LOGGER.warn(
              "Failed to execute create database {} in config node, status is {}.",
              databaseSchemaStatement.getDatabasePath(),
              tsStatus);
          future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
        }

      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> alterDatabase(
      DatabaseSchemaStatement databaseSchemaStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    TDatabaseSchema databaseSchema =
        DatabaseSchemaTask.constructDatabaseSchema(databaseSchemaStatement);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.alterDatabase(databaseSchema);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        if (databaseSchemaStatement.getEnablePrintExceptionLog()) {
          LOGGER.warn(
              "Failed to execute alter database {} in config node, status is {}.",
              databaseSchemaStatement.getDatabasePath(),
              tsStatus);
        }
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showDatabase(
      ShowDatabaseStatement showDatabaseStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    List<String> databasePathPattern =
        Arrays.asList(showDatabaseStatement.getPathPattern().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TGetDatabaseReq req =
          new TGetDatabaseReq(
              databasePathPattern, showDatabaseStatement.getAuthorityScope().serialize());
      TShowDatabaseResp resp = client.showDatabase(req);
      // build TSBlock
      showDatabaseStatement.buildTSBlock(resp.getDatabaseInfoMap(), future);
    } catch (IOException | ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countDatabase(
      CountDatabaseStatement countDatabaseStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int databaseNum;
    List<String> databasePathPattern =
        Arrays.asList(countDatabaseStatement.getPathPattern().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetDatabaseReq req =
          new TGetDatabaseReq(
              databasePathPattern, countDatabaseStatement.getAuthorityScope().serialize());
      TCountDatabaseResp resp = client.countMatchedDatabases(req);
      databaseNum = resp.getCount();
      // build TSBlock
      CountDatabaseTask.buildTSBlock(databaseNum, future);
    } catch (IOException | ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteDatabase(
      DeleteDatabaseStatement deleteDatabaseStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TDeleteDatabasesReq req = new TDeleteDatabasesReq(deleteDatabaseStatement.getPrefixPath());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus = client.deleteDatabases(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute delete database {} in config node, status is {}.",
            deleteDatabaseStatement.getPrefixPath(),
            tsStatus);
        if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          future.setException(
              new BatchProcessException(tsStatus.subStatus.toArray(new TSStatus[0])));
        } else {
          future.setException(new IoTDBException(tsStatus.message, tsStatus.getCode()));
        }
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createFunction(
      CreateFunctionStatement createFunctionStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String udfName = createFunctionStatement.getUdfName();
    String className = createFunctionStatement.getClassName();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TCreateFunctionReq tCreateFunctionReq = new TCreateFunctionReq(udfName, className, false);
      String libRoot = UDFExecutableManager.getInstance().getLibRoot();
      String jarFileName;
      ByteBuffer jarFile;
      String jarMd5;
      if (createFunctionStatement.isUsingURI()) {
        String uriString = createFunctionStatement.getUriString();
        if (uriString == null || uriString.isEmpty()) {
          future.setException(
              new IoTDBException(
                  "URI is empty, please specify the URI.",
                  TSStatusCode.UDF_DOWNLOAD_ERROR.getStatusCode()));
          return future;
        }
        jarFileName = new File(uriString).getName();
        try {
          URI uri = new URI(uriString);
          if (uri.getScheme() == null) {
            future.setException(
                new IoTDBException(
                    "The scheme of URI is not set, please specify the scheme of URI.",
                    TSStatusCode.UDF_DOWNLOAD_ERROR.getStatusCode()));
            return future;
          }
          if (!uri.getScheme().equals("file")) {
            // Download executable
            ExecutableResource resource =
                UDFExecutableManager.getInstance().request(Collections.singletonList(uriString));
            String jarFilePathUnderTempDir =
                UDFExecutableManager.getInstance()
                        .getDirStringUnderTempRootByRequestId(resource.getRequestId())
                    + File.separator
                    + jarFileName;
            // libRoot should be the path of the specified jar
            libRoot = jarFilePathUnderTempDir;
            jarFile = ExecutableManager.transferToBytebuffer(jarFilePathUnderTempDir);
            jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(jarFilePathUnderTempDir)));
          } else {
            // libRoot should be the path of the specified jar
            libRoot = new File(new URI(uriString)).getAbsolutePath();
            // If jarPath is a file path on datanode, we transfer it to ByteBuffer and send it to
            // ConfigNode.
            jarFile = ExecutableManager.transferToBytebuffer(libRoot);
            // Set md5 of the jar file
            jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(libRoot)));
          }
        } catch (IOException | URISyntaxException e) {
          LOGGER.warn(
              "Failed to get executable for UDF({}) using URI: {}, the cause is: {}",
              createFunctionStatement.getUdfName(),
              createFunctionStatement.getUriString(),
              e);
          future.setException(
              new IoTDBException(
                  "Failed to get executable for UDF '"
                      + createFunctionStatement.getUdfName()
                      + "', please check the URI.",
                  TSStatusCode.TRIGGER_DOWNLOAD_ERROR.getStatusCode()));
          return future;
        }
        // modify req
        tCreateFunctionReq.setJarFile(jarFile);
        tCreateFunctionReq.setJarMD5(jarMd5);
        tCreateFunctionReq.setIsUsingURI(true);
        tCreateFunctionReq.setJarName(
            String.format(
                "%s-%s.%s",
                jarFileName.substring(0, jarFileName.lastIndexOf(".")),
                jarMd5,
                jarFileName.substring(jarFileName.lastIndexOf(".") + 1)));
      }

      // try to create instance, this request will fail if creation is not successful
      try (UDFClassLoader classLoader = new UDFClassLoader(libRoot)) {
        // ensure that jar file contains the class and the class is a UDF
        Class<?> clazz = Class.forName(createFunctionStatement.getClassName(), true, classLoader);
        UDF udf = (UDF) clazz.getDeclaredConstructor().newInstance();
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | ClassCastException e) {
        LOGGER.warn(
            "Failed to create function when try to create UDF({}) instance first, the cause is: {}",
            createFunctionStatement.getUdfName(),
            e);
        future.setException(
            new IoTDBException(
                "Failed to load class '"
                    + createFunctionStatement.getClassName()
                    + "', because it's not found in jar file: "
                    + createFunctionStatement.getUriString(),
                TSStatusCode.UDF_LOAD_CLASS_ERROR.getStatusCode()));
        return future;
      }

      final TSStatus executionStatus = client.createFunction(tCreateFunctionReq);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn(
            "Failed to create function {}({}) because {}",
            udfName,
            className,
            executionStatus.getMessage());
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | IOException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropFunction(String udfName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus executionStatus = client.dropFunction(new TDropFunctionReq(udfName));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn("[{}] Failed to drop function {}.", executionStatus, udfName);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showFunctions() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetUDFTableResp getUDFTableResp = client.getUDFTable();
      if (getUDFTableResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                getUDFTableResp.getStatus().message, getUDFTableResp.getStatus().code));
        return future;
      }
      // convert UDFTable and buildTsBlock
      ShowFunctionsTask.buildTsBlock(getUDFTableResp.getAllUDFInformation(), future);
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }

    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createTrigger(
      CreateTriggerStatement createTriggerStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

      TCreateTriggerReq tCreateTriggerReq =
          new TCreateTriggerReq(
              createTriggerStatement.getTriggerName(),
              createTriggerStatement.getClassName(),
              createTriggerStatement.getTriggerEvent().getId(),
              createTriggerStatement.getTriggerType().getId(),
              createTriggerStatement.getPathPattern().serialize(),
              createTriggerStatement.getAttributes(),
              FailureStrategy.OPTIMISTIC.getId(),
              createTriggerStatement.isUsingURI()); // set default strategy

      String libRoot = TriggerExecutableManager.getInstance().getLibRoot();
      String jarFileName;
      ByteBuffer jarFile;
      String jarMd5;
      if (createTriggerStatement.isUsingURI()) {
        String uriString = createTriggerStatement.getUriString();
        if (uriString == null || uriString.isEmpty()) {
          future.setException(
              new IoTDBException(
                  "URI is empty, please specify the URI.",
                  TSStatusCode.TRIGGER_DOWNLOAD_ERROR.getStatusCode()));
          return future;
        }
        jarFileName = new File(uriString).getName();
        try {
          URI uri = new URI(uriString);
          if (uri.getScheme() == null) {
            future.setException(
                new IoTDBException(
                    "The scheme of URI is not set, please specify the scheme of URI.",
                    TSStatusCode.TRIGGER_DOWNLOAD_ERROR.getStatusCode()));
            return future;
          }
          if (!uri.getScheme().equals("file")) {
            // download executable
            ExecutableResource resource =
                TriggerExecutableManager.getInstance()
                    .request(Collections.singletonList(uriString));
            String jarFilePathUnderTempDir =
                TriggerExecutableManager.getInstance()
                        .getDirStringUnderTempRootByRequestId(resource.getRequestId())
                    + File.separator
                    + jarFileName;
            // libRoot should be the path of the specified jar
            libRoot = jarFilePathUnderTempDir;
            jarFile = ExecutableManager.transferToBytebuffer(jarFilePathUnderTempDir);
            jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(jarFilePathUnderTempDir)));

          } else {
            // libRoot should be the path of the specified jar
            libRoot = new File(new URI(uriString)).getAbsolutePath();
            // If jarPath is a file path on datanode, we transfer it to ByteBuffer and send it to
            // ConfigNode.
            jarFile = ExecutableManager.transferToBytebuffer(libRoot);
            // set md5 of the jar file
            jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(libRoot)));
          }
        } catch (IOException | URISyntaxException e) {
          LOGGER.warn(
              "Failed to get executable for Trigger({}) using URI: {}, the cause is: {}",
              createTriggerStatement.getTriggerName(),
              createTriggerStatement.getUriString(),
              e);
          future.setException(
              new IoTDBException(
                  "Failed to get executable for Trigger '"
                      + createTriggerStatement.getUriString()
                      + "', please check the URI.",
                  TSStatusCode.TRIGGER_DOWNLOAD_ERROR.getStatusCode()));
          return future;
        }
        // modify req
        tCreateTriggerReq.setJarFile(jarFile);
        tCreateTriggerReq.setJarMD5(jarMd5);
        tCreateTriggerReq.setIsUsingURI(true);
        tCreateTriggerReq.setJarName(
            String.format(
                "%s-%s.%s",
                jarFileName.substring(0, jarFileName.lastIndexOf(".")),
                jarMd5,
                jarFileName.substring(jarFileName.lastIndexOf(".") + 1)));
      }

      // try to create instance, this request will fail if creation is not successful
      try (TriggerClassLoader classLoader = new TriggerClassLoader(libRoot)) {
        Class<?> triggerClass =
            Class.forName(createTriggerStatement.getClassName(), true, classLoader);
        Trigger trigger = (Trigger) triggerClass.getDeclaredConstructor().newInstance();
        tCreateTriggerReq.setFailureStrategy(trigger.getFailureStrategy().getId());
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | ClassCastException e) {
        LOGGER.warn(
            "Failed to create trigger when try to create trigger({}) instance first, the cause is: {}",
            createTriggerStatement.getTriggerName(),
            e);
        future.setException(
            new IoTDBException(
                "Failed to load class '"
                    + createTriggerStatement.getClassName()
                    + "', because it's not found in jar file: "
                    + createTriggerStatement.getUriString(),
                TSStatusCode.TRIGGER_LOAD_CLASS_ERROR.getStatusCode()));
        return future;
      }

      final TSStatus executionStatus = client.createTrigger(tCreateTriggerReq);

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn(
            "[{}] Failed to create trigger {}. TSStatus is {}",
            executionStatus,
            createTriggerStatement.getTriggerName(),
            executionStatus.message);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropTrigger(String triggerName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus executionStatus = client.dropTrigger(new TDropTriggerReq(triggerName));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn("[{}] Failed to drop trigger {}.", executionStatus, triggerName);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTriggers() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetTriggerTableResp getTriggerTableResp = client.getTriggerTable();
      if (getTriggerTableResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                getTriggerTableResp.getStatus().message, getTriggerTableResp.getStatus().code));
        return future;
      }
      // convert triggerTable and buildTsBlock
      ShowTriggersTask.buildTsBlock(getTriggerTableResp.getAllTriggerInformation(), future);
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }

    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createPipePlugin(
      CreatePipePluginStatement createPipePluginStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    final String pluginName = createPipePluginStatement.getPluginName();
    final String className = createPipePluginStatement.getClassName();
    final String uriString = createPipePluginStatement.getUriString();

    if (uriString == null || uriString.isEmpty()) {
      future.setException(
          new IoTDBException(
              "Failed to create pipe plugin, because the URI is empty.",
              TSStatusCode.PIPE_PLUGIN_DOWNLOAD_ERROR.getStatusCode()));
      return future;
    }

    try (final ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      String libRoot;
      ByteBuffer jarFile;
      String jarMd5;

      final String jarFileName = new File(uriString).getName();
      try {
        URI uri = new URI(uriString);
        if (uri.getScheme() == null) {
          future.setException(
              new IoTDBException(
                  "The scheme of URI is not set, please specify the scheme of URI.",
                  TSStatusCode.PIPE_PLUGIN_DOWNLOAD_ERROR.getStatusCode()));
          return future;
        }
        if (!uri.getScheme().equals("file")) {
          // Download executable
          ExecutableResource resource =
              PipePluginExecutableManager.getInstance()
                  .request(Collections.singletonList(uriString));
          String jarFilePathUnderTempDir =
              PipePluginExecutableManager.getInstance()
                      .getDirStringUnderTempRootByRequestId(resource.getRequestId())
                  + File.separator
                  + jarFileName;
          // libRoot should be the path of the specified jar
          libRoot = jarFilePathUnderTempDir;
          jarFile = ExecutableManager.transferToBytebuffer(jarFilePathUnderTempDir);
          jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(jarFilePathUnderTempDir)));
        } else {
          // libRoot should be the path of the specified jar
          libRoot = new File(new URI(uriString)).getAbsolutePath();
          // If jarPath is a file path on datanode, we transfer it to ByteBuffer and send it to
          // ConfigNode.
          jarFile = ExecutableManager.transferToBytebuffer(libRoot);
          // Set md5 of the jar file
          jarMd5 = DigestUtils.md5Hex(Files.newInputStream(Paths.get(libRoot)));
        }
      } catch (IOException | URISyntaxException e) {
        LOGGER.warn(
            "Failed to get executable for PipePlugin({}) using URI: {}, the cause is: {}",
            createPipePluginStatement.getPluginName(),
            createPipePluginStatement.getUriString(),
            e);
        future.setException(
            new IoTDBException(
                "Failed to get executable for PipePlugin"
                    + createPipePluginStatement.getPluginName()
                    + "', please check the URI.",
                TSStatusCode.PIPE_PLUGIN_DOWNLOAD_ERROR.getStatusCode()));
        return future;
      }

      // try to create instance, this request will fail if creation is not successful
      try (PipePluginClassLoader classLoader = new PipePluginClassLoader(libRoot)) {
        // ensure that jar file contains the class and the class is a pipe plugin
        Class<?> clazz = Class.forName(createPipePluginStatement.getClassName(), true, classLoader);
        PipePlugin ignored = (PipePlugin) clazz.getDeclaredConstructor().newInstance();
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | ClassCastException e) {
        LOGGER.warn(
            "Failed to create function when try to create PipePlugin({}) instance first, the cause is: {}",
            createPipePluginStatement.getPluginName(),
            e);
        future.setException(
            new IoTDBException(
                "Failed to load class '"
                    + createPipePluginStatement.getClassName()
                    + "', because it's not found in jar file: "
                    + createPipePluginStatement.getUriString(),
                TSStatusCode.PIPE_PLUGIN_LOAD_CLASS_ERROR.getStatusCode()));
        return future;
      }

      final TSStatus executionStatus =
          client.createPipePlugin(
              new TCreatePipePluginReq()
                  .setPluginName(pluginName)
                  .setClassName(className)
                  .setJarFile(jarFile)
                  .setJarMD5(jarMd5)
                  .setJarName(
                      String.format(
                          "%s-%s.%s",
                          jarFileName.substring(0, jarFileName.lastIndexOf(".")),
                          jarMd5,
                          jarFileName.substring(jarFileName.lastIndexOf(".") + 1))));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn(
            "Failed to create PipePlugin {}({}) because {}",
            pluginName,
            className,
            executionStatus.getMessage());
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropPipePlugin(String pluginName) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (final ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus executionStatus = client.dropPipePlugin(new TDropPipePluginReq(pluginName));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn("[{}] Failed to drop pipe plugin {}.", executionStatus, pluginName);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPipePlugins() {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (final ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetPipePluginTableResp getPipePluginTableResp = client.getPipePluginTable();
      if (getPipePluginTableResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                getPipePluginTableResp.getStatus().message,
                getPipePluginTableResp.getStatus().code));
        return future;
      }
      // convert PipePluginTable and buildTsBlock
      ShowPipePluginsTask.buildTsBlock(getPipePluginTableResp.getAllPipePluginMeta(), future);
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<String> databasePathPattern = Arrays.asList(setTTLStatement.getDatabasePath().getNodes());
    TSetTTLReq setTTLReq = new TSetTTLReq(databasePathPattern, setTTLStatement.getTTL());
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setTTL(setTTLReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute {} {} in config node, status is {}.",
            taskName,
            setTTLStatement.getDatabasePath(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> merge(boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.merge();
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        StorageEngine.getInstance().mergeAll();
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (StorageEngineException e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq, boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.flush(tFlushReq);
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        StorageEngine.getInstance().operateFlush(tFlushReq);
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (Exception e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> clearCache(boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.clearCache();
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        StorageEngine.getInstance().clearCache();
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (Exception e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> startRepairData(boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.startRepairData();
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      if (!StorageEngine.getInstance().isAllSgReady()) {
        future.setException(
            new IoTDBException(
                "not all sg is ready", TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
        return future;
      }
      IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
      if (!iotdbConfig.isEnableSeqSpaceCompaction()
          || !iotdbConfig.isEnableUnseqSpaceCompaction()) {
        future.setException(
            new IoTDBException(
                "cannot start repair task because inner space compaction is not enabled",
                TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
        return future;
      }
      try {
        if (StorageEngine.getInstance().repairData()) {
          tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        } else {
          if (CompactionScheduleTaskManager.getRepairTaskManagerInstance().getRepairTaskStatus()
              == RepairTaskStatus.STOPPING) {
            tsStatus =
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, "previous repair task is still stopping");
          } else {
            tsStatus =
                RpcUtils.getStatus(
                    TSStatusCode.EXECUTE_STATEMENT_ERROR, "already have a running repair task");
          }
        }
      } catch (Exception e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> stopRepairData(boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.stopRepairData();
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        StorageEngine.getInstance().stopRepairData();
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (StorageEngineException e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> loadConfiguration(boolean onCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.loadConfiguration();
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        IoTDBDescriptor.getInstance().loadHotModifiedProps();
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (Exception e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new StatementExecutionException(tsStatus));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setSystemStatus(boolean onCluster, NodeStatus status) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (onCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        tsStatus = client.setSystemStatus(status.getStatus());
      } catch (ClientManagerException | TException e) {
        future.setException(e);
      }
    } else {
      try {
        CommonDescriptor.getInstance().getConfig().setNodeStatus(status);
        tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } catch (Exception e) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new StatementExecutionException(tsStatus));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> killQuery(KillQueryStatement killQueryStatement) {
    int dataNodeId = -1;
    String queryId = killQueryStatement.getQueryId();
    if (!killQueryStatement.isKillAll()) {
      String[] splits = queryId.split("_");
      try {
        // We just judge the input queryId has three '_' and the DataNodeId from it is non-negative
        // here
        if (splits.length != 4 || ((dataNodeId = Integer.parseInt(splits[3])) < 0)) {
          throw new SemanticException("Please ensure your input <queryId> is correct");
        }
      } catch (NumberFormatException e) {
        throw new SemanticException("Please ensure your input <queryId> is correct");
      }
    }
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus executionStatus = client.killQuery(queryId, dataNodeId);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn("Failed to kill query [{}], because {}", queryId, executionStatus.message);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showCluster(ShowClusterStatement showClusterStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowClusterResp showClusterResp = new TShowClusterResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showClusterResp = client.showCluster();
    } catch (ClientManagerException | TException e) {
      if (showClusterResp.getConfigNodeList() == null) {
        future.setException(new TException(MSG_RECONNECTION_FAIL));
      } else {
        future.setException(e);
      }
      return future;
    }
    // build TSBlock
    if (showClusterStatement.isDetails()) {
      ShowClusterDetailsTask.buildTSBlock(showClusterResp, future);
    } else {
      ShowClusterTask.buildTsBlock(showClusterResp, future);
    }

    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showClusterParameters() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowVariablesResp showVariablesResp = new TShowVariablesResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showVariablesResp = client.showVariables();
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }

    // build TSBlock
    ShowVariablesTask.buildTSBlock(showVariablesResp, future);

    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showClusterId() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowClusterIdTask.buildTSBlock(
        IoTDBDescriptor.getInstance().getConfig().getClusterId(), future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> databasePaths = showTTLStatement.getPaths();
    Map<String, Long> databaseToTTL = new HashMap<>();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      ByteBuffer scope = showTTLStatement.getAuthorityScope().serialize();
      if (showTTLStatement.isAll()) {
        List<String> allStorageGroupPathPattern = Arrays.asList("root", "**");
        TGetDatabaseReq req = new TGetDatabaseReq(allStorageGroupPathPattern, scope);
        TDatabaseSchemaResp resp = client.getMatchedDatabaseSchemas(req);
        for (Map.Entry<String, TDatabaseSchema> entry : resp.getDatabaseSchemaMap().entrySet()) {
          databaseToTTL.put(entry.getKey(), entry.getValue().getTTL());
        }
      } else {
        for (PartialPath databasePath : databasePaths) {
          List<String> databasePathPattern = Arrays.asList(databasePath.getNodes());
          TGetDatabaseReq req = new TGetDatabaseReq(databasePathPattern, scope);
          TDatabaseSchemaResp resp = client.getMatchedDatabaseSchemas(req);
          for (Map.Entry<String, TDatabaseSchema> entry : resp.getDatabaseSchemaMap().entrySet()) {
            if (!databaseToTTL.containsKey(entry.getKey())) {
              databaseToTTL.put(entry.getKey(), entry.getValue().getTTL());
            }
          }
        }
      }
    } catch (IOException | ClientManagerException | TException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowTTLTask.buildTSBlock(databaseToTTL, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showRegion(ShowRegionStatement showRegionStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowRegionResp showRegionResp = new TShowRegionResp();
    TShowRegionReq showRegionReq = new TShowRegionReq();
    showRegionReq.setConsensusGroupType(showRegionStatement.getRegionType());
    if (showRegionStatement.getStorageGroups() == null) {
      showRegionReq.setDatabases(null);
    } else {
      showRegionReq.setDatabases(
          showRegionStatement.getStorageGroups().stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toList()));
    }
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showRegionResp = client.showRegion(showRegionReq);
      if (showRegionResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showRegionResp.getStatus().message, showRegionResp.getStatus().code));
        return future;
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }

    // filter the regions by nodeId
    if (showRegionStatement.getNodeIds() != null) {
      List<TRegionInfo> regionInfos = showRegionResp.getRegionInfoList();
      regionInfos =
          regionInfos.stream()
              .filter(
                  regionInfo ->
                      showRegionStatement.getNodeIds().contains(regionInfo.getDataNodeId()))
              .collect(Collectors.toList());
      showRegionResp.setRegionInfoList(regionInfos);
    }

    // build TSBlock
    ShowRegionTask.buildTSBlock(showRegionResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showDataNodes(
      ShowDataNodesStatement showDataNodesStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowDataNodesResp showDataNodesResp = new TShowDataNodesResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showDataNodesResp = client.showDataNodes();
      if (showDataNodesResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showDataNodesResp.getStatus().message, showDataNodesResp.getStatus().code));
        return future;
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowDataNodesTask.buildTSBlock(showDataNodesResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showConfigNodes() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowConfigNodesResp showConfigNodesResp = new TShowConfigNodesResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      showConfigNodesResp = client.showConfigNodes();
      if (showConfigNodesResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showConfigNodesResp.getStatus().message, showConfigNodesResp.getStatus().code));
        return future;
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowConfigNodesTask.buildTSBlock(showConfigNodesResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    try {
      // Send request to some API server
      TSStatus tsStatus =
          ClusterTemplateManager.getInstance().createSchemaTemplate(createSchemaTemplateStatement);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute create device template {} in config node, status is {}.",
            createSchemaTemplateStatement.getName(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e.getCause());
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      // Send request to some API server
      List<Template> templateList = ClusterTemplateManager.getInstance().getAllTemplates();
      // build TSBlock
      ShowSchemaTemplateTask.buildTSBlock(templateList, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String req = showNodesInSchemaTemplateStatement.getTemplateName();
    try {
      // Send request to some API server
      Template template = ClusterTemplateManager.getInstance().getTemplate(req);
      // Build TSBlock
      ShowNodesInSchemaTemplateTask.buildTSBlock(template, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setSchemaTemplate(
      String queryId, SetSchemaTemplateStatement setSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String templateName = setSchemaTemplateStatement.getTemplateName();
    PartialPath path = setSchemaTemplateStatement.getPath();
    try {
      // Send request to some API server
      ClusterTemplateManager.getInstance().setSchemaTemplate(queryId, templateName, path);
      // build TSBlock
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Throwable e) {
      if (e.getCause() instanceof IoTDBException) {
        future.setException(e.getCause());
      } else {
        future.setException(e);
      }
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      // Send request to some API server
      List<PartialPath> listPath =
          ClusterTemplateManager.getInstance()
              .getPathsSetTemplate(
                  showPathSetTemplateStatement.getTemplateName(),
                  showPathSetTemplateStatement.getAuthorityScope());
      // Build TSBlock
      ShowPathSetTemplateTask.buildTSBlock(listPath, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deactivateSchemaTemplate(
      String queryId, DeactivateTemplateStatement deactivateTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TDeactivateSchemaTemplateReq req = new TDeactivateSchemaTemplateReq();
    req.setQueryId(queryId);
    req.setTemplateName(deactivateTemplateStatement.getTemplateName());
    req.setPathPatternTree(
        serializePatternListToByteBuffer(deactivateTemplateStatement.getPathPatternList()));
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.deactivateSchemaTemplate(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // Time out mainly caused by slow execution, just wait
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // Keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute deactivate device template {} from {} in config node, status is {}.",
            deactivateTemplateStatement.getTemplateName(),
            deactivateTemplateStatement.getPathPatternList(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TSStatus tsStatus =
          configNodeClient.dropSchemaTemplate(dropSchemaTemplateStatement.getTemplateName());
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute drop device template {} in config node, status is {}.",
            dropSchemaTemplateStatement.getTemplateName(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> alterSchemaTemplate(
      String queryId, AlterSchemaTemplateStatement alterSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    if (alterSchemaTemplateStatement
        .getOperationType()
        .equals(TemplateAlterOperationType.EXTEND_TEMPLATE)) {
      // check duplicate measurement
      TemplateExtendInfo templateExtendInfo =
          (TemplateExtendInfo) alterSchemaTemplateStatement.getTemplateAlterInfo();
      String duplicateMeasurement = templateExtendInfo.getFirstDuplicateMeasurement();
      if (duplicateMeasurement != null) {
        future.setException(
            new MetadataException(
                String.format(
                    "Duplicated measurement [%s] in device template alter request",
                    duplicateMeasurement)));
        return future;
      }
      // check schema quota
      long localNeedQuota =
          (long) templateExtendInfo.getMeasurements().size()
              * SchemaEngine.getInstance()
                  .getSchemaEngineStatistics()
                  .getTemplateUsingNumber(templateExtendInfo.getTemplateName());
      if (localNeedQuota != 0) {
        try {

          DataNodeSchemaQuotaManager.getInstance().check(localNeedQuota, 0);
        } catch (SchemaQuotaExceededException e) {
          future.setException(e);
          return future;
        }
      }
    }

    TAlterSchemaTemplateReq req = new TAlterSchemaTemplateReq();
    req.setQueryId(queryId);
    req.setTemplateAlterInfo(
        TemplateAlterOperationUtil.generateExtendTemplateReqInfo(
            alterSchemaTemplateStatement.getOperationType(),
            alterSchemaTemplateStatement.getTemplateAlterInfo()));
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.alterSchemaTemplate(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // Time out mainly caused by slow execution, just wait
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // Keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to alter device template {} in config node, status is {}.",
            alterSchemaTemplateStatement.getTemplateAlterInfo().getTemplateName(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  private ByteBuffer serializePatternListToByteBuffer(List<PartialPath> patternList) {
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath pathPattern : patternList) {
      patternTree.appendPathPattern(pathPattern);
    }
    patternTree.constructTree();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // memory operation, won't happen
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public SettableFuture<ConfigTaskResult> unsetSchemaTemplate(
      String queryId, UnsetSchemaTemplateStatement unsetSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TUnsetSchemaTemplateReq req = new TUnsetSchemaTemplateReq();
    req.setQueryId(queryId);
    req.setTemplateName(unsetSchemaTemplateStatement.getTemplateName());
    req.setPath(unsetSchemaTemplateStatement.getPath().getFullPath());
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.unsetSchemaTemplate(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute unset device template {} from {} in config node, status is {}.",
            unsetSchemaTemplateStatement.getTemplateName(),
            unsetSchemaTemplateStatement.getPath(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createPipe(CreatePipeStatement createPipeStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // Validate pipe name
    if (createPipeStatement.getPipeName().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
      String exceptionMessage =
          String.format(
              "Failed to create pipe %s, pipe name starting with \"%s\" are not allowed to be created.",
              createPipeStatement.getPipeName(), PipeStaticMeta.SYSTEM_PIPE_PREFIX);
      LOGGER.warn(exceptionMessage);
      future.setException(
          new IoTDBException(exceptionMessage, TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    // Validate pipe plugin before creation
    try {
      PipeAgent.plugin()
          .validate(
              createPipeStatement.getPipeName(),
              createPipeStatement.getExtractorAttributes(),
              createPipeStatement.getProcessorAttributes(),
              createPipeStatement.getConnectorAttributes());
    } catch (Exception e) {
      LOGGER.info("Failed to validate create pipe statement, because {}", e.getMessage(), e);
      future.setException(
          new IoTDBException(e.getMessage(), TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TCreatePipeReq req =
          new TCreatePipeReq()
              .setPipeName(createPipeStatement.getPipeName())
              .setExtractorAttributes(createPipeStatement.getExtractorAttributes())
              .setProcessorAttributes(createPipeStatement.getProcessorAttributes())
              .setConnectorAttributes(createPipeStatement.getConnectorAttributes());
      TSStatus tsStatus = configNodeClient.createPipe(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to create pipe {} in config node, status is {}.",
            createPipeStatement.getPipeName(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> alterPipe(AlterPipeStatement alterPipeStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // Validate pipe name
    if (alterPipeStatement.getPipeName().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
      String exceptionMessage =
          String.format(
              "Failed to alter pipe %s, pipe name starting with \"%s\" are not allowed to be altered.",
              alterPipeStatement.getPipeName(), PipeStaticMeta.SYSTEM_PIPE_PREFIX);
      LOGGER.warn(exceptionMessage);
      future.setException(
          new IoTDBException(exceptionMessage, TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    // Validate pipe plugin before alteration - only validate replace mode
    final String pipeName = alterPipeStatement.getPipeName();
    try {
      if (!alterPipeStatement.getProcessorAttributes().isEmpty()
          && alterPipeStatement.isReplaceAllProcessorAttributes()) {
        PipeAgent.plugin().validateProcessor(alterPipeStatement.getProcessorAttributes());
      }
      if (!alterPipeStatement.getConnectorAttributes().isEmpty()
          && alterPipeStatement.isReplaceAllConnectorAttributes()) {
        PipeAgent.plugin().validateConnector(pipeName, alterPipeStatement.getConnectorAttributes());
      }
    } catch (Exception e) {
      LOGGER.info("Failed to validate alter pipe statement, because {}", e.getMessage(), e);
      future.setException(
          new IoTDBException(e.getMessage(), TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TAlterPipeReq req =
          new TAlterPipeReq(
              pipeName,
              alterPipeStatement.getProcessorAttributes(),
              alterPipeStatement.getConnectorAttributes(),
              alterPipeStatement.isReplaceAllProcessorAttributes(),
              alterPipeStatement.isReplaceAllConnectorAttributes());
      final TSStatus tsStatus = configNodeClient.alterPipe(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn("Failed to alter pipe {} in config node, status is {}.", pipeName, tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> startPipe(StartPipeStatement startPipeStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // Validate pipe name
    if (startPipeStatement.getPipeName().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
      String exceptionMessage =
          String.format(
              "Failed to start pipe %s, pipe name starting with \"%s\" are not allowed to be started.",
              startPipeStatement.getPipeName(), PipeStaticMeta.SYSTEM_PIPE_PREFIX);
      LOGGER.warn(exceptionMessage);
      future.setException(
          new IoTDBException(exceptionMessage, TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus = configNodeClient.startPipe(startPipeStatement.getPipeName());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to start pipe {}, status is {}.", startPipeStatement.getPipeName(), tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropPipe(DropPipeStatement dropPipeStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // Validate pipe name
    if (dropPipeStatement.getPipeName().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
      String exceptionMessage =
          String.format(
              "Failed to drop pipe %s, pipe name starting with \"%s\" are not allowed to be dropped.",
              dropPipeStatement.getPipeName(), PipeStaticMeta.SYSTEM_PIPE_PREFIX);
      LOGGER.warn(exceptionMessage);
      future.setException(
          new IoTDBException(exceptionMessage, TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus tsStatus = configNodeClient.dropPipe(dropPipeStatement.getPipeName());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to drop pipe {}, status is {}.", dropPipeStatement.getPipeName(), tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> stopPipe(StopPipeStatement stopPipeStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // Validate pipe name
    if (stopPipeStatement.getPipeName().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
      String exceptionMessage =
          String.format(
              "Failed to stop pipe %s, pipe name starting with \"%s\" are not allowed to be stopped.",
              stopPipeStatement.getPipeName(), PipeStaticMeta.SYSTEM_PIPE_PREFIX);
      LOGGER.warn(exceptionMessage);
      future.setException(
          new IoTDBException(exceptionMessage, TSStatusCode.PIPE_ERROR.getStatusCode()));
      return future;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus tsStatus = configNodeClient.stopPipe(stopPipeStatement.getPipeName());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to stop pipe {}, status is {}.", stopPipeStatement.getPipeName(), tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPipes(ShowPipesStatement showPipesStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TShowPipeReq tShowPipeReq = new TShowPipeReq();
      if (showPipesStatement.getPipeName() != null) {
        tShowPipeReq.setPipeName(showPipesStatement.getPipeName());
      }
      if (showPipesStatement.getWhereClause()) {
        tShowPipeReq.setWhereClause(true);
      }
      final List<TShowPipeInfo> tShowPipeInfoList =
          configNodeClient.showPipe(tShowPipeReq).getPipeInfoList();
      ShowPipeTask.buildTSBlock(tShowPipeInfoList, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showSubscriptions(
      ShowSubscriptionsStatement showSubscriptionsStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TShowSubscriptionReq showSubscriptionReq = new TShowSubscriptionReq();
      if (showSubscriptionsStatement.getTopicName() != null) {
        showSubscriptionReq.setTopicName(showSubscriptionsStatement.getTopicName());
      }

      final TShowSubscriptionResp showSubscriptionResp =
          configNodeClient.showSubscription(showSubscriptionReq);
      if (showSubscriptionResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showSubscriptionResp.getStatus().getMessage(),
                showSubscriptionResp.getStatus().getCode()));
        return future;
      }

      ShowSubscriptionTask.buildTSBlock(
          showSubscriptionResp.isSetSubscriptionInfoList()
              ? showSubscriptionResp.getSubscriptionInfoList()
              : Collections.emptyList(),
          future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createTopic(CreateTopicStatement createTopicStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    final String topicName = createTopicStatement.getTopicName();
    final Map<String, String> topicAttributes = createTopicStatement.getTopicAttributes();

    // Validate topic config
    final TopicMeta temporaryTopicMeta =
        new TopicMeta(topicName, System.currentTimeMillis(), topicAttributes);
    try {
      PipeAgent.plugin().validateExtractor(temporaryTopicMeta.generateExtractorAttributes());
      PipeAgent.plugin().validateProcessor(temporaryTopicMeta.generateProcessorAttributes());
    } catch (Exception e) {
      LOGGER.info("Failed to validate create topic statement, because {}", e.getMessage(), e);
      future.setException(
          new IoTDBException(e.getMessage(), TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode()));
      return future;
    }

    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCreateTopicReq req =
          new TCreateTopicReq().setTopicName(topicName).setTopicAttributes(topicAttributes);
      final TSStatus tsStatus = configNodeClient.createTopic(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn("Failed to create topic {} in config node, status is {}.", topicName, tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropTopic(DropTopicStatement dropTopicStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus tsStatus = configNodeClient.dropTopic(dropTopicStatement.getTopicName());
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to drop topic {}, status is {}.", dropTopicStatement.getTopicName(), tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTopics(ShowTopicsStatement showTopicsStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TShowTopicReq showTopicReq = new TShowTopicReq();
      if (showTopicsStatement.getTopicName() != null) {
        showTopicReq.setTopicName(showTopicsStatement.getTopicName());
      }

      final TShowTopicResp showTopicResp = configNodeClient.showTopic(showTopicReq);
      if (showTopicResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showTopicResp.getStatus().getMessage(), showTopicResp.getStatus().getCode()));
        return future;
      }

      ShowTopicsTask.buildTSBlock(
          showTopicResp.isSetTopicInfoList()
              ? showTopicResp.getTopicInfoList()
              : Collections.emptyList(),
          future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteTimeSeries(
      String queryId, DeleteTimeSeriesStatement deleteTimeSeriesStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    final TDeleteTimeSeriesReq req =
        new TDeleteTimeSeriesReq(
            queryId,
            serializePatternListToByteBuffer(deleteTimeSeriesStatement.getPathPatternList()));
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.deleteTimeSeries(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute delete timeseries {} in config node, status is {}.",
            deleteTimeSeriesStatement.getPathPatternList(),
            tsStatus);
        if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          future.setException(
              new BatchProcessException(tsStatus.subStatus.toArray(new TSStatus[0])));
        } else {
          future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
        }
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteLogicalView(
      String queryId, DeleteLogicalViewStatement deleteLogicalViewStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    final TDeleteLogicalViewReq req =
        new TDeleteLogicalViewReq(
            queryId,
            serializePatternListToByteBuffer(deleteLogicalViewStatement.getPathPatternList()));
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.deleteLogicalView(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute delete view {}, status is {}.",
            deleteLogicalViewStatement.getPathPatternList(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> renameLogicalView(
      String queryId, RenameLogicalViewStatement renameLogicalViewStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    // check path
    final PartialPath oldName = renameLogicalViewStatement.getOldName();
    if (oldName.hasWildcard()) {
      future.setException(
          new MetadataException("Rename view doesn't support path pattern with wildcard."));
      return future;
    }

    // fetch viewExpression
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(oldName);
    patternTree.constructTree();
    final ISchemaTree schemaTree =
        ClusterSchemaFetcher.getInstance().fetchSchema(patternTree, true, null);
    final List<MeasurementPath> measurementPathList =
        schemaTree.searchMeasurementPaths(oldName).left;
    if (measurementPathList.isEmpty()) {
      future.setException(new PathNotExistException(oldName.getFullPath()));
      return future;
    }
    final LogicalViewSchema logicalViewSchema =
        (LogicalViewSchema) measurementPathList.get(0).getMeasurementSchema();
    final ViewExpression viewExpression = logicalViewSchema.getExpression();

    // create new view
    final CreateLogicalViewStatement createLogicalViewStatement = new CreateLogicalViewStatement();
    createLogicalViewStatement.setTargetFullPaths(
        Collections.singletonList(renameLogicalViewStatement.getNewName()));
    createLogicalViewStatement.setViewExpressions(Collections.singletonList(viewExpression));
    final ExecutionResult executionResult =
        Coordinator.getInstance()
            .executeForTreeModel(
                createLogicalViewStatement,
                0,
                null,
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.setException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
      return future;
    }

    // delete old view
    final TDeleteLogicalViewReq req =
        new TDeleteLogicalViewReq(
            queryId, serializePatternListToByteBuffer(Collections.singletonList(oldName)));
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.deleteLogicalView(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn("Failed to execute delete view {}, status is {}.", oldName, tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> alterLogicalView(
      AlterLogicalViewStatement alterLogicalViewStatement, MPPQueryContext context) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    final CreateLogicalViewStatement createLogicalViewStatement = new CreateLogicalViewStatement();
    createLogicalViewStatement.setTargetPaths(alterLogicalViewStatement.getTargetPaths());
    createLogicalViewStatement.setSourcePaths(alterLogicalViewStatement.getSourcePaths());
    createLogicalViewStatement.setQueryStatement(alterLogicalViewStatement.getQueryStatement());

    final Analysis analysis = Analyzer.analyze(createLogicalViewStatement, context);
    if (analysis.isFailed()) {
      future.setException(
          new IoTDBException(
              analysis.getFailStatus().getMessage(), analysis.getFailStatus().getCode()));
      return future;
    }

    // Transform all Expressions into ViewExpressions.
    final TransformToViewExpressionVisitor transformToViewExpressionVisitor =
        new TransformToViewExpressionVisitor();
    final List<Expression> expressionList = alterLogicalViewStatement.getSourceExpressionList();
    final List<ViewExpression> viewExpressionList = new ArrayList<>();
    for (Expression expression : expressionList) {
      viewExpressionList.add(transformToViewExpressionVisitor.process(expression, null));
    }

    final List<PartialPath> viewPathList = alterLogicalViewStatement.getTargetPathList();

    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(viewPathList.size(), stream);
      for (int i = 0; i < viewPathList.size(); i++) {
        viewPathList.get(i).serialize(stream);
        ViewExpression.serialize(viewExpressionList.get(i), stream);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TAlterLogicalViewReq req =
        new TAlterLogicalViewReq(
            context.getQueryId().getId(), ByteBuffer.wrap(stream.toByteArray()));
    try (final ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus;
      do {
        try {
          tsStatus = client.alterLogicalView(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute alter view {}, status is {}.",
            alterLogicalViewStatement.getTargetPathList(),
            tsStatus);
        if (tsStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
          future.setException(
              new BatchProcessException(tsStatus.subStatus.toArray(new TSStatus[0])));
        } else {
          future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
        }
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
      return future;
    } catch (ClientManagerException | TException e) {
      future.setException(e);
      return future;
    }
  }

  @Override
  public TSStatus alterLogicalViewByPipe(AlterLogicalViewNode alterLogicalViewNode) {
    final Map<PartialPath, ViewExpression> viewPathToSourceMap =
        alterLogicalViewNode.getViewPathToSourceMap();

    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(viewPathToSourceMap.size(), stream);
      for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
        entry.getKey().serialize(stream);
        ViewExpression.serialize(entry.getValue(), stream);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TAlterLogicalViewReq req =
        new TAlterLogicalViewReq(
                Coordinator.getInstance().createQueryId().getId(),
                ByteBuffer.wrap(stream.toByteArray()))
            .setIsGeneratedByPipe(true);
    TSStatus tsStatus;
    try (ConfigNodeClient client =
        CLUSTER_DELETION_CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      do {
        try {
          tsStatus = client.alterLogicalView(req);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT
              || e.getCause() instanceof SocketTimeoutException) {
            // time out mainly caused by slow execution, wait until
            tsStatus = RpcUtils.getStatus(TSStatusCode.OVERLAP_WITH_EXISTING_TASK);
          } else {
            throw e;
          }
        }
        // keep waiting until task ends
      } while (TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode() == tsStatus.getCode());

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.warn(
            "Failed to execute alter view {} by pipe, status is {}.",
            viewPathToSourceMap,
            tsStatus);
      }
    } catch (ClientManagerException | TException e) {
      tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      tsStatus.setMessage(e.toString());
    }
    return tsStatus;
  }

  @Override
  public SettableFuture<ConfigTaskResult> getRegionId(GetRegionIdStatement getRegionIdStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TGetRegionIdResp resp = new TGetRegionIdResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetRegionIdReq tGetRegionIdReq =
          new TGetRegionIdReq(getRegionIdStatement.getPartitionType());
      if (getRegionIdStatement.getDevice() != null) {
        tGetRegionIdReq.setDevice(getRegionIdStatement.getDevice());
      } else {
        tGetRegionIdReq.setDatabase(getRegionIdStatement.getDatabase());
      }
      tGetRegionIdReq.setStartTimeSlot(
          TimePartitionUtils.getTimePartitionSlot(getRegionIdStatement.getStartTimeStamp()));
      tGetRegionIdReq.setEndTimeSlot(
          TimePartitionUtils.getTimePartitionSlot(getRegionIdStatement.getEndTimeStamp()));
      resp = configNodeClient.getRegionId(tGetRegionIdReq);
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(new IoTDBException(resp.getStatus().message, resp.getStatus().code));
        return future;
      }
    } catch (Exception e) {
      future.setException(e);
    }
    GetRegionIdTask.buildTsBlock(resp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> getSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TGetSeriesSlotListResp resp = new TGetSeriesSlotListResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetSeriesSlotListReq tGetSeriesSlotListReq =
          new TGetSeriesSlotListReq(
              getSeriesSlotListStatement.getDatabase(),
              getSeriesSlotListStatement.getPartitionType());
      resp = configNodeClient.getSeriesSlotList(tGetSeriesSlotListReq);
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(new IoTDBException(resp.getStatus().message, resp.getStatus().code));
        return future;
      }
    } catch (Exception e) {
      future.setException(e);
    }
    GetSeriesSlotListTask.buildTsBlock(resp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> getTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TGetTimeSlotListResp resp = new TGetTimeSlotListResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetTimeSlotListReq tGetTimeSlotListReq = new TGetTimeSlotListReq();
      if (getTimeSlotListStatement.getDatabase() != null) {
        tGetTimeSlotListReq.setDatabase(getTimeSlotListStatement.getDatabase());
      } else if (getTimeSlotListStatement.getDevice() != null) {
        tGetTimeSlotListReq.setDevice(getTimeSlotListStatement.getDevice());
      } else if (getTimeSlotListStatement.getRegionId() != -1) {
        tGetTimeSlotListReq.setRegionId(getTimeSlotListStatement.getRegionId());
      }
      if (getTimeSlotListStatement.getStartTime() != -1) {
        tGetTimeSlotListReq.setStartTime(getTimeSlotListStatement.getStartTime());
      }
      if (getTimeSlotListStatement.getEndTime() != -1) {
        tGetTimeSlotListReq.setEndTime(getTimeSlotListStatement.getEndTime());
      }
      resp = configNodeClient.getTimeSlotList(tGetTimeSlotListReq);
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(new IoTDBException(resp.getStatus().message, resp.getStatus().code));
        return future;
      }
    } catch (Exception e) {
      future.setException(e);
    }
    GetTimeSlotListTask.buildTSBlock(resp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TCountTimeSlotListResp resp = new TCountTimeSlotListResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCountTimeSlotListReq tCountTimeSlotListReq = new TCountTimeSlotListReq();
      if (countTimeSlotListStatement.getDatabase() != null) {
        tCountTimeSlotListReq.setDatabase(countTimeSlotListStatement.getDatabase());
      } else if (countTimeSlotListStatement.getDevice() != null) {
        tCountTimeSlotListReq.setDevice(countTimeSlotListStatement.getDevice());
      } else if (countTimeSlotListStatement.getRegionId() != -1) {
        tCountTimeSlotListReq.setRegionId(countTimeSlotListStatement.getRegionId());
      }
      if (countTimeSlotListStatement.getStartTime() != -1) {
        tCountTimeSlotListReq.setStartTime(countTimeSlotListStatement.getStartTime());
      }
      if (countTimeSlotListStatement.getEndTime() != -1) {
        tCountTimeSlotListReq.setEndTime(countTimeSlotListStatement.getEndTime());
      }
      resp = configNodeClient.countTimeSlotList(tCountTimeSlotListReq);
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(new IoTDBException(resp.getStatus().message, resp.getStatus().code));
        return future;
      }
    } catch (Exception e) {
      future.setException(e);
    }
    CountTimeSlotListTask.buildTSBlock(resp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> migrateRegion(
      MigrateRegionStatement migrateRegionStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TMigrateRegionReq tMigrateRegionReq =
          new TMigrateRegionReq(
              migrateRegionStatement.getRegionId(),
              migrateRegionStatement.getFromId(),
              migrateRegionStatement.getToId());
      final TSStatus status = configNodeClient.migrateRegion(tMigrateRegionReq);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(new IoTDBException(status.message, status.code));
        return future;
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, MPPQueryContext context) {
    createContinuousQueryStatement.semanticCheck();

    final String queryBody = createContinuousQueryStatement.getQueryBody();
    // TODO Do not modify Statement in Analyzer
    Analyzer.analyze(createContinuousQueryStatement.getQueryBodyStatement(), context);

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCreateCQReq tCreateCQReq =
          new TCreateCQReq(
              createContinuousQueryStatement.getCqId(),
              createContinuousQueryStatement.getEveryInterval(),
              createContinuousQueryStatement.getBoundaryTime(),
              createContinuousQueryStatement.getStartTimeOffset(),
              createContinuousQueryStatement.getEndTimeOffset(),
              createContinuousQueryStatement.getTimeoutPolicy().getType(),
              queryBody,
              context.getSql(),
              context.getZoneId().getId(),
              context.getSession() == null ? null : context.getSession().getUserName());
      final TSStatus executionStatus = client.createCQ(tCreateCQReq);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn(
            "[{}] Failed to create continuous query {}. TSStatus is {}",
            executionStatus,
            createContinuousQueryStatement.getCqId(),
            executionStatus.message);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropContinuousQuery(String cqId) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus executionStatus = client.dropCQ(new TDropCQReq(cqId));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.warn("[{}] Failed to drop continuous query {}.", executionStatus, cqId);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showContinuousQueries() {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TShowCQResp showCQResp = client.showCQ();
      if (showCQResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(showCQResp.getStatus().message, showCQResp.getStatus().code));
        return future;
      }
      // convert cqList and buildTsBlock
      ShowContinuousQueriesTask.buildTsBlock(showCQResp.getCqList(), future);
    } catch (ClientManagerException | TException e) {
      future.setException(e);
    }

    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setSpaceQuota(
      SetSpaceQuotaStatement setSpaceQuotaStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    final TSetSpaceQuotaReq req = new TSetSpaceQuotaReq();
    req.setDatabase(setSpaceQuotaStatement.getPrefixPathList());
    final TSpaceQuota spaceQuota = new TSpaceQuota();
    spaceQuota.setDeviceNum(setSpaceQuotaStatement.getDeviceNum());
    spaceQuota.setTimeserieNum(setSpaceQuotaStatement.getTimeSeriesNum());
    spaceQuota.setDiskSize(setSpaceQuotaStatement.getDiskSize());
    req.setSpaceLimit(spaceQuota);
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      tsStatus = client.setSpaceQuota(req);
    } catch (Exception e) {
      future.setException(e);
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showSpaceQuota(
      ShowSpaceQuotaStatement showSpaceQuotaStatement) {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final List<String> databases = new ArrayList<>();
      if (showSpaceQuotaStatement.getDatabases() != null) {
        showSpaceQuotaStatement
            .getDatabases()
            .forEach(database -> databases.add(database.toString()));
      }
      // Send request to some API server
      final TSpaceQuotaResp showSpaceQuotaResp = configNodeClient.showSpaceQuota(databases);
      // build TSBlock
      ShowSpaceQuotaTask.buildTsBlock(showSpaceQuotaResp, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserName(setThrottleQuotaStatement.getUserName());
    TThrottleQuota throttleQuota = new TThrottleQuota();
    throttleQuota.setThrottleLimit(setThrottleQuotaStatement.getThrottleLimit());
    throttleQuota.setMemLimit(setThrottleQuotaStatement.getMemLimit());
    throttleQuota.setCpuLimit(setThrottleQuotaStatement.getCpuLimit());
    req.setThrottleQuota(throttleQuota);
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      tsStatus = client.setThrottleQuota(req);
    } catch (Exception e) {
      future.setException(e);
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TShowThrottleReq req = new TShowThrottleReq();
      req.setUserName(showThrottleQuotaStatement.getUserName());
      TThrottleQuotaResp throttleQuotaResp = configNodeClient.showThrottleQuota(req);
      // build TSBlock
      ShowThrottleQuotaTask.buildTSBlock(throttleQuotaResp, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public TThrottleQuotaResp getThrottleQuota() {
    TThrottleQuotaResp throttleQuotaResp = new TThrottleQuotaResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      throttleQuotaResp = configNodeClient.getThrottleQuota();
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return throttleQuotaResp;
  }

  @Override
  public TSpaceQuotaResp getSpaceQuota() {
    TSpaceQuotaResp spaceQuotaResp = new TSpaceQuotaResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      spaceQuotaResp = configNodeClient.getSpaceQuota();
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return spaceQuotaResp;
  }

  @Override
  public TPipeTransferResp handleTransferConfigPlan(String clientId, TPipeTransferReq req) {
    final TPipeConfigTransferReq configTransferReq =
        new TPipeConfigTransferReq(
            req.version,
            req.type,
            req.body,
            req instanceof AirGapPseudoTPipeTransferRequest,
            clientId);

    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TPipeConfigTransferResp pipeConfigTransferResp =
          configNodeClient.handleTransferConfigPlan(configTransferReq);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode()
          != pipeConfigTransferResp.getStatus().getCode()) {
        LOGGER.warn("Failed to handleTransferConfigPlan, status is {}.", pipeConfigTransferResp);
      }
      return new TPipeTransferResp(pipeConfigTransferResp.status)
          .setBody(pipeConfigTransferResp.body);
    } catch (Exception e) {
      return new TPipeTransferResp(
          new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
              .setMessage(e.toString()));
    }
  }

  @Override
  public void handlePipeConfigClientExit(String clientId) {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSStatus status = configNodeClient.handlePipeConfigClientExit(clientId);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
        LOGGER.warn("Failed to handlePipeConfigClientExit, status is {}.", status);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to handlePipeConfigClientExit.", e);
    }
  }
}
