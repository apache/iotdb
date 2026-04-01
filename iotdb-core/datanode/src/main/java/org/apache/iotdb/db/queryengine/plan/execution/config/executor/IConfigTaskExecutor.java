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

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.schema.cache.CacheClearOptions;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ExtendRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.MigrateRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ReconstructRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.RemoveRegionTask;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterEncodingCompressorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveAINodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropSubscriptionStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public interface IConfigTaskExecutor {

  SettableFuture<ConfigTaskResult> setDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> alterDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> showDatabase(ShowDatabaseStatement showDatabaseStatement);

  SettableFuture<ConfigTaskResult> countDatabase(CountDatabaseStatement countDatabaseStatement);

  SettableFuture<ConfigTaskResult> deleteDatabase(DeleteDatabaseStatement deleteDatabaseStatement);

  SettableFuture<ConfigTaskResult> createFunction(
      Model model,
      String udfName,
      String className,
      Optional<String> stringURI,
      Class<?> baseClazz);

  SettableFuture<ConfigTaskResult> dropFunction(Model model, String udfName);

  SettableFuture<ConfigTaskResult> showFunctions(Model model);

  SettableFuture<ConfigTaskResult> createTrigger(CreateTriggerStatement createTriggerStatement);

  SettableFuture<ConfigTaskResult> dropTrigger(String triggerName);

  SettableFuture<ConfigTaskResult> showTriggers();

  SettableFuture<ConfigTaskResult> createPipePlugin(CreatePipePluginStatement createPipeStatement);

  SettableFuture<ConfigTaskResult> dropPipePlugin(DropPipePluginStatement dropPipePluginStatement);

  SettableFuture<ConfigTaskResult> showPipePlugins(
      ShowPipePluginsStatement showPipePluginsStatement);

  SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName);

  SettableFuture<ConfigTaskResult> merge(boolean onCluster);

  SettableFuture<ConfigTaskResult> startRepairData(boolean onCluster);

  SettableFuture<ConfigTaskResult> stopRepairData(boolean onCluster);

  SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq, boolean onCluster);

  SettableFuture<ConfigTaskResult> clearCache(boolean onCluster, Set<CacheClearOptions> options);

  SettableFuture<ConfigTaskResult> setConfiguration(TSetConfigurationReq tSetConfigurationReq);

  SettableFuture<ConfigTaskResult> showAppliedConfigurations(
      ShowConfigurationStatement showConfigurationStatement);

  SettableFuture<ConfigTaskResult> loadConfiguration(boolean onCluster);

  SettableFuture<ConfigTaskResult> setSystemStatus(boolean onCluster, NodeStatus status);

  SettableFuture<ConfigTaskResult> killQuery(KillQueryStatement killQueryStatement);

  SettableFuture<ConfigTaskResult> showCluster(ShowClusterStatement showClusterStatement);

  SettableFuture<ConfigTaskResult> showClusterParameters();

  SettableFuture<ConfigTaskResult> showClusterId();

  SettableFuture<ConfigTaskResult> testConnection(boolean needDetails);

  SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement);

  SettableFuture<ConfigTaskResult> showRegion(
      final ShowRegionStatement showRegionStatement, final boolean isTableModel);

  SettableFuture<ConfigTaskResult> showDataNodes();

  SettableFuture<ConfigTaskResult> showAvailableUrls();

  SettableFuture<ConfigTaskResult> showConfigNodes();

  SettableFuture<ConfigTaskResult> showAINodes();

  SettableFuture<ConfigTaskResult> createSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> showSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> showNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> setSchemaTemplate(
      String queryId, SetSchemaTemplateStatement setSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> showPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement);

  SettableFuture<ConfigTaskResult> deactivateSchemaTemplate(
      String queryId, DeactivateTemplateStatement deactivateTemplateStatement);

  SettableFuture<ConfigTaskResult> unsetSchemaTemplate(
      String queryId, UnsetSchemaTemplateStatement unsetSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> dropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> alterSchemaTemplate(
      String queryId, AlterSchemaTemplateStatement alterSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> dropPipe(DropPipeStatement dropPipeStatement);

  SettableFuture<ConfigTaskResult> createPipe(CreatePipeStatement createPipeStatement);

  SettableFuture<ConfigTaskResult> alterPipe(AlterPipeStatement alterPipeStatement);

  SettableFuture<ConfigTaskResult> startPipe(StartPipeStatement startPipeStatement);

  SettableFuture<ConfigTaskResult> stopPipe(StopPipeStatement stopPipeStatement);

  SettableFuture<ConfigTaskResult> showPipes(
      ShowPipesStatement showPipesStatement, String userName);

  SettableFuture<ConfigTaskResult> showSubscriptions(
      ShowSubscriptionsStatement showSubscriptionsStatement);

  SettableFuture<ConfigTaskResult> dropSubscription(
      DropSubscriptionStatement dropSubscriptionStatement);

  SettableFuture<ConfigTaskResult> createTopic(CreateTopicStatement createTopicStatement);

  SettableFuture<ConfigTaskResult> dropTopic(DropTopicStatement dropTopicStatement);

  SettableFuture<ConfigTaskResult> showTopics(ShowTopicsStatement showTopicsStatement);

  SettableFuture<ConfigTaskResult> alterEncodingCompressor(
      String queryId, AlterEncodingCompressorStatement alterEncodingCompressorStatement);

  SettableFuture<ConfigTaskResult> deleteTimeSeries(
      String queryId, DeleteTimeSeriesStatement deleteTimeSeriesStatement);

  SettableFuture<ConfigTaskResult> deleteLogicalView(
      String queryId, DeleteLogicalViewStatement deleteLogicalViewStatement);

  SettableFuture<ConfigTaskResult> renameLogicalView(
      String queryId, RenameLogicalViewStatement renameLogicalViewStatement);

  SettableFuture<ConfigTaskResult> alterLogicalView(
      AlterLogicalViewStatement alterLogicalViewStatement, MPPQueryContext context);

  SettableFuture<ConfigTaskResult> alterTimeSeriesDataType(
      String queryId, AlterTimeSeriesStatement alterTimeSeriesStatement);

  TSStatus alterLogicalViewByPipe(
      AlterLogicalViewNode alterLogicalViewNode, boolean shouldMarkAsPipeRequest);

  SettableFuture<ConfigTaskResult> getRegionId(GetRegionIdStatement getRegionIdStatement);

  SettableFuture<ConfigTaskResult> getSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement);

  SettableFuture<ConfigTaskResult> getTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement);

  SettableFuture<ConfigTaskResult> countTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement);

  SettableFuture<ConfigTaskResult> migrateRegion(MigrateRegionTask migrateRegionTask);

  SettableFuture<ConfigTaskResult> reconstructRegion(ReconstructRegionTask reconstructRegionTask);

  SettableFuture<ConfigTaskResult> extendRegion(ExtendRegionTask extendRegionTask);

  SettableFuture<ConfigTaskResult> removeRegion(RemoveRegionTask removeRegionTask);

  SettableFuture<ConfigTaskResult> removeDataNode(RemoveDataNodeStatement removeDataNodeStatement);

  SettableFuture<ConfigTaskResult> removeConfigNode(
      RemoveConfigNodeStatement removeConfigNodeStatement);

  SettableFuture<ConfigTaskResult> removeAINode(RemoveAINodeStatement removeAINodeStatement);

  SettableFuture<ConfigTaskResult> createContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, MPPQueryContext context);

  SettableFuture<ConfigTaskResult> dropContinuousQuery(String cqId);

  SettableFuture<ConfigTaskResult> showContinuousQueries();

  SettableFuture<ConfigTaskResult> setSpaceQuota(SetSpaceQuotaStatement setSpaceQuotaStatement);

  SettableFuture<ConfigTaskResult> showSpaceQuota(ShowSpaceQuotaStatement showSpaceQuotaStatement);

  TSpaceQuotaResp getSpaceQuota();

  SettableFuture<ConfigTaskResult> setThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement);

  SettableFuture<ConfigTaskResult> showThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement);

  TThrottleQuotaResp getThrottleQuota();

  TPipeTransferResp handleTransferConfigPlan(String clientId, TPipeTransferReq req);

  void handlePipeConfigClientExit(String clientId);

  SettableFuture<ConfigTaskResult> createExternalService(String serviceName, String className);

  SettableFuture<ConfigTaskResult> startExternalService(String serviceName);

  SettableFuture<ConfigTaskResult> stopExternalService(String serviceName);

  SettableFuture<ConfigTaskResult> dropExternalService(String serviceName, boolean forcedly);

  SettableFuture<ConfigTaskResult> showExternalService(int dataNodeId);

  // =============================== table syntax =========================================

  SettableFuture<ConfigTaskResult> showDatabases(
      final ShowDB showDB, final Predicate<String> canSeenDB);

  SettableFuture<ConfigTaskResult> showCluster(ShowCluster showCluster);

  SettableFuture<ConfigTaskResult> useDatabase(final Use useDB, final IClientSession clientSession);

  SettableFuture<ConfigTaskResult> dropDatabase(final DropDB dropDB, final IClientSession session);

  SettableFuture<ConfigTaskResult> createDatabase(
      final TDatabaseSchema databaseSchema, final boolean ifNotExists);

  SettableFuture<ConfigTaskResult> alterDatabase(
      final TDatabaseSchema databaseSchema, final boolean ifNotExists);

  SettableFuture<ConfigTaskResult> createTable(
      final TsTable table, final String database, final boolean ifNotExists);

  SettableFuture<ConfigTaskResult> describeTable(
      final String database,
      final String tableName,
      final boolean isDetails,
      final Boolean isShowCreateView);

  SettableFuture<ConfigTaskResult> showTables(
      final String database, final Predicate<String> canSeenDB, final boolean isDetails);

  TFetchTableResp fetchTables(final Map<String, Set<String>> fetchTableMap);

  SettableFuture<ConfigTaskResult> alterTableRenameTable(
      final String database,
      final String sourceName,
      final String targetName,
      final String queryId,
      final boolean tableIfExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableAddColumn(
      final String database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList,
      final String queryId,
      final boolean tableIfExists,
      final boolean columnIfExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterColumnDataType(
      final String database,
      final String tableName,
      final String columnName,
      final TSDataType newType,
      final String queryId,
      final boolean tableIfExists,
      boolean ifColumnExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableRenameColumn(
      final String database,
      final String tableName,
      final String oldName,
      final String newName,
      final String queryId,
      final boolean tableIfExists,
      final boolean columnIfExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableDropColumn(
      final String database,
      final String tableName,
      final String columnName,
      final String queryId,
      final boolean tableIfExists,
      final boolean columnIfExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableSetProperties(
      final String database,
      final String tableName,
      final Map<String, String> properties,
      final String queryId,
      final boolean ifExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableCommentTable(
      final String database,
      final String tableName,
      final String queryId,
      final boolean ifExists,
      final String comment,
      final boolean isView);

  SettableFuture<ConfigTaskResult> alterTableCommentColumn(
      final String database,
      final String tableName,
      final String columnName,
      final String queryId,
      final boolean tableIfExists,
      final boolean columnIfExists,
      final String comment,
      final boolean isView);

  SettableFuture<ConfigTaskResult> dropTable(
      final String database,
      final String tableName,
      final String queryId,
      final boolean ifExists,
      final boolean isView);

  SettableFuture<ConfigTaskResult> deleteDevice(
      final DeleteDevice deleteDevice, final String queryId, final SessionInfo sessionInfo);

  SettableFuture<ConfigTaskResult> createTableView(
      final TsTable table, final String database, final boolean replace);

  SettableFuture<ConfigTaskResult> showVersion();

  SettableFuture<ConfigTaskResult> showCurrentSqlDialect(String sqlDialect);

  SettableFuture<ConfigTaskResult> setSqlDialect(IClientSession.SqlDialect sqlDialect);

  SettableFuture<ConfigTaskResult> showCurrentUser(String currentUser);

  SettableFuture<ConfigTaskResult> showCurrentDatabase(@Nullable String currentDatabase);

  SettableFuture<ConfigTaskResult> showCurrentTimestamp();

  // =============================== AI =========================================
  SettableFuture<ConfigTaskResult> createModel(String modelId, String uri);

  SettableFuture<ConfigTaskResult> dropModel(String modelId);

  SettableFuture<ConfigTaskResult> showModels(String modelId);

  SettableFuture<ConfigTaskResult> showLoadedModels(List<String> deviceIdList);

  SettableFuture<ConfigTaskResult> showAIDevices();

  SettableFuture<ConfigTaskResult> loadModel(String existingModelId, List<String> deviceIdList);

  SettableFuture<ConfigTaskResult> unloadModel(String existingModelId, List<String> deviceIdList);

  SettableFuture<ConfigTaskResult> createTuningTask(
      String modelId,
      boolean isTableModel,
      Map<String, String> parameters,
      List<List<Long>> timeRanges,
      String existingModelId,
      @Nullable String targetSql,
      @Nullable List<String> pathList);
}
