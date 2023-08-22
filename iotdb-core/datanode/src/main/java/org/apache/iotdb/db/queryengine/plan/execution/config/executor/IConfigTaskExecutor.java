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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreatePipePluginStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;

import com.google.common.util.concurrent.SettableFuture;

public interface IConfigTaskExecutor {

  SettableFuture<ConfigTaskResult> setDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> alterDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> showDatabase(ShowDatabaseStatement showDatabaseStatement);

  SettableFuture<ConfigTaskResult> countDatabase(CountDatabaseStatement countDatabaseStatement);

  SettableFuture<ConfigTaskResult> deleteDatabase(DeleteDatabaseStatement deleteDatabaseStatement);

  SettableFuture<ConfigTaskResult> createFunction(CreateFunctionStatement createFunctionStatement);

  SettableFuture<ConfigTaskResult> dropFunction(String udfName);

  SettableFuture<ConfigTaskResult> showFunctions();

  SettableFuture<ConfigTaskResult> createTrigger(CreateTriggerStatement createTriggerStatement);

  SettableFuture<ConfigTaskResult> dropTrigger(String triggerName);

  SettableFuture<ConfigTaskResult> showTriggers();

  SettableFuture<ConfigTaskResult> createPipePlugin(CreatePipePluginStatement createPipeStatement);

  SettableFuture<ConfigTaskResult> dropPipePlugin(String pluginName);

  SettableFuture<ConfigTaskResult> showPipePlugins();

  SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName);

  SettableFuture<ConfigTaskResult> merge(boolean onCluster);

  SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq, boolean onCluster);

  SettableFuture<ConfigTaskResult> clearCache(boolean onCluster);

  SettableFuture<ConfigTaskResult> loadConfiguration(boolean onCluster);

  SettableFuture<ConfigTaskResult> setSystemStatus(boolean onCluster, NodeStatus status);

  SettableFuture<ConfigTaskResult> killQuery(KillQueryStatement killQueryStatement);

  SettableFuture<ConfigTaskResult> showCluster(ShowClusterStatement showClusterStatement);

  SettableFuture<ConfigTaskResult> showClusterParameters();

  SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement);

  SettableFuture<ConfigTaskResult> showRegion(ShowRegionStatement showRegionStatement);

  SettableFuture<ConfigTaskResult> showDataNodes(ShowDataNodesStatement showDataNodesStatement);

  SettableFuture<ConfigTaskResult> showConfigNodes();

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

  SettableFuture<ConfigTaskResult> startPipe(StartPipeStatement startPipeStatement);

  SettableFuture<ConfigTaskResult> stopPipe(StopPipeStatement stopPipeStatement);

  SettableFuture<ConfigTaskResult> showPipes(ShowPipesStatement showPipesStatement);

  SettableFuture<ConfigTaskResult> deleteTimeSeries(
      String queryId, DeleteTimeSeriesStatement deleteTimeSeriesStatement);

  SettableFuture<ConfigTaskResult> deleteLogicalView(
      String queryId, DeleteLogicalViewStatement deleteLogicalViewStatement);

  SettableFuture<ConfigTaskResult> renameLogicalView(
      String queryId, RenameLogicalViewStatement renameLogicalViewStatement);

  SettableFuture<ConfigTaskResult> alterLogicalView(
      String queryId, AlterLogicalViewStatement alterLogicalViewStatement);

  SettableFuture<ConfigTaskResult> getRegionId(GetRegionIdStatement getRegionIdStatement);

  SettableFuture<ConfigTaskResult> getSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement);

  SettableFuture<ConfigTaskResult> getTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement);

  SettableFuture<ConfigTaskResult> countTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement);

  SettableFuture<ConfigTaskResult> migrateRegion(MigrateRegionStatement migrateRegionStatement);

  SettableFuture<ConfigTaskResult> createContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, String sql, String username);

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

  SettableFuture<ConfigTaskResult> createModel(CreateModelStatement createModelStatement);

  SettableFuture<ConfigTaskResult> dropModel(String modelId);

  SettableFuture<ConfigTaskResult> showModels();

  SettableFuture<ConfigTaskResult> showTrails(String modelId);
}
