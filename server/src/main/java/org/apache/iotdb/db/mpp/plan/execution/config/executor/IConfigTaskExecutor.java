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

package org.apache.iotdb.db.mpp.plan.execution.config.executor;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreatePipePluginStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.CreatePipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.DropPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.ShowPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StartPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StopPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.DropPipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkStatement;

import com.google.common.util.concurrent.SettableFuture;

public interface IConfigTaskExecutor {

  SettableFuture<ConfigTaskResult> setDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> alterDatabase(DatabaseSchemaStatement databaseSchemaStatement);

  SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement);

  SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement);

  SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement);

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
      SetSchemaTemplateStatement setSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> showPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement);

  SettableFuture<ConfigTaskResult> deactivateSchemaTemplate(
      String queryId, DeactivateTemplateStatement deactivateTemplateStatement);

  SettableFuture<ConfigTaskResult> unsetSchemaTemplate(
      String queryId, UnsetSchemaTemplateStatement unsetSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> dropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement);

  SettableFuture<ConfigTaskResult> createPipeSink(CreatePipeSinkStatement createPipeSinkStatement);

  SettableFuture<ConfigTaskResult> dropPipeSink(DropPipeSinkStatement dropPipeSinkStatement);

  SettableFuture<ConfigTaskResult> showPipeSink(ShowPipeSinkStatement showPipeSinkStatement);

  SettableFuture<ConfigTaskResult> dropPipe(DropPipeStatement dropPipeStatement);

  SettableFuture<ConfigTaskResult> createPipe(CreatePipeStatement createPipeStatement);

  SettableFuture<ConfigTaskResult> startPipe(StartPipeStatement startPipeStatement);

  SettableFuture<ConfigTaskResult> stopPipe(StopPipeStatement stopPipeStatement);

  SettableFuture<ConfigTaskResult> showPipe(ShowPipeStatement showPipeStatement);

  SettableFuture<ConfigTaskResult> deleteTimeSeries(
      String queryId, DeleteTimeSeriesStatement deleteTimeSeriesStatement);

  SettableFuture<ConfigTaskResult> getRegionId(GetRegionIdStatement getRegionIdStatement);

  SettableFuture<ConfigTaskResult> getSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement);

  SettableFuture<ConfigTaskResult> getTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement);

  SettableFuture<ConfigTaskResult> migrateRegion(MigrateRegionStatement migrateRegionStatement);

  SettableFuture<ConfigTaskResult> createContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, String sql, String username);

  SettableFuture<ConfigTaskResult> dropContinuousQuery(String cqId);

  SettableFuture<ConfigTaskResult> showContinuousQueries();
}
