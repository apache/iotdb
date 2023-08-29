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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateContinuousQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreatePipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateTriggerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DatabaseSchemaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DeleteStorageGroupTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DeleteTimeSeriesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropContinuousQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropPipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropTriggerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetRegionIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetSeriesSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.MigrateRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.SetTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowContinuousQueriesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTriggersTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowVariablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.UnSetTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.model.CreateModelTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.model.DropModelTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.model.ShowModelsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.model.ShowTrailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.AlterSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.CreateSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.DeactivateSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.DropSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.SetSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowNodesInSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowPathSetTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.UnsetSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.AlterLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.DeleteLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.RenameLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.AuthorizerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.ClearCacheTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.FlushTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.KillQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.LoadConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.MergeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetSystemStatusTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.CreatePipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.DropPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.ShowPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StartPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StopPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.SetSpaceQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.SetThrottleQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowSpaceQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowThrottleQuotaTask;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.DropModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowTrailsStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.MergeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

public class ConfigTaskVisitor
    extends StatementVisitor<IConfigTask, ConfigTaskVisitor.TaskContext> {

  @Override
  public IConfigTask visitNode(StatementNode node, TaskContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  public IConfigTask visitStatement(Statement statement, TaskContext context) {
    throw new NotImplementedException("ConfigTask is not implemented for: " + statement);
  }

  @Override
  public IConfigTask visitSetDatabase(DatabaseSchemaStatement statement, TaskContext context) {
    return new DatabaseSchemaTask(statement);
  }

  @Override
  public IConfigTask visitAlterDatabase(DatabaseSchemaStatement statement, TaskContext context) {
    return new DatabaseSchemaTask(statement);
  }

  @Override
  public IConfigTask visitDeleteStorageGroup(
      DeleteDatabaseStatement statement, TaskContext context) {
    return new DeleteStorageGroupTask(statement);
  }

  @Override
  public IConfigTask visitShowStorageGroup(ShowDatabaseStatement statement, TaskContext context) {
    return new ShowDatabaseTask(statement);
  }

  @Override
  public IConfigTask visitCountStorageGroup(CountDatabaseStatement statement, TaskContext context) {
    return new CountDatabaseTask(statement);
  }

  @Override
  public IConfigTask visitSetTTL(SetTTLStatement statement, TaskContext context) {
    return new SetTTLTask(statement);
  }

  @Override
  public IConfigTask visitUnSetTTL(UnSetTTLStatement statement, TaskContext context) {
    return new UnSetTTLTask(statement);
  }

  @Override
  public IConfigTask visitShowTTL(ShowTTLStatement showTTLStatement, TaskContext context) {
    return new ShowTTLTask(showTTLStatement);
  }

  @Override
  public IConfigTask visitShowVariables(
      ShowVariablesStatement showVariablesStatement, TaskContext context) {
    return new ShowVariablesTask();
  }

  @Override
  public IConfigTask visitShowCluster(
      ShowClusterStatement showClusterStatement, TaskContext context) {
    if (showClusterStatement.isDetails()) {
      return new ShowClusterDetailsTask(showClusterStatement);
    } else {
      return new ShowClusterTask(showClusterStatement);
    }
  }

  @Override
  public IConfigTask visitAuthor(AuthorStatement statement, TaskContext context) {
    return new AuthorizerTask(statement);
  }

  @Override
  public IConfigTask visitMerge(MergeStatement mergeStatement, TaskContext context) {
    return new MergeTask(mergeStatement);
  }

  @Override
  public IConfigTask visitFlush(FlushStatement flushStatement, TaskContext context) {
    return new FlushTask(flushStatement);
  }

  @Override
  public IConfigTask visitClearCache(ClearCacheStatement clearCacheStatement, TaskContext context) {
    return new ClearCacheTask(clearCacheStatement);
  }

  @Override
  public IConfigTask visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, TaskContext context) {
    return new LoadConfigurationTask(loadConfigurationStatement);
  }

  @Override
  public IConfigTask visitSetSystemStatus(
      SetSystemStatusStatement setSystemStatusStatement, TaskContext context) {
    return new SetSystemStatusTask(setSystemStatusStatement);
  }

  @Override
  public IConfigTask visitKillQuery(KillQueryStatement killQueryStatement, TaskContext context) {
    return new KillQueryTask(killQueryStatement);
  }

  @Override
  public IConfigTask visitCreateFunction(
      CreateFunctionStatement createFunctionStatement, TaskContext context) {
    return new CreateFunctionTask(createFunctionStatement);
  }

  @Override
  public IConfigTask visitDropFunction(
      DropFunctionStatement dropFunctionStatement, TaskContext context) {
    return new DropFunctionTask(dropFunctionStatement);
  }

  @Override
  public IConfigTask visitShowFunctions(
      ShowFunctionsStatement showFunctionsStatement, TaskContext context) {
    return new ShowFunctionsTask();
  }

  @Override
  public IConfigTask visitCreateTrigger(
      CreateTriggerStatement createTriggerStatement, TaskContext context) {
    return new CreateTriggerTask(createTriggerStatement);
  }

  @Override
  public IConfigTask visitDropTrigger(
      DropTriggerStatement dropTriggerStatement, TaskContext context) {
    return new DropTriggerTask(dropTriggerStatement);
  }

  @Override
  public IConfigTask visitShowTriggers(
      ShowTriggersStatement showTriggersStatement, TaskContext context) {
    return new ShowTriggersTask();
  }

  @Override
  public IConfigTask visitCreatePipePlugin(
      CreatePipePluginStatement createPipePluginStatement, TaskContext context) {
    return new CreatePipePluginTask(createPipePluginStatement);
  }

  @Override
  public IConfigTask visitDropPipePlugin(
      DropPipePluginStatement dropPipePluginStatement, TaskContext context) {
    return new DropPipePluginTask(dropPipePluginStatement);
  }

  @Override
  public IConfigTask visitShowPipePlugins(
      ShowPipePluginsStatement showPipePluginStatement, TaskContext context) {
    return new ShowPipePluginsTask();
  }

  @Override
  public IConfigTask visitShowRegion(ShowRegionStatement showRegionStatement, TaskContext context) {
    return new ShowRegionTask(showRegionStatement);
  }

  @Override
  public IConfigTask visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement, TaskContext context) {
    return new CreateSchemaTemplateTask(createSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement, TaskContext context) {
    return new ShowNodesInSchemaTemplateTask(showNodesInSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, TaskContext context) {
    return new ShowSchemaTemplateTask(showSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, TaskContext context) {
    return new SetSchemaTemplateTask(context.getQueryId(), setSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, TaskContext context) {
    return new ShowPathSetTemplateTask(showPathSetTemplateStatement);
  }

  @Override
  public IConfigTask visitDeactivateTemplate(
      DeactivateTemplateStatement deactivateTemplateStatement, TaskContext context) {
    return new DeactivateSchemaTemplateTask(context.getQueryId(), deactivateTemplateStatement);
  }

  @Override
  public IConfigTask visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, TaskContext context) {
    return new UnsetSchemaTemplateTask(context.getQueryId(), unsetSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, TaskContext context) {
    return new DropSchemaTemplateTask(dropSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, TaskContext context) {
    return new AlterSchemaTemplateTask(alterSchemaTemplateStatement, context.getQueryId());
  }

  @Override
  public IConfigTask visitShowDataNodes(
      ShowDataNodesStatement showDataNodesStatement, TaskContext context) {
    return new ShowDataNodesTask(showDataNodesStatement);
  }

  @Override
  public IConfigTask visitShowConfigNodes(
      ShowConfigNodesStatement showConfigNodesStatement, TaskContext context) {
    return new ShowConfigNodesTask();
  }

  @Override
  public IConfigTask visitShowPipes(ShowPipesStatement showPipesStatement, TaskContext context) {
    return new ShowPipeTask(showPipesStatement);
  }

  @Override
  public IConfigTask visitDropPipe(DropPipeStatement dropPipeStatement, TaskContext context) {
    return new DropPipeTask(dropPipeStatement);
  }

  @Override
  public IConfigTask visitCreatePipe(CreatePipeStatement createPipeStatement, TaskContext context) {
    return new CreatePipeTask(createPipeStatement);
  }

  @Override
  public IConfigTask visitStartPipe(StartPipeStatement startPipeStatement, TaskContext context) {
    return new StartPipeTask(startPipeStatement);
  }

  @Override
  public IConfigTask visitStopPipe(StopPipeStatement stopPipeStatement, TaskContext context) {
    return new StopPipeTask(stopPipeStatement);
  }

  @Override
  public IConfigTask visitDeleteTimeseries(
      DeleteTimeSeriesStatement deleteTimeSeriesStatement, TaskContext context) {
    return new DeleteTimeSeriesTask(context.getQueryId(), deleteTimeSeriesStatement);
  }

  @Override
  public IConfigTask visitDeleteLogicalView(
      DeleteLogicalViewStatement deleteLogicalViewStatement, TaskContext context) {
    return new DeleteLogicalViewTask(context.getQueryId(), deleteLogicalViewStatement);
  }

  @Override
  public IConfigTask visitRenameLogicalView(
      RenameLogicalViewStatement renameLogicalViewStatement, TaskContext context) {
    return new RenameLogicalViewTask(context.queryId, renameLogicalViewStatement);
  }

  @Override
  public IConfigTask visitAlterLogicalView(
      AlterLogicalViewStatement alterLogicalViewStatement, TaskContext context) {
    return new AlterLogicalViewTask(context.queryId, alterLogicalViewStatement);
  }

  @Override
  public IConfigTask visitGetRegionId(
      GetRegionIdStatement getRegionIdStatement, TaskContext context) {
    return new GetRegionIdTask(getRegionIdStatement);
  }

  @Override
  public IConfigTask visitGetSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement, TaskContext context) {
    return new GetSeriesSlotListTask(getSeriesSlotListStatement);
  }

  @Override
  public IConfigTask visitGetTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement, TaskContext context) {
    return new GetTimeSlotListTask(getTimeSlotListStatement);
  }

  @Override
  public IConfigTask visitCountTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement, TaskContext context) {
    return new CountTimeSlotListTask(countTimeSlotListStatement);
  }

  @Override
  public IConfigTask visitMigrateRegion(
      MigrateRegionStatement migrateRegionStatement, TaskContext context) {
    return new MigrateRegionTask(migrateRegionStatement);
  }

  @Override
  public IConfigTask visitCreateContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, TaskContext context) {
    return new CreateContinuousQueryTask(
        createContinuousQueryStatement, context.sql, context.username);
  }

  @Override
  public IConfigTask visitDropContinuousQuery(
      DropContinuousQueryStatement dropContinuousQueryStatement, TaskContext context) {
    return new DropContinuousQueryTask(dropContinuousQueryStatement);
  }

  @Override
  public IConfigTask visitShowContinuousQueries(
      ShowContinuousQueriesStatement showContinuousQueriesStatement, TaskContext context) {
    return new ShowContinuousQueriesTask();
  }

  @Override
  public IConfigTask visitSetSpaceQuota(
      SetSpaceQuotaStatement setSpaceQuotaStatement, TaskContext context) {
    return new SetSpaceQuotaTask(setSpaceQuotaStatement);
  }

  @Override
  public IConfigTask visitShowSpaceQuota(
      ShowSpaceQuotaStatement showSpaceQuotaStatement, TaskContext context) {
    return new ShowSpaceQuotaTask(showSpaceQuotaStatement);
  }

  @Override
  public IConfigTask visitSetThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement, TaskContext context) {
    return new SetThrottleQuotaTask(setThrottleQuotaStatement);
  }

  @Override
  public IConfigTask visitShowThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement, TaskContext context) {
    return new ShowThrottleQuotaTask(showThrottleQuotaStatement);
  }

  /** ML Model Management */
  @Override
  public IConfigTask visitCreateModel(
      CreateModelStatement createModelStatement, TaskContext context) {
    return new CreateModelTask(createModelStatement);
  }

  @Override
  public IConfigTask visitDropModel(DropModelStatement dropModelStatement, TaskContext context) {
    return new DropModelTask(dropModelStatement.getModelId());
  }

  @Override
  public IConfigTask visitShowModels(ShowModelsStatement showModelsStatement, TaskContext context) {
    return new ShowModelsTask();
  }

  @Override
  public IConfigTask visitShowTrails(ShowTrailsStatement showTrailsStatement, TaskContext context) {
    return new ShowTrailsTask(showTrailsStatement.getModelId());
  }

  public static class TaskContext {

    private final String queryId;

    private final String sql;

    private final String username;

    public TaskContext(String queryId, String sql, String username) {
      this.queryId = queryId;
      this.sql = sql;
      this.username = username;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getSql() {
      return sql;
    }

    public String getUsername() {
      return username;
    }
  }
}
