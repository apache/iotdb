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

package org.apache.iotdb.db.mpp.plan.statement;

import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreatePipePluginStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropPipePluginStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowPipePluginsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.MergeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.CreatePipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.DropPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.ShowPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StartPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StopPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.DropPipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkTypeStatement;

/**
 * This class provides a visitor of {@link StatementNode}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class StatementVisitor<R, C> {

  public R process(StatementNode node, C context) {
    return node.accept(this, context);
  }

  /** Top Level Description */
  public abstract R visitNode(StatementNode node, C context);

  public R visitStatement(Statement statement, C context) {
    return visitNode(statement, context);
  }

  /** Data Definition Language (DDL) */

  // Create Timeseries
  public R visitCreateTimeseries(CreateTimeSeriesStatement createTimeSeriesStatement, C context) {
    return visitStatement(createTimeSeriesStatement, context);
  }

  // Create Aligned Timeseries
  public R visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, C context) {
    return visitStatement(createAlignedTimeSeriesStatement, context);
  }

  // Create Timeseries by device
  public R visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement, C context) {
    return visitStatement(internalCreateTimeSeriesStatement, context);
  }

  // Create Multi Timeseries
  public R visitCreateMultiTimeseries(
      CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, C context) {
    return visitStatement(createMultiTimeSeriesStatement, context);
  }

  // Alter Timeseries
  public R visitAlterTimeseries(AlterTimeSeriesStatement alterTimeSeriesStatement, C context) {
    return visitStatement(alterTimeSeriesStatement, context);
  }

  public R visitDeleteTimeseries(DeleteTimeSeriesStatement deleteTimeSeriesStatement, C context) {
    return visitStatement(deleteTimeSeriesStatement, context);
  }

  public R visitDeleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement, C context) {
    return visitStatement(deleteStorageGroupStatement, context);
  }

  public R visitSetDatabase(DatabaseSchemaStatement databaseSchemaStatement, C context) {
    return visitStatement(databaseSchemaStatement, context);
  }

  public R visitAlterDatabase(DatabaseSchemaStatement databaseSchemaStatement, C context) {
    return visitStatement(databaseSchemaStatement, context);
  }

  // Alter TTL
  public R visitSetTTL(SetTTLStatement setTTLStatement, C context) {
    return visitStatement(setTTLStatement, context);
  }

  public R visitUnSetTTL(UnSetTTLStatement unSetTTLStatement, C context) {
    return visitStatement(unSetTTLStatement, context);
  }

  public R visitShowTTL(ShowTTLStatement showTTLStatement, C context) {
    return visitStatement(showTTLStatement, context);
  }

  public R visitShowVariables(ShowVariablesStatement showVariablesStatement, C context) {
    return visitStatement(showVariablesStatement, context);
  }

  public R visitShowCluster(ShowClusterStatement showClusterStatement, C context) {
    return visitStatement(showClusterStatement, context);
  }

  // UDF
  public R visitCreateFunction(CreateFunctionStatement createFunctionStatement, C context) {
    return visitStatement(createFunctionStatement, context);
  }

  public R visitDropFunction(DropFunctionStatement dropFunctionStatement, C context) {
    return visitStatement(dropFunctionStatement, context);
  }

  public R visitShowFunctions(ShowFunctionsStatement showFunctionsStatement, C context) {
    return visitStatement(showFunctionsStatement, context);
  }

  // Trigger
  public R visitCreateTrigger(CreateTriggerStatement createTriggerStatement, C context) {
    return visitStatement(createTriggerStatement, context);
  }

  public R visitDropTrigger(DropTriggerStatement dropTriggerStatement, C context) {
    return visitStatement(dropTriggerStatement, context);
  }

  public R visitShowTriggers(ShowTriggersStatement showTriggersStatement, C context) {
    return visitStatement(showTriggersStatement, context);
  }

  // Pipe Plugin
  public R visitCreatePipePlugin(CreatePipePluginStatement createPipePluginStatement, C context) {
    return visitStatement(createPipePluginStatement, context);
  }

  public R visitDropPipePlugin(DropPipePluginStatement dropPipePluginStatement, C context) {
    return visitStatement(dropPipePluginStatement, context);
  }

  public R visitShowPipePlugins(ShowPipePluginsStatement showPipePluginsStatement, C context) {
    return visitStatement(showPipePluginsStatement, context);
  }

  /** Data Manipulation Language (DML) */

  // Select Statement
  public R visitQuery(QueryStatement queryStatement, C context) {
    return visitStatement(queryStatement, context);
  }

  // Insert Statement
  public R visitInsert(InsertStatement insertStatement, C context) {
    return visitStatement(insertStatement, context);
  }

  public R visitInsertTablet(InsertTabletStatement insertTabletStatement, C context) {
    return visitStatement(insertTabletStatement, context);
  }

  public R visitLoadFile(LoadTsFileStatement loadTsFileStatement, C context) {
    return visitStatement(loadTsFileStatement, context);
  }

  /** Data Control Language (DCL) */
  public R visitAuthor(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  public R visitShowStorageGroup(ShowStorageGroupStatement showStorageGroupStatement, C context) {
    return visitStatement(showStorageGroupStatement, context);
  }

  public R visitShowTimeSeries(ShowTimeSeriesStatement showTimeSeriesStatement, C context) {
    return visitStatement(showTimeSeriesStatement, context);
  }

  public R visitShowDevices(ShowDevicesStatement showDevicesStatement, C context) {
    return visitStatement(showDevicesStatement, context);
  }

  public R visitCountStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement, C context) {
    return visitStatement(countStorageGroupStatement, context);
  }

  public R visitCountDevices(CountDevicesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountTimeSeries(CountTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountLevelTimeSeries(CountLevelTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountNodes(CountNodesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitInsertRow(InsertRowStatement insertRowStatement, C context) {
    return visitStatement(insertRowStatement, context);
  }

  public R visitInsertRows(InsertRowsStatement insertRowsStatement, C context) {
    return visitStatement(insertRowsStatement, context);
  }

  public R visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, C context) {
    return visitStatement(insertMultiTabletsStatement, context);
  }

  public R visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, C context) {
    return visitStatement(insertRowsOfOneDeviceStatement, context);
  }

  public R visitSchemaFetch(SchemaFetchStatement schemaFetchStatement, C context) {
    return visitStatement(schemaFetchStatement, context);
  }

  public R visitShowChildPaths(ShowChildPathsStatement showChildPathsStatement, C context) {
    return visitStatement(showChildPathsStatement, context);
  }

  public R visitShowChildNodes(ShowChildNodesStatement showChildNodesStatement, C context) {
    return visitStatement(showChildNodesStatement, context);
  }

  public R visitExplain(ExplainStatement explainStatement, C context) {
    return visitStatement(explainStatement, context);
  }

  public R visitDeleteData(DeleteDataStatement deleteDataStatement, C context) {
    return visitStatement(deleteDataStatement, context);
  }

  public R visitMerge(MergeStatement mergeStatement, C context) {
    return visitStatement(mergeStatement, context);
  }

  public R visitFlush(FlushStatement flushStatement, C context) {
    return visitStatement(flushStatement, context);
  }

  public R visitClearCache(ClearCacheStatement clearCacheStatement, C context) {
    return visitStatement(clearCacheStatement, context);
  }

  public R visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, C context) {
    return visitStatement(loadConfigurationStatement, context);
  }

  public R visitSetSystemStatus(SetSystemStatusStatement setSystemStatusStatement, C context) {
    return visitStatement(setSystemStatusStatement, context);
  }

  public R visitKillQuery(KillQueryStatement killQueryStatement, C context) {
    return visitStatement(killQueryStatement, context);
  }

  public R visitShowQueries(ShowQueriesStatement showQueriesStatement, C context) {
    return visitStatement(showQueriesStatement, context);
  }

  public R visitShowRegion(ShowRegionStatement showRegionStatement, C context) {
    return visitStatement(showRegionStatement, context);
  }

  public R visitShowDataNodes(ShowDataNodesStatement showDataNodesStatement, C context) {
    return visitStatement(showDataNodesStatement, context);
  }

  public R visitShowConfigNodes(ShowConfigNodesStatement showConfigNodesStatement, C context) {
    return visitStatement(showConfigNodesStatement, context);
  }

  public R visitShowVersion(ShowVersionStatement showVersionStatement, C context) {
    return visitStatement(showVersionStatement, context);
  }

  public R visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, C context) {
    return visitStatement(createTemplateStatement, context);
  }

  public R visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement, C context) {
    return visitStatement(showNodesInSchemaTemplateStatement, context);
  }

  public R visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, C context) {
    return visitStatement(showSchemaTemplateStatement, context);
  }

  public R visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, C context) {
    return visitStatement(setSchemaTemplateStatement, context);
  }

  public R visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, C context) {
    return visitStatement(showPathSetTemplateStatement, context);
  }

  public R visitActivateTemplate(ActivateTemplateStatement activateTemplateStatement, C context) {
    return visitStatement(activateTemplateStatement, context);
  }

  public R visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, C context) {
    return visitStatement(showPathsUsingTemplateStatement, context);
  }

  public R visitShowPipeSink(ShowPipeSinkStatement showPipeSinkStatement, C context) {
    return visitStatement(showPipeSinkStatement, context);
  }

  public R visitShowPipeSinkType(ShowPipeSinkTypeStatement showPipeSinkTypeStatement, C context) {
    return visitStatement(showPipeSinkTypeStatement, context);
  }

  public R visitShowPipe(ShowPipeStatement showPipeStatement, C context) {
    return visitStatement(showPipeStatement, context);
  }

  public R visitCreatePipe(CreatePipeStatement createPipeStatement, C context) {
    return visitStatement(createPipeStatement, context);
  }

  public R visitCreatePipeSink(CreatePipeSinkStatement createPipeSinkStatement, C context) {
    return visitStatement(createPipeSinkStatement, context);
  }

  public R visitDropPipeSink(DropPipeSinkStatement dropPipeSinkStatement, C context) {
    return visitStatement(dropPipeSinkStatement, context);
  }

  public R visitDropPipe(DropPipeStatement dropPipeStatement, C context) {
    return visitStatement(dropPipeStatement, context);
  }

  public R visitStartPipe(StartPipeStatement startPipeStatement, C context) {
    return visitStatement(startPipeStatement, context);
  }

  public R visitStopPipe(StopPipeStatement stopPipeStatement, C context) {
    return visitStatement(stopPipeStatement, context);
  }

  public R visitGetRegionId(GetRegionIdStatement getRegionIdStatement, C context) {
    return visitStatement(getRegionIdStatement, context);
  }

  public R visitGetSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement, C context) {
    return visitStatement(getSeriesSlotListStatement, context);
  }

  public R visitGetTimeSlotList(GetTimeSlotListStatement getTimeSlotListStatement, C context) {
    return visitStatement(getTimeSlotListStatement, context);
  }

  public R visitMigrateRegion(MigrateRegionStatement migrateRegionStatement, C context) {
    return visitStatement(migrateRegionStatement, context);
  }

  public R visitDeactivateTemplate(
      DeactivateTemplateStatement deactivateTemplateStatement, C context) {
    return visitStatement(deactivateTemplateStatement, context);
  }

  public R visitCreateContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, C context) {
    return visitStatement(createContinuousQueryStatement, context);
  }

  public R visitDropContinuousQuery(
      DropContinuousQueryStatement dropContinuousQueryStatement, C context) {
    return visitStatement(dropContinuousQueryStatement, context);
  }

  public R visitShowContinuousQueries(
      ShowContinuousQueriesStatement showContinuousQueriesStatement, C context) {
    return visitStatement(showContinuousQueriesStatement, context);
  }

  public R visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, C context) {
    return visitStatement(unsetSchemaTemplateStatement, context);
  }

  public R visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, C context) {
    return visitStatement(dropSchemaTemplateStatement, context);
  }

  public R visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement internalBatchActivateTemplateStatement, C context) {
    return visitStatement(internalBatchActivateTemplateStatement, context);
  }

  public R visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement, C context) {
    return visitStatement(internalCreateMultiTimeSeriesStatement, context);
  }
}
