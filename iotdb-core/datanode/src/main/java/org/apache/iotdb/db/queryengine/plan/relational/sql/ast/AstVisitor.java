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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;

public interface AstVisitor<R, C> extends CommonQueryAstVisitor<R, C> {

  default R visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  default R visitCopyTo(CopyTo node, C context) {
    return visitStatement(node, context);
  }

  default R visitExplainAnalyze(ExplainAnalyze node, C context) {
    return visitStatement(node, context);
  }

  default R visitUse(Use node, C context) {
    return visitStatement(node, context);
  }

  default R visitPrepare(Prepare node, C context) {
    return visitStatement(node, context);
  }

  default R visitExecute(Execute node, C context) {
    return visitStatement(node, context);
  }

  default R visitExecuteImmediate(ExecuteImmediate node, C context) {
    return visitStatement(node, context);
  }

  default R visitDeallocate(Deallocate node, C context) {
    return visitStatement(node, context);
  }

  default R visitColumnDefinition(ColumnDefinition node, C context) {
    return visitNode(node, context);
  }

  default R visitViewFieldDefinition(ViewFieldDefinition node, C context) {
    return visitNode(node, context);
  }

  default R visitCreateDB(final CreateDB node, final C context) {
    return visitStatement(node, context);
  }

  default R visitAlterDB(final AlterDB node, final C context) {
    return visitStatement(node, context);
  }

  default R visitDropDB(final DropDB node, final C context) {
    return visitStatement(node, context);
  }

  default R visitShowDB(final ShowDB node, final C context) {
    return visitStatement(node, context);
  }

  default R visitCreateTable(final CreateTable node, final C context) {
    return visitStatement(node, context);
  }

  default R visitCreateView(final CreateView node, final C context) {
    return visitStatement(node, context);
  }

  default R visitProperty(final Property node, final C context) {
    return visitNode(node, context);
  }

  default R visitDropTable(final DropTable node, final C context) {
    return visitStatement(node, context);
  }

  default R visitDeleteDevice(final DeleteDevice node, final C context) {
    return visitStatement(node, context);
  }

  default R visitShowTables(ShowTables node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowCluster(ShowCluster node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowRegions(ShowRegions node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowDataNodes(ShowDataNodes node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowAvailableUrls(ShowAvailableUrls node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowConfigNodes(ShowConfigNodes node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowAINodes(ShowAINodes node, C context) {
    return visitStatement(node, context);
  }

  default R visitRemoveAINode(RemoveAINode node, C context) {
    return visitStatement(node, context);
  }

  default R visitClearCache(ClearCache node, C context) {
    return visitStatement(node, context);
  }

  default R visitRenameTable(RenameTable node, C context) {
    return visitStatement(node, context);
  }

  default R visitRemoveDataNode(RemoveDataNode node, C context) {
    return visitStatement(node, context);
  }

  default R visitRemoveConfigNode(RemoveConfigNode node, C context) {
    return visitStatement(node, context);
  }

  default R visitDescribeTable(DescribeTable node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetProperties(SetProperties node, C context) {
    return visitStatement(node, context);
  }

  default R visitRenameColumn(RenameColumn node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropColumn(DropColumn node, C context) {
    return visitStatement(node, context);
  }

  default R visitAddColumn(AddColumn node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetTableComment(SetTableComment node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetColumnComment(SetColumnComment node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateIndex(CreateIndex node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropIndex(DropIndex node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowIndex(ShowIndex node, C context) {
    return visitStatement(node, context);
  }

  default R visitInsert(Insert node, C context) {
    return visitStatement(node, context);
  }

  default R visitInsertTablet(InsertTablet node, C context) {
    return visitStatement(node, context);
  }

  default R visitFlush(Flush node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetConfiguration(SetConfiguration node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowConfiguration(ShowConfiguration node, C context) {
    return visitStatement(node, context);
  }

  default R visitStartRepairData(StartRepairData node, C context) {
    return visitStatement(node, context);
  }

  default R visitStopRepairData(StopRepairData node, C context) {
    return visitStatement(node, context);
  }

  default R visitLoadConfiguration(LoadConfiguration node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetSystemStatus(SetSystemStatus node, C context) {
    return visitStatement(node, context);
  }

  default R visitInsertRow(InsertRow node, C context) {
    return visitStatement(node, context);
  }

  default R visitInsertRows(InsertRows node, C context) {
    return visitStatement(node, context);
  }

  default R visitDelete(Delete node, C context) {
    return visitStatement(node, context);
  }

  default R visitUpdate(Update node, C context) {
    return visitStatement(node, context);
  }

  default R visitUpdateAssignment(UpdateAssignment node, C context) {
    return visitNode(node, context);
  }

  default R visitShowFunctions(ShowFunctions node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateFunction(CreateFunction node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropFunction(DropFunction node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateExternalService(CreateExternalService node, C context) {
    return visitStatement(node, context);
  }

  default R visitStartExternalService(StartExternalService node, C context) {
    return visitStatement(node, context);
  }

  default R visitStopExternalService(StopExternalService node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropExternalService(DropExternalService node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowExternalService(ShowExternalService node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateOrUpdateDevice(CreateOrUpdateDevice node, C context) {
    return visitStatement(node, context);
  }

  default R visitFetchDevice(FetchDevice node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowDevice(ShowDevice node, C context) {
    return visitStatement(node, context);
  }

  default R visitCountDevice(CountDevice node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreatePipe(CreatePipe node, C context) {
    return visitStatement(node, context);
  }

  default R visitAlterPipe(AlterPipe node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropPipe(DropPipe node, C context) {
    return visitStatement(node, context);
  }

  default R visitStartPipe(StartPipe node, C context) {
    return visitStatement(node, context);
  }

  default R visitStopPipe(StopPipe node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowPipes(ShowPipes node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreatePipePlugin(CreatePipePlugin node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropPipePlugin(DropPipePlugin node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowPipePlugins(ShowPipePlugins node, C context) {
    return visitStatement(node, context);
  }

  default R visitLoadTsFile(LoadTsFile node, C context) {
    return visitStatement(node, context);
  }

  default R visitPipeEnriched(PipeEnriched node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateTopic(CreateTopic node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropTopic(DropTopic node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowTopics(ShowTopics node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowSubscriptions(ShowSubscriptions node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropSubscription(DropSubscription node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowVersion(ShowVersion node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowCurrentUser(ShowCurrentUser node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowCurrentDatabase(ShowCurrentDatabase node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowCurrentSqlDialect(ShowCurrentSqlDialect node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowVariables(ShowVariables node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowClusterId(ShowClusterId node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowCurrentTimestamp(ShowCurrentTimestamp node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowStatement(ShowStatement node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowQueriesStatement(ShowQueriesStatement node, C context) {
    return visitShowStatement(node, context);
  }

  default R visitCountStatement(CountStatement node, C context) {
    return visitStatement(node, context);
  }

  default R visitKillQuery(KillQuery node, C context) {
    return visitStatement(node, context);
  }

  default R visitAlterColumnDataType(AlterColumnDataType node, C context) {
    return visitStatement(node, context);
  }

  default R visitRelationalAuthorPlan(RelationalAuthorStatement node, C context) {
    return visitStatement(node, context);
  }

  default R visitMigrateRegion(MigrateRegion node, C context) {
    return visitStatement(node, context);
  }

  default R visitReconstructRegion(ReconstructRegion node, C context) {
    return visitStatement(node, context);
  }

  default R visitExtendRegion(ExtendRegion node, C context) {
    return visitStatement(node, context);
  }

  default R visitRemoveRegion(RemoveRegion node, C context) {
    return visitStatement(node, context);
  }

  default R visitSetSqlDialect(SetSqlDialect node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateTraining(CreateTraining node, C context) {
    return visitStatement(node, context);
  }

  default R visitCreateModel(CreateModel node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowModels(ShowModels node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowLoadedModels(ShowLoadedModels node, C context) {
    return visitStatement(node, context);
  }

  default R visitShowAIDevices(ShowAIDevices node, C context) {
    return visitStatement(node, context);
  }

  default R visitLoadModel(LoadModel node, C context) {
    return visitStatement(node, context);
  }

  default R visitUnloadModel(UnloadModel node, C context) {
    return visitStatement(node, context);
  }

  default R visitDropModel(DropModel node, C context) {
    return visitStatement(node, context);
  }
}
