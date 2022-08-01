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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
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
  public IConfigTask visitSetStorageGroup(SetStorageGroupStatement statement, TaskContext context) {
    return new SetStorageGroupTask(statement);
  }

  @Override
  public IConfigTask visitDeleteStorageGroup(
      DeleteStorageGroupStatement statement, TaskContext context) {
    return new DeleteStorageGroupTask(statement);
  }

  @Override
  public IConfigTask visitShowStorageGroup(
      ShowStorageGroupStatement statement, TaskContext context) {
    return new ShowStorageGroupTask(statement);
  }

  @Override
  public IConfigTask visitCountStorageGroup(
      CountStorageGroupStatement statement, TaskContext context) {
    return new CountStorageGroupTask(statement);
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
  public IConfigTask visitShowCluster(
      ShowClusterStatement showClusterStatement, TaskContext context) {
    return new ShowClusterTask(showClusterStatement);
  }

  @Override
  public IConfigTask visitAuthor(AuthorStatement statement, TaskContext context) {
    return new AuthorizerTask(statement);
  }

  @Override
  public IConfigTask visitCreateFunction(
      CreateFunctionStatement createFunctionStatement, TaskContext context) {
    return new CreateFunctionTask(createFunctionStatement);
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
    return new SetSchemaTemplateTask(setSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, TaskContext context) {
    return new ShowPathSetTemplateTask(showPathSetTemplateStatement);
  }

  @Override
  public IConfigTask visitShowDataNodes(
      ShowDataNodesStatement showDataNodesStatement, TaskContext context) {
    return new ShowDataNodesTask(showDataNodesStatement);
  }

  @Override
  public IConfigTask visitShowConfigNodes(
      ShowConfigNodesStatement showConfigNodesStatement, TaskContext context) {
    return new ShowConfigNodesTask(showConfigNodesStatement);
  }

  public static class TaskContext {}
}
