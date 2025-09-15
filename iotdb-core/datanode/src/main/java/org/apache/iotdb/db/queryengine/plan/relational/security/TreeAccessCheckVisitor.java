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

package org.apache.iotdb.db.queryengine.plan.relational.security;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorityInformationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveAINodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.DropModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAINodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ExtendRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ReconstructRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.RemoveRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropSubscriptionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentUserStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.TestConnectionStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;

public class TreeAccessCheckVisitor extends StatementVisitor<TSStatus, TreeAccessCheckContext> {

  @Override
  public TSStatus visitNode(StatementNode node, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.SUPER_USER.equals(context.userName),
        "Only the admin user can perform this operation");
  }

  @Override
  public TSStatus visitActivateTemplate(
      ActivateTemplateStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitAlterLogicalView(
      AlterLogicalViewStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitAlterPipe(AlterPipeStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitAlterTimeSeries(
      AlterTimeSeriesStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitAuthor(AuthorStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitAuthorityInformation(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    try {
      if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
        statement.setAuthorityScope(
            AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA));
      }
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCountDevices(
      CountDevicesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
          statement.setAuthorityScope(
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_DATA)));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountTimeSeries(
      CountTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
          statement.setAuthorityScope(
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_DATA)));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountTimeSlotList(
      CountTimeSlotListStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateAlignedTimeSeries(
      CreateAlignedTimeSeriesStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateContinuousQuery(
      CreateContinuousQueryStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateFunction(
      CreateFunctionStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateLogicalView(
      CreateLogicalViewStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateModel(
      CreateModelStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreatePipePlugin(
      CreatePipePluginStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreatePipe(CreatePipeStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateTimeSeries(
      CreateTimeSeriesStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateTopic(
      CreateTopicStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitCreateTrigger(
      CreateTriggerStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDatabaseSchema(
      DatabaseSchemaStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDeactivateTemplate(
      DeactivateTemplateStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDeleteData(DeleteDataStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDeleteDatabase(
      DeleteDatabaseStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDeleteLogicalView(
      DeleteLogicalViewStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDeleteTimeSeries(
      DeleteTimeSeriesStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropContinuousQuery(
      DropContinuousQueryStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropFunction(
      DropFunctionStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropModel(DropModelStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropPipePlugin(
      DropPipePluginStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropPipe(DropPipeStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropSubscription(
      DropSubscriptionStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropTopic(DropTopicStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitDropTrigger(
      DropTriggerStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitExplainAnalyze(
      ExplainAnalyzeStatement statement, TreeAccessCheckContext context) {
    statement.getQueryStatement().accept(this, context);
  }

  @Override
  public TSStatus visitExtendRegion(
      ExtendRegionStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitGetRegionId(
      GetRegionIdStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitGetSeriesSlotList(
      GetSeriesSlotListStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitGetTimeSlotList(
      GetTimeSlotListStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitInsertBase(InsertBaseStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitInsert(InsertStatement statement, TreeAccessCheckContext context) {}

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitKillQuery(KillQueryStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitLoadFile(LoadTsFileStatement statement, TreeAccessCheckContext context) {
    // no need to check here, it will be checked in process phase
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitMigrateRegion(
      MigrateRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitQuery(QueryStatement statement, TreeAccessCheckContext context) {
    try {
      if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
        statement.setAuthorityScope(
            AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA));
      }
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitReconstructRegion(
      ReconstructRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitRemoveAINode(
      RemoveAINodeStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitRemoveConfigNode(
      RemoveConfigNodeStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitRemoveDataNode(
      RemoveDataNodeStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitRemoveRegion(
      RemoveRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitRenameLogicalView(
      RenameLogicalViewStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths =
        ImmutableList.of(statement.getOldName(), statement.getNewName());
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitSetSqlDialect(
      SetSqlDialectStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitSetTTL(SetTTLStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowAINodes(ShowAINodesStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowClusterId(
      ShowClusterIdStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowCluster(ShowClusterStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowConfigNodes(
      ShowConfigNodesStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowContinuousQueries(
      ShowContinuousQueriesStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_CQ),
        PrivilegeType.USE_CQ);
  }

  @Override
  public TSStatus visitShowCurrentSqlDialect(
      ShowCurrentSqlDialectStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowCurrentUser(
      ShowCurrentUserStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowDataNodes(
      ShowDataNodesStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowDevices(ShowDevicesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
          statement.setAuthorityScope(
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_DATA)));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitShowFunctions(
      ShowFunctionsStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_UDF),
        PrivilegeType.USE_UDF);
  }

  @Override
  public TSStatus visitShowModels(ShowModelsStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_MODEL),
        PrivilegeType.USE_MODEL);
  }

  @Override
  public TSStatus visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.SUPER_USER.equals(context.userName),
        "Only the admin user can perform this operation");
  }

  @Override
  public TSStatus visitShowPathSetTemplate(
      ShowPathSetTemplateStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.SUPER_USER.equals(context.userName),
        "Only the admin user can perform this operation");
  }

  @Override
  public TSStatus visitShowPipePlugins(
      ShowPipePluginsStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitShowPipes(ShowPipesStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitShowQueries(ShowQueriesStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowRegion(ShowRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowSchemaTemplate(
      ShowSchemaTemplateStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.SUPER_USER.equals(context.userName),
        "Only the admin user can perform this operation");
  }

  @Override
  public TSStatus visitShowSubscriptions(
      ShowSubscriptionsStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitShowTimeSeries(
      ShowTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(context.userName)) {
          statement.setAuthorityScope(
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(
                      context.userName, PrivilegeType.READ_DATA)));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitShowTopics(ShowTopicsStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitShowTriggers(
      ShowTriggersStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_TRIGGER),
        PrivilegeType.USE_TRIGGER);
  }

  @Override
  public TSStatus visitShowVariables(
      ShowVariablesStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowVersion(ShowVersionStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitStartPipe(StartPipeStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitStopPipe(StopPipeStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public TSStatus visitTestConnection(
      TestConnectionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }
}
