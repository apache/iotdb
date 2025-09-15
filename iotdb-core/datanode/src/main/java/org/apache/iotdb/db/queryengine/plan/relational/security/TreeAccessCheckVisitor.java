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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowCurrentTimestampStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.ShowLogicalViewStatement;
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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;

/** userName in TreeAccessCheckContext will never be SUPER_USER */
public class TreeAccessCheckVisitor extends StatementVisitor<TSStatus, TreeAccessCheckContext> {

  @Override
  public TSStatus visitNode(StatementNode node, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(false, "Only the admin user can perform this operation");
  }

  @Override
  public TSStatus visitAuthorityInformation(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA));
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  // ====================== template related =================================

  @Override
  public TSStatus visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(createTemplateStatement, context);
  }

  @Override
  public TSStatus visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(setSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitActivateTemplate(
      ActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(showSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitDeactivateTemplate(
      DeactivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(unsetSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(dropSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(alterSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement,
      TreeAccessCheckContext context) {
    return visitAuthorityInformation(showPathsUsingTemplateStatement, context);
  }

  @Override
  public TSStatus visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, TreeAccessCheckContext context) {
    return visitNode(showPathSetTemplateStatement, context);
  }

  // ============================= timeseries view related ===============
  @Override
  public TSStatus visitCreateLogicalView(
      CreateLogicalViewStatement statement, TreeAccessCheckContext context) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PartialPath> sourcePathList = statement.getSourcePaths().fullPathList;
    if (sourcePathList != null) {
      status =
          AuthorityChecker.getTSStatus(
              AuthorityChecker.checkFullPathOrPatternListPermission(
                  context.userName, sourcePathList, PrivilegeType.READ_SCHEMA),
              sourcePathList,
              PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      sourcePathList = queryStatement.getPaths();
      status =
          AuthorityChecker.getTSStatus(
              AuthorityChecker.checkFullPathOrPatternListPermission(
                  context.userName, sourcePathList, PrivilegeType.READ_SCHEMA),
              sourcePathList,
              PrivilegeType.READ_SCHEMA);
    }

    final List<PartialPath> paths =
        Objects.nonNull(statement.getTargetPathList())
            ? statement.getTargetPathList()
            : Collections.singletonList(
                statement
                    .getBatchGenerationItem()
                    .getIntoDevice()
                    .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkFullPathOrPatternListPermission(
              context.userName, paths, PrivilegeType.WRITE_SCHEMA),
          paths,
          PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitDeleteLogicalView(
      DeleteLogicalViewStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowLogicalView(
      ShowLogicalViewStatement showLogicalViewStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showLogicalViewStatement, context);
  }

  @Override
  public TSStatus visitAlterLogicalView(
      AlterLogicalViewStatement statement, TreeAccessCheckContext context) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PartialPath> sourcePathList = statement.getSourcePaths().fullPathList;
    if (sourcePathList != null) {
      status =
          AuthorityChecker.getTSStatus(
              AuthorityChecker.checkFullPathOrPatternListPermission(
                  context.userName, sourcePathList, PrivilegeType.READ_SCHEMA),
              sourcePathList,
              PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      sourcePathList = queryStatement.getPaths();
      status =
          AuthorityChecker.getTSStatus(
              AuthorityChecker.checkFullPathOrPatternListPermission(
                  context.userName, sourcePathList, PrivilegeType.READ_SCHEMA),
              sourcePathList,
              PrivilegeType.READ_SCHEMA);
    }

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkFullPathOrPatternListPermission(
              context.userName, statement.getTargetPathList(), PrivilegeType.WRITE_SCHEMA),
          statement.getTargetPathList(),
          PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitRenameLogicalView(
      RenameLogicalViewStatement statement, TreeAccessCheckContext context) {
    List<PartialPath> checkedPaths =
        ImmutableList.of(statement.getOldName(), statement.getNewName());
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  // ===================================== security related ===============================
  @Override
  public TSStatus visitAuthor(AuthorStatement statement, TreeAccessCheckContext context) {
    AuthorType authorType = statement.getAuthorType();
    switch (authorType) {
      case CREATE_USER:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot create user has same name with admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_USER),
            PrivilegeType.MANAGE_USER);

      case UPDATE_USER:
        // users can change passwords of themselves
        if (AuthorityChecker.SUPER_USER.equals(context.userName)
            || statement.getUserName().equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_USER),
            PrivilegeType.MANAGE_USER);

      case DROP_USER:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())
            || statement.getUserName().equals(context.userName)) {
          return AuthorityChecker.getTSStatus(false, "Cannot drop admin user or yourself");
        }
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_USER),
            PrivilegeType.MANAGE_USER);

      case LIST_USER:
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_USER),
            PrivilegeType.MANAGE_USER);

      case LIST_USER_PRIVILEGE:
        if (AuthorityChecker.SUPER_USER.equals(context.userName)
            || context.userName.equals(statement.getUserName())) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_USER),
            PrivilegeType.MANAGE_USER);

      case LIST_ROLE_PRIVILEGE:
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        if (!AuthorityChecker.checkRole(context.userName, statement.getRoleName())) {
          return AuthorityChecker.getTSStatus(
              AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_ROLE),
              PrivilegeType.MANAGE_ROLE);
        } else {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }

      case LIST_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        if (statement.getUserName() != null && context.userName.equals(statement.getUserName())) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        } else {
          return AuthorityChecker.getTSStatus(
              AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_ROLE),
              PrivilegeType.MANAGE_ROLE);
        }

      case CREATE_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getRoleName())) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot create role has same name with admin user");
        }
      case DROP_ROLE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }
        return AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(context.userName, PrivilegeType.MANAGE_ROLE),
            PrivilegeType.MANAGE_ROLE);

      case REVOKE_USER:
      case GRANT_USER:
      case GRANT_ROLE:
      case REVOKE_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant/revoke privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(context.userName)) {
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }

        for (String s : statement.getPrivilegeList()) {
          PrivilegeType privilegeType = PrivilegeType.valueOf(s.toUpperCase());
          if (privilegeType.isSystemPrivilege()) {
            if (!AuthorityChecker.checkSystemPermissionGrantOption(
                context.userName, privilegeType)) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted)");
            }
          } else if (privilegeType.isPathPrivilege()) {
            if (!AuthorityChecker.checkPathPermissionGrantOption(
                context.userName, privilegeType, statement.getNodeNameList())) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted)");
            }
          } else {
            return AuthorityChecker.getTSStatus(
                false, "Not support Relation statement in tree sql_dialect");
          }
        }
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      default:
        throw new IllegalArgumentException("Unknown authorType: " + authorType);
    }
  }

  // =================================== CQ related ====================================
  @Override
  public TSStatus visitCreateContinuousQuery(
      CreateContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.userName);
  }

  @Override
  public TSStatus visitDropContinuousQuery(
      DropContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.userName);
  }

  @Override
  public TSStatus visitShowContinuousQueries(
      ShowContinuousQueriesStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.userName);
  }

  private TSStatus checkCQManagement(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)
            || AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_CQ),
        PrivilegeType.USE_CQ);
  }

  // =================================== UDF related ====================================
  @Override
  public TSStatus visitCreateFunction(
      CreateFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(context.userName);
  }

  @Override
  public TSStatus visitDropFunction(
      DropFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(context.userName);
  }

  @Override
  public TSStatus visitShowFunctions(
      ShowFunctionsStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(context.userName);
  }

  private TSStatus checkUDFManagement(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)
            || AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_UDF),
        PrivilegeType.USE_UDF);
  }

  // =================================== model related ====================================
  @Override
  public TSStatus visitCreateModel(CreateModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.userName);
  }

  @Override
  public TSStatus visitDropModel(DropModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.userName);
  }

  @Override
  public TSStatus visitShowModels(ShowModelsStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.userName);
  }

  private TSStatus checkModelManagement(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)
            || AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_MODEL),
        PrivilegeType.USE_MODEL);
  }

  // ================================ pipe plugin related ==================================
  @Override
  public TSStatus visitCreatePipePlugin(
      CreatePipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitDropPipePlugin(
      DropPipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitShowPipePlugins(
      ShowPipePluginsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  // =============================== pipe related ========================================

  @Override
  public TSStatus visitCreatePipe(CreatePipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitShowPipes(ShowPipesStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitDropPipe(DropPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitAlterPipe(AlterPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitStartPipe(StartPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitStopPipe(StopPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  private TSStatus checkPipeManagement(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)
            || AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  // =============================== subscription related ========================================

  @Override
  public TSStatus visitCreateTopic(CreateTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitShowTopics(ShowTopicsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitDropTopic(DropTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitShowSubscriptions(
      ShowSubscriptionsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  @Override
  public TSStatus visitDropSubscription(
      DropSubscriptionStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.userName);
  }

  // ======================= trigger related ================================
  @Override
  public TSStatus visitCreateTrigger(
      CreateTriggerStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.userName);
  }

  @Override
  public TSStatus visitDropTrigger(DropTriggerStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.userName);
  }

  @Override
  public TSStatus visitShowTriggers(
      ShowTriggersStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.userName);
  }

  private TSStatus checkTriggerManagement(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)
            || AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_TRIGGER),
        PrivilegeType.USE_TRIGGER);
  }

  // ============================== database related ===========================
  @Override
  public TSStatus visitSetDatabase(
      DatabaseSchemaStatement statement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(context.userName);
  }

  @Override
  public TSStatus visitAlterDatabase(
      DatabaseSchemaStatement databaseSchemaStatement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(context.userName);
  }

  @Override
  public TSStatus visitShowStorageGroup(
      ShowDatabaseStatement showDatabaseStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showDatabaseStatement, context);
  }

  @Override
  public TSStatus visitCountStorageGroup(
      CountDatabaseStatement countDatabaseStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(countDatabaseStatement, context);
  }

  @Override
  public TSStatus visitDeleteStorageGroup(
      DeleteDatabaseStatement statement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(context.userName);
  }

  private TSStatus checkCreateOrAlterDatabasePermission(String userName) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE),
        PrivilegeType.MANAGE_DATABASE);
  }

  // ==================================== data related ========================================
  @Override
  public TSStatus visitInsertBase(InsertBaseStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName,
        statement.getPaths().stream().distinct().collect(Collectors.toList()),
        PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitInsert(InsertStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitLoadFile(LoadTsFileStatement statement, TreeAccessCheckContext context) {
    // no need to check here, it will be checked in process phase
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitQuery(QueryStatement statement, TreeAccessCheckContext context) {
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA));
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitExplainAnalyze(
      ExplainAnalyzeStatement statement, TreeAccessCheckContext context) {
    return statement.getQueryStatement().accept(this, context);
  }

  // ============================= timeseries related =================================
  private TSStatus checkTimeSeriesPermission(
      String userName, List<? extends PartialPath> checkedPaths, PrivilegeType permission) {
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(userName, checkedPaths, permission),
        checkedPaths,
        permission);
  }

  @Override
  public TSStatus visitCreateTimeseries(
      CreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement statement, TreeAccessCheckContext context) {
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowTimeSeries(
      ShowTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA)));
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
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountLevelTimeSeries(
      CountLevelTimeSeriesStatement countStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitCountNodes(
      CountNodesStatement countStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showChildNodesStatement, context);
  }

  @Override
  public TSStatus visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showChildPathsStatement, context);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      AlterTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitDeleteTimeSeries(
      DeleteTimeSeriesStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.userName, statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  // ================================== maintain related =============================
  @Override
  public TSStatus visitExtendRegion(
      ExtendRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitGetRegionId(GetRegionIdStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitGetSeriesSlotList(
      GetSeriesSlotListStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitGetTimeSlotList(
      GetTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitCountTimeSlotList(
      CountTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitKillQuery(KillQueryStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitMigrateRegion(
      MigrateRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
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
  public TSStatus visitSetSqlDialect(
      SetSqlDialectStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
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
  public TSStatus visitShowQueries(ShowQueriesStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowRegion(ShowRegionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
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
  public TSStatus visitTestConnection(
      TestConnectionStatement statement, TreeAccessCheckContext context) {
    return AuthorityChecker.checkSuperUserOrMaintain(context.userName);
  }

  @Override
  public TSStatus visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showCurrentTimestampStatement, context);
  }

  // ======================== TTL related ===========================
  @Override
  public TSStatus visitSetTTL(SetTTLStatement statement, TreeAccessCheckContext context) {
    List<PartialPath> checkedPaths = statement.getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.userName, checkedPaths, PrivilegeType.WRITE_SCHEMA),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowTTL(ShowTTLStatement showTTLStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showTTLStatement, context);
  }

  @Override
  public TSStatus visitUnSetTTL(
      UnSetTTLStatement unSetTTLStatement, TreeAccessCheckContext context) {
    return visitSetTTL(unSetTTLStatement, context);
  }

  // ================================= device related =============================
  @Override
  public TSStatus visitShowDevices(ShowDevicesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountDevices(
      CountDevicesStatement statement, TreeAccessCheckContext context) {
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(context.userName, PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }
}
