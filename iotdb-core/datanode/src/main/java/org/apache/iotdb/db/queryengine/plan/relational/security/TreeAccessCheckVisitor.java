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
import org.apache.iotdb.commons.audit.IAuditEntity;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateTrainingStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.DropModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.LoadModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAIDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAINodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowLoadedModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.UnloadModelStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentUserStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StartRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StopRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.TestConnectionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE_PATH;
import static org.apache.iotdb.commons.schema.table.Audit.includeByAuditTreeDB;
import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;
import static org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl.READ_ONLY_DB_ERROR_MSG;

public class TreeAccessCheckVisitor extends StatementVisitor<TSStatus, TreeAccessCheckContext> {

  @Override
  public TSStatus visitNode(StatementNode node, TreeAccessCheckContext context) {
    throw new IllegalStateException("Each operation should have permission check.");
  }

  @Override
  public TSStatus visitAuthorityInformation(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.getUsername(), PrivilegeType.READ_SCHEMA));
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  // ====================== template related =================================

  @Override
  public TSStatus visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(context.getUsername());
  }

  @Override
  public TSStatus visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, TreeAccessCheckContext context) {
    // root.__audit can never be set template
    TSStatus status = checkWriteOnReadOnlyPath(setSchemaTemplateStatement.getPath());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    return checkSystemAuth(context.getUsername());
  }

  @Override
  public TSStatus visitActivateTemplate(
      ActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  private TSStatus checkTemplateShowRelated(
      ShowSchemaTemplateStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAll(true);
      return SUCCEED;
    }
    // own SYSTEM can see all, otherwise can only see PATHS that user has READ_SCHEMA auth
    if (!checkHasGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM)) {
      statement.setCanSeeAll(false);
      return visitAuthorityInformation(statement, context);
    } else {
      statement.setCanSeeAll(true);
      return SUCCEED;
    }
  }

  @Override
  public TSStatus visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkTemplateShowRelated(showSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement,
      TreeAccessCheckContext context) {
    return checkTemplateShowRelated(showNodesInSchemaTemplateStatement, context);
  }

  @Override
  public TSStatus visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, TreeAccessCheckContext context) {
    return checkTemplateShowRelated(showPathSetTemplateStatement, context);
  }

  @Override
  public TSStatus visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement,
      TreeAccessCheckContext context) {
    return visitAuthorityInformation(showPathsUsingTemplateStatement, context);
  }

  @Override
  public TSStatus visitDeactivateTemplate(
      DeactivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(context.getUsername());
  }

  @Override
  public TSStatus visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(context.getUsername());
  }

  @Override
  public TSStatus visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkCanAlterTemplate(context.getUserEntity());
  }

  public TSStatus checkCanAlterTemplate(IAuditEntity entity) {
    if (AuthorityChecker.SUPER_USER.equals(entity.getUsername())) {
      return SUCCEED;
    }
    return checkGlobalAuth(entity.getUsername(), PrivilegeType.EXTEND_TEMPLATE);
  }

  // ============================= timeseries view related ===============
  @Override
  public TSStatus visitCreateLogicalView(
      CreateLogicalViewStatement statement, TreeAccessCheckContext context) {
    final List<PartialPath> paths =
        Objects.nonNull(statement.getTargetPathList())
            ? statement.getTargetPathList()
            : Collections.singletonList(
                statement
                    .getBatchGenerationItem()
                    .getIntoDevice()
                    .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    for (PartialPath path : paths) {
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(true);
      }
      return SUCCEED;
    }
    if (!checkHasGlobalAuth(context.getUsername(), PrivilegeType.AUDIT)) {
      statement.setCanSeeAuditDB(false);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(false);
      }
    }

    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PartialPath> sourcePathList = statement.getSourcePaths().fullPathList;
    if (sourcePathList != null) {
      status =
          checkTimeSeriesPermission(
              context.getUsername(), sourcePathList, PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      sourcePathList = queryStatement.getPaths();
      status =
          checkTimeSeriesPermission(
              context.getUsername(), sourcePathList, PrivilegeType.READ_SCHEMA);
    }

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return checkTimeSeriesPermission(context.getUsername(), paths, PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitDeleteLogicalView(
      DeleteLogicalViewStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowLogicalView(
      ShowLogicalViewStatement showLogicalViewStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showLogicalViewStatement, context);
  }

  @Override
  public TSStatus visitAlterLogicalView(
      AlterLogicalViewStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(true);
      }
      return SUCCEED;
    }
    if (!checkHasGlobalAuth(context.getUsername(), PrivilegeType.AUDIT)) {
      statement.setCanSeeAuditDB(false);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(false);
      }
    }

    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<PartialPath> sourcePathList = statement.getSourcePaths().fullPathList;
    if (sourcePathList != null) {
      status =
          checkTimeSeriesPermission(
              context.getUsername(), sourcePathList, PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      sourcePathList = queryStatement.getPaths();
      status =
          checkTimeSeriesPermission(
              context.getUsername(), sourcePathList, PrivilegeType.READ_SCHEMA);
    }

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return checkTimeSeriesPermission(
          context.getUsername(), statement.getTargetPathList(), PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitRenameLogicalView(
      RenameLogicalViewStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getNewName())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(),
        ImmutableList.of(statement.getOldName(), statement.getNewName()),
        PrivilegeType.WRITE_SCHEMA);
  }

  // ===================================== security related ===============================
  @Override
  public TSStatus visitAuthor(AuthorStatement statement, TreeAccessCheckContext context) {
    AuthorType authorType = statement.getAuthorType();
    switch (authorType) {
      case CREATE_USER:
      case DROP_USER:
        return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_USER);
      case UPDATE_USER:
        // users can change passwords of themselves
        if (statement.getUserName().equals(context.getUsername())) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_USER);

      case LIST_USER:
        if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_USER)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        statement.setUserName(context.getUsername());
        return RpcUtils.SUCCESS_STATUS;

      case LIST_USER_PRIVILEGE:
        if (context.getUsername().equals(statement.getUserName())) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_USER);

      case LIST_ROLE_PRIVILEGE:
        if (!AuthorityChecker.checkRole(context.getUsername(), statement.getRoleName())) {
          return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_ROLE);
        } else {
          return SUCCEED;
        }

      case LIST_ROLE:
        if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_ROLE)) {
          return SUCCEED;
        }
        // list roles of other user is not allowed
        if (statement.getUserName() != null
            && !statement.getUserName().equals(context.getUsername())) {
          return AuthorityChecker.getTSStatus(false, PrivilegeType.MANAGE_ROLE);
        }
        statement.setUserName(context.getUsername());
        return RpcUtils.SUCCESS_STATUS;

      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_ROLE);

      case REVOKE_USER:
      case GRANT_USER:
      case GRANT_ROLE:
      case REVOKE_ROLE:
        if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.SECURITY)) {
          return RpcUtils.SUCCESS_STATUS;
        }

        for (String s : statement.getPrivilegeList()) {
          PrivilegeType privilegeType = PrivilegeType.valueOf(s.toUpperCase());
          if (privilegeType.isSystemPrivilege()) {
            if (!checkHasGlobalAuth(context.getUsername(), privilegeType)) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted)");
            }
          } else if (privilegeType.isPathPrivilege()) {
            if (!AuthorityChecker.checkPathPermissionGrantOption(
                context.getUsername(), privilegeType, statement.getNodeNameList())) {
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
        return RpcUtils.SUCCESS_STATUS;
      default:
        throw new IllegalArgumentException("Unknown authorType: " + authorType);
    }
  }

  // =================================== CQ related ====================================
  @Override
  public TSStatus visitCreateContinuousQuery(
      CreateContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropContinuousQuery(
      DropContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowContinuousQueries(
      ShowContinuousQueriesStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(context.getUsername());
  }

  private TSStatus checkCQManagement(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }
    return checkGlobalAuth(userName, PrivilegeType.USE_CQ);
  }

  // =================================== UDF related ====================================
  @Override
  public TSStatus visitCreateFunction(
      CreateFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropFunction(
      DropFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowFunctions(
      ShowFunctionsStatement statement, TreeAccessCheckContext context) {
    // anyone can show functions
    return SUCCEED;
  }

  private TSStatus checkUDFManagement(String userName) {
    return checkGlobalAuth(userName, PrivilegeType.USE_UDF);
  }

  // =================================== model related ====================================
  @Override
  public TSStatus visitCreateModel(CreateModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropModel(DropModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitCreateTraining(
      CreateTrainingStatement createTrainingStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitUnloadModel(
      UnloadModelStatement unloadModelStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitLoadModel(
      LoadModelStatement loadModelStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowAIDevices(
      ShowAIDevicesStatement showAIDevicesStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowLoadedModels(
      ShowLoadedModelsStatement showLoadedModelsStatement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowModels(ShowModelsStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  private TSStatus checkModelManagement(String userName) {
    return checkGlobalAuth(userName, PrivilegeType.USE_MODEL);
  }

  // ================================ pipe plugin related ==================================
  @Override
  public TSStatus visitCreatePipePlugin(
      CreatePipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropPipePlugin(
      DropPipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowPipePlugins(
      ShowPipePluginsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  // =============================== pipe related ========================================

  @Override
  public TSStatus visitCreatePipe(CreatePipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowPipes(ShowPipesStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropPipe(DropPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitAlterPipe(AlterPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitStartPipe(StartPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitStopPipe(StopPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  private TSStatus checkPipeManagement(String userName) {
    return checkGlobalAuth(userName, PrivilegeType.USE_PIPE);
  }

  // =============================== subscription related ========================================

  @Override
  public TSStatus visitCreateTopic(CreateTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowTopics(ShowTopicsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropTopic(DropTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowSubscriptions(
      ShowSubscriptionsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropSubscription(
      DropSubscriptionStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(context.getUsername());
  }

  // ======================= trigger related ================================
  @Override
  public TSStatus visitCreateTrigger(
      CreateTriggerStatement statement, TreeAccessCheckContext context) {
    if (TREE_MODEL_AUDIT_DATABASE_PATH.include(statement.getPathPattern())) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTriggerManagement(context.getUsername());
  }

  @Override
  public TSStatus visitDropTrigger(DropTriggerStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.getUsername());
  }

  @Override
  public TSStatus visitShowTriggers(
      ShowTriggersStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.getUsername());
  }

  private TSStatus checkTriggerManagement(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }
    return checkGlobalAuth(userName, PrivilegeType.USE_TRIGGER);
  }

  // ============================== database related ===========================
  @Override
  public TSStatus visitSetDatabase(
      DatabaseSchemaStatement statement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(context.getUsername(), statement.getDatabasePath());
  }

  @Override
  public TSStatus visitAlterDatabase(
      DatabaseSchemaStatement databaseSchemaStatement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(
        context.getUsername(), databaseSchemaStatement.getDatabasePath());
  }

  @Override
  public TSStatus visitShowStorageGroup(
      ShowDatabaseStatement showDatabaseStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    setCanSeeAuditDB(showDatabaseStatement, context.getUsername());
    return checkShowOrCountDatabasePermission(showDatabaseStatement, context);
  }

  @Override
  public TSStatus visitCountStorageGroup(
      CountDatabaseStatement countDatabaseStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    setCanSeeAuditDB(countDatabaseStatement, context.getUsername());
    return checkShowOrCountDatabasePermission(countDatabaseStatement, context);
  }

  @Override
  public TSStatus visitDeleteStorageGroup(
      DeleteDatabaseStatement statement, TreeAccessCheckContext context) {
    for (String prefixPath : statement.getPrefixPath()) {
      // root.__audit can never be deleted
      if (TREE_MODEL_AUDIT_DATABASE.equals(prefixPath)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_DATABASE);
  }

  protected TSStatus checkCreateOrAlterDatabasePermission(
      String userName, PartialPath databaseName) {
    // root.__audit can never be created or alter
    if (TREE_MODEL_AUDIT_DATABASE_PATH.equals(databaseName)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }

    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }

    return checkGlobalAuth(userName, PrivilegeType.MANAGE_DATABASE);
  }

  private TSStatus checkShowOrCountDatabasePermission(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    // own SYSTEM/MAINTAIN can see all except for root.__audit, otherwise can only see PATHS that
    // user has READ_SCHEMA auth
    if (!checkHasGlobalAuth(context.getUsername(), PrivilegeType.MANAGE_DATABASE)) {
      return visitAuthorityInformation(statement, context);
    } else {
      return SUCCEED;
    }
  }

  // ==================================== data related ========================================
  @Override
  public TSStatus visitInsertBase(InsertBaseStatement statement, TreeAccessCheckContext context) {

    for (PartialPath path : statement.getDevicePaths()) {
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    return checkTimeSeriesPermission(
        context.getUsername(),
        statement.getPaths().stream().distinct().collect(Collectors.toList()),
        PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitInsert(InsertStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevice())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitLoadFile(LoadTsFileStatement statement, TreeAccessCheckContext context) {
    // no need to check here, it will be checked in process phase
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataStatement statement, TreeAccessCheckContext context) {
    for (PartialPath path : statement.getPaths()) {
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitQuery(QueryStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context.getUsername());
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.getUsername(), PrivilegeType.READ_DATA));
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

  @Override
  public TSStatus visitExplain(ExplainStatement explainStatement, TreeAccessCheckContext context) {
    return explainStatement.getQueryStatement().accept(this, context);
  }

  // ============================= timeseries related =================================
  public static TSStatus checkTimeSeriesPermission(
      String userName, List<? extends PartialPath> checkedPaths, PrivilegeType permission) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(userName, checkedPaths, permission),
        checkedPaths,
        permission);
  }

  @Override
  public TSStatus visitCreateTimeseries(
      CreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getPath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevicePath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    for (PartialPath path : statement.getPaths()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    for (PartialPath path : statement.getDeviceMap().keySet()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevicePath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowTimeSeries(
      ShowTimeSeriesStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context.getUsername());
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
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
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context.getUsername());
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
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
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      countStatement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(countStatement, context.getUsername());
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitCountNodes(
      CountNodesStatement countStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      countStatement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(countStatement, context.getUsername());
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      showChildNodesStatement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(showChildNodesStatement, context.getUsername());
    return visitAuthorityInformation(showChildNodesStatement, context);
  }

  @Override
  public TSStatus visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      showChildPathsStatement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(showChildPathsStatement, context.getUsername());
    return visitAuthorityInformation(showChildPathsStatement, context);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      AlterTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getPath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitDeleteTimeSeries(
      DeleteTimeSeriesStatement statement, TreeAccessCheckContext context) {
    // audit db is read-only
    for (PartialPath path : statement.getPathPatternList()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(
        context.getUsername(), statement.getPaths(), PrivilegeType.WRITE_SCHEMA);
  }

  // ================================== maintain related =============================
  @Override
  public TSStatus visitExtendRegion(
      ExtendRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitGetRegionId(GetRegionIdStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitGetSeriesSlotList(
      GetSeriesSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitGetTimeSlotList(
      GetTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitCountTimeSlotList(
      CountTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitKillQuery(KillQueryStatement statement, TreeAccessCheckContext context) {
    if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN)) {
      statement.setAllowedUsername(context.getUsername());
    }
    return SUCCEED;
  }

  @Override
  public TSStatus visitFlush(FlushStatement flushStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitSetConfiguration(
      SetConfigurationStatement setConfigurationStatement, TreeAccessCheckContext context) {
    try {
      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkUserMissingSystemPermissions(
              context.getUsername(), setConfigurationStatement.getNeededPrivileges()));
    } catch (IOException e) {
      return AuthorityChecker.getTSStatus(false, "Failed to check config item permission");
    }
  }

  @Override
  public TSStatus visitSetSystemStatus(
      SetSystemStatusStatement setSystemStatusStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitStartRepairData(
      StartRepairDataStatement startRepairDataStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitStopRepairData(
      StopRepairDataStatement stopRepairDataStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitClearCache(
      ClearCacheStatement clearCacheStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitMigrateRegion(
      MigrateRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitReconstructRegion(
      ReconstructRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitRemoveAINode(
      RemoveAINodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitRemoveConfigNode(
      RemoveConfigNodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitRemoveDataNode(
      RemoveDataNodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitRemoveRegion(
      RemoveRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitSetSqlDialect(
      SetSqlDialectStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowAINodes(ShowAINodesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowClusterId(
      ShowClusterIdStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowCluster(ShowClusterStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowConfigNodes(
      ShowConfigNodesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
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
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowQueries(ShowQueriesStatement statement, TreeAccessCheckContext context) {
    if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN)) {
      statement.setAllowedUsername(context.getUsername());
    }
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowRegion(ShowRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitSetSpaceQuota(
      SetSpaceQuotaStatement setSpaceQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitSetThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitShowThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitShowSpaceQuota(
      ShowSpaceQuotaStatement showSpaceQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM);
  }

  @Override
  public TSStatus visitShowVariables(
      ShowVariablesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowVersion(ShowVersionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitTestConnection(
      TestConnectionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context.getUsername(), PrivilegeType.MAINTAIN);
  }

  @Override
  public TSStatus visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, TreeAccessCheckContext context) {
    return checkOnlySuperUser(context.getUsername());
  }

  // ======================== TTL related ===========================
  @Override
  public TSStatus visitSetTTL(SetTTLStatement statement, TreeAccessCheckContext context) {
    List<PartialPath> checkedPaths = statement.getPaths();
    boolean[] pathsNotEndWithMultiLevelWildcard = null;
    for (int i = 0; i < checkedPaths.size(); i++) {
      PartialPath checkedPath = checkedPaths.get(i);
      TSStatus status = checkWriteOnReadOnlyPath(checkedPath);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      if (!checkedPath.endWithMultiLevelWildcard()) {
        pathsNotEndWithMultiLevelWildcard =
            pathsNotEndWithMultiLevelWildcard == null
                ? new boolean[checkedPaths.size()]
                : pathsNotEndWithMultiLevelWildcard;
        pathsNotEndWithMultiLevelWildcard[i] = true;
      }
    }
    if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM)) {
      return SUCCEED;
    }

    // Using paths end with '**' to check permission
    List<PartialPath> pathsForCheckingPermissions = checkedPaths;
    if (pathsNotEndWithMultiLevelWildcard != null) {
      pathsForCheckingPermissions = new ArrayList<>(checkedPaths.size());
      for (int i = 0; i < checkedPaths.size(); i++) {
        if (pathsNotEndWithMultiLevelWildcard[i]) {
          pathsForCheckingPermissions.add(
              checkedPaths.get(i).concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
          continue;
        }
        pathsForCheckingPermissions.add(checkedPaths.get(i));
      }
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.getUsername(), pathsForCheckingPermissions, PrivilegeType.WRITE_SCHEMA),
        pathsForCheckingPermissions,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowTTL(ShowTTLStatement showTTLStatement, TreeAccessCheckContext context) {
    if (checkHasGlobalAuth(context.getUsername(), PrivilegeType.SYSTEM)) {
      return SUCCEED;
    }
    for (PartialPath path : showTTLStatement.getPaths()) {
      if (path.endWithMultiLevelWildcard()) {
        continue;
      }
      if (!AuthorityChecker.checkFullPathOrPatternPermission(
          context.getUsername(),
          path.concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD),
          PrivilegeType.READ_SCHEMA)) {
        return AuthorityChecker.getTSStatus(false, path, PrivilegeType.READ_SCHEMA);
      }
    }
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
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context.getUsername());
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
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
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context.getUsername());
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  protected TSStatus checkSystemAuth(String userName) {
    return checkGlobalAuth(userName, PrivilegeType.SYSTEM);
  }

  protected TSStatus checkGlobalAuth(String userName, PrivilegeType requiredPrivilege) {
    if (checkHasGlobalAuth(userName, requiredPrivilege)) {
      return SUCCEED;
    }
    return AuthorityChecker.getTSStatus(false, requiredPrivilege);
  }

  protected boolean checkHasGlobalAuth(String userName, PrivilegeType requiredPrivilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return true;
    }
    return AuthorityChecker.checkSystemPermission(userName, requiredPrivilege);
  }

  protected TSStatus checkWriteOnReadOnlyPath(PartialPath path) {
    if (includeByAuditTreeDB(path)
        && !AuthorityChecker.INTERNAL_AUDIT_USER.equals(path.getFullPath())) {
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return SUCCEED;
  }

  protected void setCanSeeAuditDB(AuthorityInformationStatement statement, String userName) {
    if (!checkHasGlobalAuth(userName, PrivilegeType.AUDIT)) {
      statement.setCanSeeAuditDB(false);
    }
  }

  private TSStatus checkOnlySuperUser(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }
    return AuthorityChecker.getTSStatus(false, "Only the admin user can perform this operation");
  }
}
