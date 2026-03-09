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
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.audit.DNAuditLogger;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterEncodingCompressorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesDataTypeStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowAvailableUrlsStatement;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.CreateExternalServiceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.DropExternalServiceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.ShowExternalServiceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.StartExternalServiceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.StopExternalServiceStatement;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE_PATH;
import static org.apache.iotdb.commons.schema.table.Audit.includeByAuditTreeDB;
import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;
import static org.apache.iotdb.db.auth.AuthorityChecker.getAuthorizedPathTree;
import static org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl.READ_ONLY_DB_ERROR_MSG;

public class TreeAccessCheckVisitor extends StatementVisitor<TSStatus, TreeAccessCheckContext> {

  private static final DNAuditLogger AUDIT_LOGGER = DNAuditLogger.getInstance();

  private static final String OBJECT_AUTHENTICATION_AUDIT_STR =
      "User %s (ID=%d) requests authority on object %s with result %s";

  @Override
  public TSStatus visitNode(StatementNode node, TreeAccessCheckContext context) {
    throw new IllegalStateException("Each operation should have permission check.");
  }

  @Override
  public TSStatus visitAuthorityInformation(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setPrivilegeType(PrivilegeType.READ_SCHEMA);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.getUsername(), PrivilegeType.READ_SCHEMA));
    } catch (AuthException e) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(e.getCode().getStatusCode());
    }
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setResult(true),
        () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public static List<MeasurementPath> getIntersectedPaths4Pipe(
      final List<MeasurementPath> paths, final TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.QUERY).setPrivilegeType(PrivilegeType.READ_DATA);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> paths.stream().distinct().collect(Collectors.toList()).toString());
      return paths;
    }
    try {
      final PathPatternTree originalTree = new PathPatternTree();
      paths.forEach(originalTree::appendPathPattern);
      originalTree.constructTree();
      final PathPatternTree tree =
          AuthorityChecker.getAuthorizedPathTree(context.getUsername(), PrivilegeType.READ_DATA);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> paths.stream().distinct().collect(Collectors.toList()).toString());
      return originalTree.intersectWithFullPathPrefixTree(tree).getAllPathPatterns(true);
    } catch (AuthException e) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> paths.stream().distinct().collect(Collectors.toList()).toString());
      return Collections.emptyList();
    }
  }

  // ====================== template related =================================

  @Override
  public TSStatus visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        () -> createTemplateStatement.getMeasurements().toString());
  }

  @Override
  public TSStatus visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // root.__audit can never be set template
    TSStatus status =
        checkWriteOnReadOnlyPath(
            context.setPrivilegeType(PrivilegeType.WRITE_DATA),
            setSchemaTemplateStatement.getPath());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }
    return checkSystemAuth(context, () -> setSchemaTemplateStatement.getPath().toString());
  }

  @Override
  public TSStatus visitActivateTemplate(
      ActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        statement::getPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        statement::getPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        statement::getPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  private TSStatus checkTemplateShowRelated(
      ShowSchemaTemplateStatement statement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAll(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.SYSTEM)
              .setResult(true),
          () -> statement.getPaths().toString());
      return SUCCEED;
    }
    // own SYSTEM can see all, otherwise can only see PATHS that user has READ_SCHEMA auth
    if (!checkHasGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY),
        PrivilegeType.SYSTEM,
        () -> statement.getPaths().toString())) {
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
        context.setAuditLogOperation(AuditLogOperation.DDL),
        statement::getPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        () -> unsetSchemaTemplateStatement.getPaths().toString());
  }

  @Override
  public TSStatus visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkSystemAuth(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        () -> dropSchemaTemplateStatement.getPaths().toString());
  }

  @Override
  public TSStatus visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, TreeAccessCheckContext context) {
    return checkCanAlterTemplate(context, () -> alterSchemaTemplateStatement.getPaths().toString());
  }

  public TSStatus checkCanAlterTemplate(IAuditEntity entity, Supplier<String> auditObject) {
    if (AuthorityChecker.SUPER_USER.equals(entity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          entity
              .setAuditLogOperation(AuditLogOperation.DDL)
              .setPrivilegeType(PrivilegeType.EXTEND_TEMPLATE)
              .setResult(true),
          auditObject);
      return SUCCEED;
    }
    return checkGlobalAuth(
        entity.setAuditLogOperation(AuditLogOperation.DDL),
        PrivilegeType.EXTEND_TEMPLATE,
        auditObject);
  }

  // ============================= timeseries view related ===============
  @Override
  public TSStatus visitCreateLogicalView(
      CreateLogicalViewStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
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
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setPrivilegeType(PrivilegeType.AUDIT).setResult(false), path::toString);
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(true);
      }
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setPrivilegeTypes(
                  Arrays.asList(PrivilegeType.WRITE_SCHEMA, PrivilegeType.READ_SCHEMA))
              .setResult(true),
          paths::toString);
      return SUCCEED;
    }
    if (!checkHasGlobalAuth(context, PrivilegeType.AUDIT, paths::toString)) {
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
              context, () -> statement.getSourcePaths().fullPathList, PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status =
          checkTimeSeriesPermission(context, queryStatement::getPaths, PrivilegeType.READ_SCHEMA);
    }

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return checkTimeSeriesPermission(context, () -> paths, PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitDeleteLogicalView(
      DeleteLogicalViewStatement statement, TreeAccessCheckContext context) {
    return checkTimeSeriesPermission(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        statement::getPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowLogicalView(
      ShowLogicalViewStatement showLogicalViewStatement, TreeAccessCheckContext context) {
    return visitAuthorityInformation(showLogicalViewStatement, context);
  }

  @Override
  public TSStatus visitAlterLogicalView(
      AlterLogicalViewStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      if (statement.getQueryStatement() != null) {
        statement.getQueryStatement().setCanSeeAuditDB(true);
      }
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setPrivilegeTypes(
                  Arrays.asList(PrivilegeType.READ_SCHEMA, PrivilegeType.WRITE_SCHEMA))
              .setResult(true),
          () -> statement.getSourcePaths().fullPathList.toString());
      return SUCCEED;
    }
    if (!checkHasGlobalAuth(
        context, PrivilegeType.AUDIT, (() -> statement.getSourcePaths().fullPathList.toString()))) {
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
              context, () -> statement.getSourcePaths().fullPathList, PrivilegeType.READ_SCHEMA);
    }
    QueryStatement queryStatement = statement.getQueryStatement();
    if (queryStatement != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status =
          checkTimeSeriesPermission(context, queryStatement::getPaths, PrivilegeType.READ_SCHEMA);
    }

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return checkTimeSeriesPermission(
          context, statement::getTargetPathList, PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus visitRenameLogicalView(
      RenameLogicalViewStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getNewName())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setPrivilegeType(PrivilegeType.WRITE_SCHEMA).setResult(false),
          () -> statement.getOldName().toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(
        context,
        () -> ImmutableList.of(statement.getOldName(), statement.getNewName()),
        PrivilegeType.WRITE_SCHEMA);
  }

  // ===================================== security related ===============================
  @Override
  public TSStatus visitAuthor(AuthorStatement statement, TreeAccessCheckContext context) {
    AuthorType authorType = statement.getAuthorType();
    Supplier<String> auditObject;
    switch (authorType) {
      case CREATE_USER:
      case DROP_USER:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.MANAGE_USER,
            statement::getUserName);
      case UPDATE_USER:
      case RENAME_USER:
        context.setAuditLogOperation(AuditLogOperation.DDL);
        if (statement.getUserName().equals(context.getUsername())) {
          // users can change the username and password of themselves
          AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
              context.setResult(true), context::getUsername);
          return RpcUtils.SUCCESS_STATUS;
        }
        if (AuthorityChecker.SUPER_USER_ID
            == AuthorityChecker.getUserId(statement.getUserName()).orElse(-1L)) {
          // Only the superuser can alter him/herself
          AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
              context.setResult(false), context::getUsername);
          return AuthorityChecker.getTSStatus(
              false,
              "Has no permission to execute "
                  + authorType
                  + ", because only the superuser can alter him/herself.");
        }
        context.setPrivilegeType(PrivilegeType.SECURITY);
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.MANAGE_USER,
            statement::getUserName);
      case LIST_USER:
        context.setAuditLogOperation(AuditLogOperation.QUERY).setResult(true);
        if (checkHasGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.MANAGE_USER,
            statement::getUserName)) {
          // MANAGE_USER privilege can list all users
          return RpcUtils.SUCCESS_STATUS;
        }
        // Can only list him/herself without MANAGE_USER privilege
        statement.setUserName(context.getUsername());
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setPrivilegeType(null).setResult(true), context::getUsername);
        return RpcUtils.SUCCESS_STATUS;
      case LIST_USER_PRIVILEGE:
        context.setAuditLogOperation(AuditLogOperation.QUERY);
        if (context.getUsername().equals(statement.getUserName())) {
          // No need any privilege to list his/her own privileges
          AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
              context.setResult(true), context::getUsername);
          return RpcUtils.SUCCESS_STATUS;
        }
        // Require MANAGE_USER privilege to list other users' privileges
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.MANAGE_USER,
            statement::getUserName);
      case LIST_ROLE_PRIVILEGE:
        context.setAuditLogOperation(AuditLogOperation.QUERY);
        if (!AuthorityChecker.checkRole(context.getUsername(), statement.getRoleName())) {
          return checkGlobalAuth(
              context.setAuditLogOperation(AuditLogOperation.QUERY),
              PrivilegeType.MANAGE_ROLE,
              statement::getRoleName);
        } else {
          // No need any privilege to list his/her own role's privileges
          AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
              context.setResult(true), context::getUsername);
          return SUCCEED;
        }
      case LIST_ROLE:
        context.setAuditLogOperation(AuditLogOperation.QUERY);
        if (checkHasGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.MANAGE_ROLE,
            statement::getRoleName)) {
          return SUCCEED;
        }
        // list roles of other user is not allowed
        if (statement.getUserName() != null
            && !statement.getUserName().equals(context.getUsername())) {
          AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
              context.setPrivilegeType(PrivilegeType.MANAGE_ROLE).setResult(false),
              context::getUsername);
          return AuthorityChecker.getTSStatus(false, PrivilegeType.MANAGE_ROLE);
        }
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setPrivilegeType(null).setResult(true), context::getUsername);
        statement.setUserName(context.getUsername());
        return RpcUtils.SUCCESS_STATUS;

      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        auditObject =
            authorType == AuthorType.CREATE_ROLE || authorType == AuthorType.DROP_ROLE
                ? statement::getRoleName
                : () -> "user: " + statement.getUserName() + ", role: " + statement.getRoleName();
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.MANAGE_ROLE,
            auditObject);

      case REVOKE_USER:
      case GRANT_USER:
      case GRANT_ROLE:
      case REVOKE_ROLE:
      case ACCOUNT_UNLOCK:
        auditObject =
            () ->
                authorType == AuthorType.REVOKE_USER || authorType == AuthorType.GRANT_USER
                    ? statement.getUserName()
                    : statement.getRoleName();
        if (checkHasGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.SECURITY,
            auditObject)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkPermissionsWithGrantOption(
            context,
            authorType,
            Arrays.stream(statement.getPrivilegeList())
                .map(s -> PrivilegeType.valueOf(s.toUpperCase()))
                .collect(Collectors.toList()),
            statement.getNodeNameList());
      default:
        throw new IllegalArgumentException("Unknown authorType: " + authorType);
    }
  }

  // =================================== CQ related ====================================
  @Override
  public TSStatus visitCreateContinuousQuery(
      CreateContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), () -> statement.getPaths().toString());
  }

  @Override
  public TSStatus visitDropContinuousQuery(
      DropContinuousQueryStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), () -> statement.getPaths().toString());
  }

  @Override
  public TSStatus visitShowContinuousQueries(
      ShowContinuousQueriesStatement statement, TreeAccessCheckContext context) {
    return checkCQManagement(
        context.setAuditLogOperation(AuditLogOperation.QUERY),
        () -> statement.getPaths().toString());
  }

  private TSStatus checkCQManagement(IAuditEntity auditEntity, Supplier<String> auditObject) {
    if (AuthorityChecker.SUPER_USER.equals(auditEntity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          auditEntity.setPrivilegeType(PrivilegeType.USE_CQ).setResult(true), auditObject);
      return SUCCEED;
    }
    return checkGlobalAuth(auditEntity, PrivilegeType.USE_CQ, auditObject);
  }

  // =================================== UDF related ====================================
  @Override
  public TSStatus visitCreateFunction(
      CreateFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getUdfName);
  }

  @Override
  public TSStatus visitDropFunction(
      DropFunctionStatement statement, TreeAccessCheckContext context) {
    return checkUDFManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getUdfName);
  }

  @Override
  public TSStatus visitShowFunctions(
      ShowFunctionsStatement statement, TreeAccessCheckContext context) {
    // anyone can show functions
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setResult(true), null);
    return SUCCEED;
  }

  private TSStatus checkUDFManagement(IAuditEntity auditEntity, Supplier<String> auditObject) {
    return checkGlobalAuth(auditEntity, PrivilegeType.USE_UDF, auditObject);
  }

  // =================================== model related ====================================
  @Override
  public TSStatus visitCreateModel(CreateModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getModelId);
  }

  @Override
  public TSStatus visitDropModel(DropModelStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getModelId);
  }

  @Override
  public TSStatus visitCreateTraining(
      CreateTrainingStatement createTrainingStatement, TreeAccessCheckContext context) {
    return checkModelManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        createTrainingStatement::getExistingModelId);
  }

  @Override
  public TSStatus visitUnloadModel(
      UnloadModelStatement unloadModelStatement, TreeAccessCheckContext context) {
    return checkModelManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), unloadModelStatement::getModelId);
  }

  @Override
  public TSStatus visitLoadModel(
      LoadModelStatement loadModelStatement, TreeAccessCheckContext context) {
    return checkModelManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), loadModelStatement::getModelId);
  }

  @Override
  public TSStatus visitShowAIDevices(
      ShowAIDevicesStatement showAIDevicesStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.setAuditLogOperation(AuditLogOperation.DDL), () -> "");
  }

  @Override
  public TSStatus visitShowLoadedModels(
      ShowLoadedModelsStatement showLoadedModelsStatement, TreeAccessCheckContext context) {
    return checkModelManagement(context.setAuditLogOperation(AuditLogOperation.DDL), () -> "");
  }

  @Override
  public TSStatus visitShowModels(ShowModelsStatement statement, TreeAccessCheckContext context) {
    return checkModelManagement(context.setAuditLogOperation(AuditLogOperation.DDL), () -> "");
  }

  private TSStatus checkModelManagement(IAuditEntity auditEntity, Supplier<String> auditObject) {
    return checkGlobalAuth(auditEntity, PrivilegeType.USE_MODEL, auditObject);
  }

  // ================================ pipe plugin related ==================================
  @Override
  public TSStatus visitCreatePipePlugin(
      CreatePipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), () -> statement.getPaths().toString());
  }

  @Override
  public TSStatus visitDropPipePlugin(
      DropPipePluginStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), () -> statement.getPaths().toString());
  }

  @Override
  public TSStatus visitShowPipePlugins(
      ShowPipePluginsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), () -> statement.getPaths().toString());
  }

  // =============================== pipe related ========================================

  @Override
  public TSStatus visitCreatePipe(CreatePipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getPipeName);
  }

  @Override
  public TSStatus visitShowPipes(ShowPipesStatement statement, TreeAccessCheckContext context) {
    // This query cannot be rejected, but will be filtered at configNode
    // Does not need auth check here
    return StatusUtils.OK;
  }

  @Override
  public TSStatus visitDropPipe(DropPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getPipeName);
  }

  @Override
  public TSStatus visitAlterPipe(AlterPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getPipeName);
  }

  @Override
  public TSStatus visitStartPipe(StartPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getPipeName);
  }

  @Override
  public TSStatus visitStopPipe(StopPipeStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getPipeName);
  }

  private TSStatus checkPipeManagement(IAuditEntity auditEntity, Supplier<String> auditObject) {
    return checkGlobalAuth(auditEntity, PrivilegeType.USE_PIPE, auditObject);
  }

  // =============================== subscription related ========================================

  @Override
  public TSStatus visitCreateTopic(CreateTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getTopicName);
  }

  @Override
  public TSStatus visitShowTopics(ShowTopicsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getTopicName);
  }

  @Override
  public TSStatus visitDropTopic(DropTopicStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getTopicName);
  }

  @Override
  public TSStatus visitShowSubscriptions(
      ShowSubscriptionsStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getTopicName);
  }

  @Override
  public TSStatus visitDropSubscription(
      DropSubscriptionStatement statement, TreeAccessCheckContext context) {
    return checkPipeManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getSubscriptionId);
  }

  // ======================= trigger related ================================
  @Override
  public TSStatus visitCreateTrigger(
      CreateTriggerStatement statement, TreeAccessCheckContext context) {
    if (TREE_MODEL_AUDIT_DATABASE_PATH.include(statement.getPathPattern())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setAuditLogOperation(AuditLogOperation.DDL)
              .setPrivilegeType(PrivilegeType.USE_TRIGGER)
              .setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTriggerManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
  }

  @Override
  public TSStatus visitDropTrigger(DropTriggerStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement::getTriggerName);
  }

  @Override
  public TSStatus visitShowTriggers(
      ShowTriggersStatement statement, TreeAccessCheckContext context) {
    return checkTriggerManagement(context.setAuditLogOperation(AuditLogOperation.QUERY), () -> "");
  }

  private TSStatus checkTriggerManagement(IAuditEntity auditEntity, Supplier<String> auditObject) {
    if (AuthorityChecker.SUPER_USER.equals(auditEntity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          auditEntity.setPrivilegeType(PrivilegeType.USE_TRIGGER).setResult(true), auditObject);
      return SUCCEED;
    }
    return checkGlobalAuth(auditEntity, PrivilegeType.USE_TRIGGER, auditObject);
  }

  // ======================= externalService related ================================
  @Override
  public TSStatus visitCreateExternalService(
      CreateExternalServiceStatement createExternalServiceStatement,
      TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context, PrivilegeType.SYSTEM, () -> createExternalServiceStatement.toString());
  }

  @Override
  public TSStatus visitStartExternalService(
      StartExternalServiceStatement startExternalServiceStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context, PrivilegeType.SYSTEM, () -> startExternalServiceStatement.toString());
  }

  @Override
  public TSStatus visitStopExternalService(
      StopExternalServiceStatement stopExternalServiceStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context, PrivilegeType.SYSTEM, () -> stopExternalServiceStatement.toString());
  }

  @Override
  public TSStatus visitDropExternalService(
      DropExternalServiceStatement dropExternalServiceStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context, PrivilegeType.SYSTEM, () -> dropExternalServiceStatement.toString());
  }

  @Override
  public TSStatus visitShowExternalService(
      ShowExternalServiceStatement showExternalServiceStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context, PrivilegeType.SYSTEM, () -> showExternalServiceStatement.toString());
  }

  // ============================== database related ===========================
  @Override
  public TSStatus visitSetDatabase(
      DatabaseSchemaStatement statement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(
        context.setAuditLogOperation(AuditLogOperation.DDL), statement.getDatabasePath());
  }

  @Override
  public TSStatus visitAlterDatabase(
      DatabaseSchemaStatement databaseSchemaStatement, TreeAccessCheckContext context) {
    return checkCreateOrAlterDatabasePermission(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        databaseSchemaStatement.getDatabasePath());
  }

  @Override
  public TSStatus visitShowDatabase(
      ShowDatabaseStatement showDatabaseStatement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setDatabase(
            showDatabaseStatement.getPaths().stream()
                .distinct()
                .collect(Collectors.toList())
                .toString());
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setPrivilegeType(PrivilegeType.SYSTEM).setResult(true), context::getDatabase);
      return SUCCEED;
    }
    setCanSeeAuditDB(showDatabaseStatement, context);
    return checkShowOrCountDatabasePermission(showDatabaseStatement, context);
  }

  @Override
  public TSStatus visitCountDatabase(
      CountDatabaseStatement countDatabaseStatement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setDatabase(
            countDatabaseStatement.getPaths().stream()
                .distinct()
                .collect(Collectors.toList())
                .toString());
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setPrivilegeType(PrivilegeType.SYSTEM).setResult(true), context::getDatabase);
      return SUCCEED;
    }
    setCanSeeAuditDB(countDatabaseStatement, context);
    return checkShowOrCountDatabasePermission(countDatabaseStatement, context);
  }

  @Override
  public TSStatus visitDeleteDatabase(
      DeleteDatabaseStatement statement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.DDL)
        .setDatabase(statement.getPrefixPath().toString());
    for (String prefixPath : statement.getPrefixPath()) {
      // root.__audit can never be deleted
      if (TREE_MODEL_AUDIT_DATABASE.equals(prefixPath)) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setPrivilegeType(PrivilegeType.MANAGE_DATABASE).setResult(false),
            () -> prefixPath);
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setPrivilegeType(PrivilegeType.MANAGE_DATABASE).setResult(true),
          () -> statement.getPrefixPath().toString());
      return SUCCEED;
    }
    return checkGlobalAuth(
        context, PrivilegeType.MANAGE_DATABASE, () -> statement.getPrefixPath().toString());
  }

  protected TSStatus checkCreateOrAlterDatabasePermission(
      IAuditEntity auditEntity, PartialPath databaseName) {
    auditEntity
        .setDatabase(databaseName.getFullPath())
        .setPrivilegeType(PrivilegeType.MANAGE_DATABASE)
        .setAuditLogOperation(AuditLogOperation.DDL);
    if (TREE_MODEL_AUDIT_DATABASE_PATH.equals(databaseName)) {
      if (AuthorityChecker.INTERNAL_AUDIT_USER.equals(auditEntity.getUsername())) {
        // root.__audit can never be created or alter by other users
        return SUCCEED;
      }
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          auditEntity.setResult(false), databaseName::getFullPath);
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }

    if (AuthorityChecker.SUPER_USER.equals(auditEntity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          auditEntity.setResult(true), databaseName::getFullPath);
      return SUCCEED;
    }

    return checkGlobalAuth(auditEntity, PrivilegeType.MANAGE_DATABASE, databaseName::getFullPath);
  }

  private TSStatus checkShowOrCountDatabasePermission(
      AuthorityInformationStatement statement, TreeAccessCheckContext context) {
    // own SYSTEM/MAINTAIN can see all except for root.__audit, otherwise can only see PATHS that
    // user has READ_SCHEMA auth
    if (!checkHasGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY),
        PrivilegeType.MANAGE_DATABASE,
        () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString())) {
      return visitAuthorityInformation(statement, context);
    } else {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.MANAGE_DATABASE)
              .setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
  }

  // ==================================== data related ========================================
  @Override
  public TSStatus visitInsertBase(InsertBaseStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DML).setPrivilegeType(PrivilegeType.WRITE_DATA);
    for (PartialPath path : statement.getDevicePaths()) {
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(context.setResult(false), path::toString);
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    return checkTimeSeriesPermission(
        context,
        () -> statement.getPaths().stream().distinct().collect(Collectors.toList()),
        PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitInsert(InsertStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DML).setPrivilegeType(PrivilegeType.WRITE_DATA);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevice())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false), () -> statement.getDevice().toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitLoadFile(LoadTsFileStatement statement, TreeAccessCheckContext context) {
    // no need to check here, it will be checked in process phase
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus visitDeleteData(DeleteDataStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DML).setPrivilegeType(PrivilegeType.WRITE_DATA);
    for (PartialPath path : statement.getPaths()) {
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(context.setResult(false), path::toString);
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_DATA);
  }

  @Override
  public TSStatus visitQuery(QueryStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.QUERY).setPrivilegeType(PrivilegeType.READ_DATA);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context);
    context.setPrivilegeType(PrivilegeType.READ_DATA);
    try {
      statement.setAuthorityScope(
          AuthorityChecker.getAuthorizedPathTree(context.getUsername(), PrivilegeType.READ_DATA));
    } catch (AuthException e) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(e.getCode().getStatusCode());
    }
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setResult(true),
        () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
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
      IAuditEntity context,
      Supplier<List<? extends PartialPath>> checkedPathsSupplier,
      PrivilegeType permission) {
    context.setPrivilegeType(permission);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true), () -> checkedPathsSupplier.get().toString());
      return SUCCEED;
    }
    List<? extends PartialPath> checkedPaths = checkedPathsSupplier.get();
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternListPermission(
                context.getUsername(), checkedPaths, permission),
            checkedPaths,
            permission);
    if (!AuthorityChecker.INTERNAL_AUDIT_USER.equals(context.getUsername())) {
      // Internal auditor no needs audit log
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          checkedPaths::toString);
    }
    return result;
  }

  public static List<Integer> checkTimeSeriesPermission4Pipe(
      IAuditEntity context, List<? extends PartialPath> checkedPaths, PrivilegeType permission) {
    context.setPrivilegeType(permission);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true), checkedPaths::toString);
      return Collections.emptyList();
    }
    final List<Integer> results =
        AuthorityChecker.checkFullPathOrPatternListPermission(
            context.getUsername(), checkedPaths, permission);
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setResult(true), checkedPaths::toString);
    return results;
  }

  @Override
  public TSStatus visitCreateTimeseries(
      CreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
        .setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getPath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
        .setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevicePath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
        .setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    for (PartialPath path : statement.getPaths()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }

    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
        .setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    for (PartialPath path : statement.getDeviceMap().keySet()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(context.setResult(false), path::toString);
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getDevicePath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitShowTimeSeries(
      ShowTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setPrivilegeTypes(Arrays.asList(PrivilegeType.READ_DATA, PrivilegeType.READ_SCHEMA));
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context);
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
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountTimeSeries(
      CountTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setPrivilegeType(PrivilegeType.READ_SCHEMA);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context);
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setResult(false),
            () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
        return new TSStatus(e.getCode().getStatusCode());
      }
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
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
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () ->
              countStatement.getPaths().stream()
                  .distinct()
                  .collect(Collectors.toList())
                  .toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(countStatement, context);
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitCountNodes(
      CountNodesStatement countStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      countStatement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () ->
              countStatement.getPaths().stream()
                  .distinct()
                  .collect(Collectors.toList())
                  .toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(countStatement, context);
    return visitAuthorityInformation(countStatement, context);
  }

  @Override
  public TSStatus visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      showChildNodesStatement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () ->
              showChildNodesStatement.getPaths().stream()
                  .distinct()
                  .collect(Collectors.toList())
                  .toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(showChildNodesStatement, context);
    return visitAuthorityInformation(showChildNodesStatement, context);
  }

  @Override
  public TSStatus visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, TreeAccessCheckContext context) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      showChildPathsStatement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () ->
              showChildPathsStatement.getPaths().stream()
                  .distinct()
                  .collect(Collectors.toList())
                  .toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(showChildPathsStatement, context);
    return visitAuthorityInformation(showChildPathsStatement, context);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      AlterTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getPath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitAlterTimeSeries(
      AlterTimeSeriesDataTypeStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    if (includeByAuditTreeDB(statement.getPath())
        && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitAlterEncodingCompressor(
      final AlterEncodingCompressorStatement alterEncodingCompressorStatement,
      final TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    final boolean audit =
        checkHasGlobalAuth(
            context, PrivilegeType.AUDIT, alterEncodingCompressorStatement.getPaths()::toString);
    if (audit) {
      alterEncodingCompressorStatement.setWithAudit(true);
    }
    if (alterEncodingCompressorStatement.ifPermitted()) {
      try {
        final PathPatternTree authTree =
            getAuthorizedPathTree(context.getUsername(), PrivilegeType.WRITE_SCHEMA);
        if (audit) {
          authTree.appendPathPattern(Audit.TREE_MODEL_AUDIT_DATABASE_PATH_PATTERN, true);
          authTree.constructTree();
        }
        alterEncodingCompressorStatement.setPatternTree(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                alterEncodingCompressorStatement.getPatternTree(), authTree));
        return StatusUtils.OK;
      } catch (final AuthException e) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setResult(false),
            () ->
                alterEncodingCompressorStatement.getPaths().stream()
                    .distinct()
                    .collect(Collectors.toList())
                    .toString());
        return new TSStatus(e.getCode().getStatusCode());
      }
    }
    // Check audit privilege
    if (!audit) {
      for (final PartialPath path : alterEncodingCompressorStatement.getPaths()) {
        if (includeByAuditTreeDB(path)) {
          return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
              .setMessage(
                  String.format(
                      "'AUDIT' permission is needed to alter the encoding and compressor of database %s",
                      TREE_MODEL_AUDIT_DATABASE));
        }
      }
    }
    return checkTimeSeriesPermission(
        context, alterEncodingCompressorStatement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  @Override
  public TSStatus visitDeleteTimeSeries(
      DeleteTimeSeriesStatement statement, TreeAccessCheckContext context) {
    context.setAuditLogOperation(AuditLogOperation.DDL);
    // audit db is read-only
    for (PartialPath path : statement.getPathPatternList()) {
      if (includeByAuditTreeDB(path)
          && !context.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setResult(false),
            () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
    }
    return checkTimeSeriesPermission(context, statement::getPaths, PrivilegeType.WRITE_SCHEMA);
  }

  // ================================== maintain related =============================
  @Override
  public TSStatus visitExtendRegion(
      ExtendRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.DDL),
        PrivilegeType.MAINTAIN,
        () -> statement.getRegionIds().toString());
  }

  @Override
  public TSStatus visitGetRegionId(GetRegionIdStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setDatabase(statement.getDatabase()),
        PrivilegeType.MAINTAIN,
        statement::getDatabase);
  }

  @Override
  public TSStatus visitGetSeriesSlotList(
      GetSeriesSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setDatabase(statement.getDatabase()),
        PrivilegeType.MAINTAIN,
        statement::getDatabase);
  }

  @Override
  public TSStatus visitGetTimeSlotList(
      GetTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setDatabase(statement.getDatabase()),
        PrivilegeType.MAINTAIN,
        statement::getDatabase);
  }

  @Override
  public TSStatus visitCountTimeSlotList(
      CountTimeSlotListStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setDatabase(statement.getDatabase()),
        PrivilegeType.MAINTAIN,
        statement::getDatabase);
  }

  @Override
  public TSStatus visitKillQuery(KillQueryStatement statement, TreeAccessCheckContext context) {
    if (checkHasGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.CONTROL),
        PrivilegeType.MAINTAIN,
        () -> "")) {
      statement.setAllowedUsername(context.getUsername());
    }
    return SUCCEED;
  }

  @Override
  public TSStatus visitFlush(FlushStatement flushStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.CONTROL),
        PrivilegeType.SYSTEM,
        () ->
            flushStatement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
  }

  @Override
  public TSStatus visitSetConfiguration(
      SetConfigurationStatement setConfigurationStatement, TreeAccessCheckContext context) {
    List<PrivilegeType> relatedPrivileges;
    try {
      relatedPrivileges = new ArrayList<>(setConfigurationStatement.getNeededPrivileges());
      TSStatus result =
          AuthorityChecker.getTSStatus(
              AuthorityChecker.checkUserMissingSystemPermissions(
                  context.getUsername(), relatedPrivileges));
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context
              .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())
              .setAuditLogOperation(AuditLogOperation.CONTROL)
              .setPrivilegeTypes(relatedPrivileges),
          () -> "");
      return result;
    } catch (IOException e) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(false).setAuditLogOperation(AuditLogOperation.CONTROL), () -> "");
      return AuthorityChecker.getTSStatus(false, "Failed to check config item permission");
    }
  }

  @Override
  public TSStatus visitSetSystemStatus(
      SetSystemStatusStatement setSystemStatusStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.CONTROL), PrivilegeType.SYSTEM, () -> "");
  }

  @Override
  public TSStatus visitStartRepairData(
      StartRepairDataStatement startRepairDataStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(
        context.setAuditLogOperation(AuditLogOperation.CONTROL), PrivilegeType.SYSTEM, () -> "");
  }

  @Override
  public TSStatus visitStopRepairData(
      StopRepairDataStatement stopRepairDataStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitClearCache(
      ClearCacheStatement clearCacheStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitMigrateRegion(
      MigrateRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitReconstructRegion(
      ReconstructRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitRemoveAINode(
      RemoveAINodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitRemoveConfigNode(
      RemoveConfigNodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitRemoveDataNode(
      RemoveDataNodeStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitRemoveRegion(
      RemoveRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitSetSqlDialect(
      SetSqlDialectStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowAINodes(ShowAINodesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowClusterId(
      ShowClusterIdStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowCluster(ShowClusterStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowAvailableUrls(
      ShowAvailableUrlsStatement showAvailableUrlsStatement, TreeAccessCheckContext context) {
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setAuditLogOperation(AuditLogOperation.QUERY).setResult(true), () -> "");
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowConfigNodes(
      ShowConfigNodesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
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
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowQueries(ShowQueriesStatement statement, TreeAccessCheckContext context) {
    if (checkHasGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "")) {
      statement.setAllowedUsername(context.getUsername());
    }
    return SUCCEED;
  }

  @Override
  public TSStatus visitShowRegion(ShowRegionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitSetSpaceQuota(
      SetSpaceQuotaStatement setSpaceQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitSetThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowSpaceQuota(
      ShowSpaceQuotaStatement showSpaceQuotaStatement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowVariables(
      ShowVariablesStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowVersion(ShowVersionStatement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitTestConnection(
      TestConnectionStatement statement, TreeAccessCheckContext context) {
    return checkGlobalAuth(context, PrivilegeType.MAINTAIN, () -> "");
  }

  @Override
  public TSStatus visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, TreeAccessCheckContext context) {
    return checkOnlySuperUser(context, null, () -> "");
  }

  // ======================== TTL related ===========================
  @Override
  public TSStatus visitSetTTL(SetTTLStatement statement, TreeAccessCheckContext context) {
    context.setPrivilegeType(PrivilegeType.SYSTEM).setAuditLogOperation(AuditLogOperation.DDL);
    List<PartialPath> checkedPaths = statement.getPaths();
    boolean[] pathsNotEndWithMultiLevelWildcard = null;
    for (int i = 0; i < checkedPaths.size(); i++) {
      PartialPath checkedPath = checkedPaths.get(i);
      TSStatus status = checkWriteOnReadOnlyPath(context, checkedPath);
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
    if (checkHasGlobalAuth(context, PrivilegeType.SYSTEM, checkedPaths::toString)) {
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
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternListPermission(
                context.getUsername(), pathsForCheckingPermissions, PrivilegeType.WRITE_SCHEMA),
            pathsForCheckingPermissions,
            PrivilegeType.WRITE_SCHEMA);
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context
            .setPrivilegeType(PrivilegeType.WRITE_SCHEMA)
            .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        pathsForCheckingPermissions::toString);
    return result;
  }

  @Override
  public TSStatus visitShowTTL(ShowTTLStatement showTTLStatement, TreeAccessCheckContext context) {
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setPrivilegeType(PrivilegeType.READ_SCHEMA);
    if (checkHasGlobalAuth(
        context,
        PrivilegeType.SYSTEM,
        () ->
            showTTLStatement.getPaths().stream()
                .distinct()
                .collect(Collectors.toList())
                .toString())) {
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
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setResult(false), path::getFullPath);
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
    context
        .setAuditLogOperation(AuditLogOperation.QUERY)
        .setPrivilegeTypes(Arrays.asList(PrivilegeType.READ_DATA, PrivilegeType.READ_SCHEMA));
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      statement.setCanSeeAuditDB(true);
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context);
    if (statement.hasTimeCondition()) {
      try {
        statement.setAuthorityScope(
            PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_SCHEMA),
                AuthorityChecker.getAuthorizedPathTree(
                    context.getUsername(), PrivilegeType.READ_DATA)));
      } catch (AuthException e) {
        AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
            context.setResult(false),
            () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
        return new TSStatus(e.getCode().getStatusCode());
      }
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      return visitAuthorityInformation(statement, context);
    }
  }

  @Override
  public TSStatus visitCountDevices(
      CountDevicesStatement statement, TreeAccessCheckContext context) {
    context
        .setPrivilegeTypes(Arrays.asList(PrivilegeType.READ_DATA, PrivilegeType.READ_SCHEMA))
        .setAuditLogOperation(AuditLogOperation.QUERY);
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setResult(true),
          () -> statement.getPaths().stream().distinct().collect(Collectors.toList()).toString());
      return SUCCEED;
    }
    setCanSeeAuditDB(statement, context);
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

  protected TSStatus checkSystemAuth(IAuditEntity context, Supplier<String> auditObject) {
    return checkGlobalAuth(context, PrivilegeType.SYSTEM, auditObject);
  }

  protected TSStatus checkGlobalAuth(
      IAuditEntity context, PrivilegeType requiredPrivilege, Supplier<String> auditObject) {
    if (checkHasGlobalAuth(context, requiredPrivilege, auditObject)) {
      return SUCCEED;
    }
    TSStatus result = AuthorityChecker.getTSStatus(false, requiredPrivilege);
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        auditObject);
    return result;
  }

  protected boolean checkHasGlobalAuth(
      IAuditEntity context, PrivilegeType requiredPrivilege, Supplier<String> auditObject) {
    return checkHasGlobalAuth(context, requiredPrivilege, auditObject, false);
  }

  protected boolean checkHasGlobalAuth(
      IAuditEntity context,
      PrivilegeType requiredPrivilege,
      Supplier<String> auditObject,
      boolean checkGrantOption) {
    if (AuthorityChecker.SUPER_USER.equals(context.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
          context.setPrivilegeType(requiredPrivilege).setResult(true), auditObject);
      return true;
    }
    boolean result =
        checkGrantOption
            ? AuthorityChecker.checkSystemPermissionGrantOption(
                context.getUsername(), requiredPrivilege)
            : AuthorityChecker.checkSystemPermission(context.getUsername(), requiredPrivilege);
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        context.setPrivilegeType(requiredPrivilege).setResult(result), auditObject);
    return result;
  }

  protected TSStatus checkPermissionsWithGrantOption(
      IAuditEntity auditEntity,
      AuthorType authorType,
      List<PrivilegeType> privilegeList,
      List<PartialPath> paths) {
    Supplier<String> supplier =
        () -> {
          StringJoiner joiner = new StringJoiner(" ");
          if (paths != null) {
            paths.forEach(path -> joiner.add(path.getFullPath()));
          }
          return joiner.toString();
        };
    auditEntity.setPrivilegeTypes(privilegeList);
    if (AuthorityChecker.SUPER_USER.equals(auditEntity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(auditEntity.setResult(true), supplier);
      return SUCCEED;
    }
    TSStatus status = SUCCEED;
    for (PrivilegeType privilegeType : privilegeList) {
      if (privilegeType.isSystemPrivilege()) {
        if (!AuthorityChecker.checkSystemPermissionGrantOption(
            auditEntity.getUsername(), privilegeType)) {
          status =
              AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted");
          break;
        }
      } else if (privilegeType.isPathPrivilege()) {
        if (!AuthorityChecker.checkPathPermissionGrantOption(
            auditEntity.getUsername(), privilegeType, paths)) {
          status =
              AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted");
          break;
        }
      } else {
        status =
            AuthorityChecker.getTSStatus(
                false, "Not support Relation statement in tree sql_dialect");
        break;
      }
    }
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(
        auditEntity.setResult(status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        supplier);
    return status;
  }

  protected TSStatus checkWriteOnReadOnlyPath(IAuditEntity auditEntity, PartialPath path) {
    if (includeByAuditTreeDB(path)
        && !AuthorityChecker.INTERNAL_AUDIT_USER.equals(path.getFullPath())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(auditEntity, path::getFullPath);
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
    }
    return SUCCEED;
  }

  protected void setCanSeeAuditDB(
      AuthorityInformationStatement statement, IAuditEntity auditEntity) {
    if (!checkHasGlobalAuth(auditEntity, PrivilegeType.AUDIT, () -> TREE_MODEL_AUDIT_DATABASE)) {
      statement.setCanSeeAuditDB(false);
    }
  }

  private TSStatus checkOnlySuperUser(
      IAuditEntity auditEntity, PrivilegeType privilegeType, Supplier<String> auditObject) {
    auditEntity.setPrivilegeType(privilegeType);
    if (AuthorityChecker.SUPER_USER.equals(auditEntity.getUsername())) {
      AUDIT_LOGGER.recordObjectAuthenticationAuditLog(auditEntity.setResult(true), auditObject);
      return SUCCEED;
    }
    AUDIT_LOGGER.recordObjectAuthenticationAuditLog(auditEntity.setResult(false), auditObject);
    return AuthorityChecker.getTSStatus(false, "Only the admin user can perform this operation");
  }
}
