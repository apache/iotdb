/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.audit.CNAuditLogger;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterEncodingCompressorPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;

public class PipeConfigTreePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<List<ConfigPhysicalPlan>, IAuditEntity> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigTreePrivilegeParseVisitor.class);
  private final boolean skip;

  PipeConfigTreePrivilegeParseVisitor(final boolean skip) {
    this.skip = skip;
  }

  @Override
  public List<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final IAuditEntity context) {
    return Collections.singletonList(plan);
  }

  @Override
  public List<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(createDatabasePlan.getSchema().getName(), userEntity, true)
        ? Collections.singletonList(createDatabasePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(alterDatabasePlan.getSchema().getName(), userEntity, true)
        ? Collections.singletonList(alterDatabasePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(deleteDatabasePlan.getName(), userEntity, true)
        ? Collections.singletonList(deleteDatabasePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final IAuditEntity userEntity) {
    return canShowSchemaTemplate(createSchemaTemplatePlan.getTemplate().getName(), userEntity)
        ? Collections.singletonList(createSchemaTemplatePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan,
      final IAuditEntity userEntity) {
    return canReadSysSchema(commitSetSchemaTemplatePlan.getPath(), userEntity, false)
        ? Collections.singletonList(commitSetSchemaTemplatePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan,
      final IAuditEntity userEntity) {
    return canReadSysSchema(pipeUnsetSchemaTemplatePlan.getPath(), userEntity, false)
        ? Collections.singletonList(pipeUnsetSchemaTemplatePlan)
        : Collections.emptyList();
  }

  @Override
  public List<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final IAuditEntity userEntity) {
    return canShowSchemaTemplate(
            extendSchemaTemplatePlan.getTemplateExtendInfo().getTemplateName(), userEntity)
        ? Collections.singletonList(extendSchemaTemplatePlan)
        : Collections.emptyList();
  }

  public boolean canShowSchemaTemplate(final String templateName, final IAuditEntity userEntity) {
    try {
      return hasGlobalPrivilege(userEntity, PrivilegeType.SYSTEM, templateName, false)
          || ConfigNode.getInstance()
              .getConfigManager()
              .getClusterSchemaManager()
              .getPathsSetTemplate(templateName, ALL_MATCH_SCOPE)
              .getPathList()
              .stream()
              .anyMatch(path -> hasReadPrivilege(userEntity, path, true, true));
    } catch (final Exception e) {
      LOGGER.warn(
          ManagerMessages.UN_PARSE_ABLE_PATH_NAME_ENCOUNTERED_DURING_TEMPLATE_PRIVILEGE_TRIMMING,
          e);
      return false;
    }
  }

  public boolean canReadSysSchema(
      final String path, final IAuditEntity userEntity, final boolean canSkipMulti) {
    return canSkipMulti && hasReadPrivilege(userEntity, path, false, false)
        || hasReadPrivilege(userEntity, path, true, false)
        || hasGlobalPrivilege(userEntity, PrivilegeType.SYSTEM, path, true);
  }

  @Override
  public List<ConfigPhysicalPlan> visitGrantUser(
      final AuthorTreePlan grantUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(grantUserPlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorTreePlan revokeUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(revokeUserPlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<ConfigPhysicalPlan> visitGrantRole(
      final AuthorTreePlan grantRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(grantRolePlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorTreePlan revokeRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(revokeRolePlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<ConfigPhysicalPlan> visitGrantRoleToUser(
      final AuthorTreePlan grantRoleToUserPlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(grantRoleToUserPlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  @Override
  public List<ConfigPhysicalPlan> visitRevokeRoleFromUser(
      final AuthorTreePlan revokeRoleFromUserPlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(revokeRoleFromUserPlan, userEntity)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  public static Optional<ConfigPhysicalPlan> visitUserRolePlan(
      final AuthorPlan plan, final IAuditEntity userEntity) {
    final Optional<ConfigPhysicalPlan> result = visitUserPlan(plan, userEntity, false);
    return result.isPresent() ? result : visitRolePlan(plan, userEntity);
  }

  public static Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorPlan plan, final IAuditEntity userEntity) {
    return visitUserPlan(plan, userEntity, true);
  }

  public static Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorPlan plan, final IAuditEntity userEntity, final boolean isLastCheck) {
    final String auditObject = plan.getUserName();
    if (userEntity.getUsername().equals(plan.getUserName())) {
      ConfigNode.getInstance()
          .getConfigManager()
          .getAuditLogger()
          .recordObjectAuthenticationAuditLog(
              userEntity.setPrivilegeType(null).setResult(true), () -> auditObject);
      return Optional.of(plan);
    }
    return hasGlobalPrivilege(
            userEntity, PrivilegeType.MANAGE_USER, plan.getUserName(), isLastCheck)
        ? Optional.of(plan)
        : Optional.empty();
  }

  public static Optional<ConfigPhysicalPlan> visitRolePlan(
      final AuthorPlan plan, final IAuditEntity userEntity) {
    final String auditObject = plan.getRoleName();
    final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
    try {
      if (configManager
              .getPermissionManager()
              .checkRoleOfUser(userEntity.getUsername(), plan.getRoleName())
              .getStatus()
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        configManager
            .getAuditLogger()
            .recordObjectAuthenticationAuditLog(
                userEntity.setPrivilegeType(null).setResult(true), () -> auditObject);
        return Optional.of(plan);
      }
    } catch (final Exception ignore) {
      // Check manage role
    }
    return hasGlobalPrivilege(userEntity, PrivilegeType.MANAGE_ROLE, plan.getRoleName(), true)
        ? Optional.of(plan)
        : Optional.empty();
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    final PathPatternTree originalTree =
        PathPatternTree.deserialize(pipeDeleteTimeSeriesPlan.getPatternTreeBytes());
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = originalTree.getAllPathPatterns().toString();
    try {
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userEntity));
      if (!skip && !originalTree.equals(intersectedTree)) {
        logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeDeleteTimeSeriesPlan);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Collections.singletonList(new PipeDeleteTimeSeriesPlan(intersectedTree.serialize()))
          : Collections.emptyList();
    } catch (final IOException e) {
      LOGGER.warn(ManagerMessages.SERIALIZATION_FAILED_FOR_THE_DELETE_TIME_SERIES_PLAN_IN_PIPE, e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Collections.emptyList();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeDeleteTimeSeriesPlan);
      }
    }
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    final PathPatternTree originalTree =
        PathPatternTree.deserialize(pipeDeleteLogicalViewPlan.getPatternTreeBytes());
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = originalTree.getAllPathPatterns().toString();
    try {
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userEntity));
      if (!skip && !originalTree.equals(intersectedTree)) {
        logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeDeleteLogicalViewPlan);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Collections.singletonList(new PipeDeleteLogicalViewPlan(intersectedTree.serialize()))
          : Collections.emptyList();
    } catch (final IOException e) {
      LOGGER.warn(ManagerMessages.SERIALIZATION_FAILED_FOR_THE_DELETE_TIME_SERIES_PLAN_IN_PIPE, e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Collections.emptyList();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeDeleteLogicalViewPlan);
      }
    }
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeAlterEncodingCompressor(
      final PipeAlterEncodingCompressorPlan pipeAlterEncodingCompressor,
      final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    final PathPatternTree originalTree =
        PathPatternTree.deserialize(pipeAlterEncodingCompressor.getPatternTreeBytes());
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = originalTree.getAllPathPatterns().toString();
    try {
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userEntity));
      if (!skip && !originalTree.equals(intersectedTree)) {
        logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeAlterEncodingCompressor);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Collections.singletonList(
              new PipeAlterEncodingCompressorPlan(
                  intersectedTree.serialize(),
                  pipeAlterEncodingCompressor.getEncoding(),
                  pipeAlterEncodingCompressor.getCompressor(),
                  pipeAlterEncodingCompressor.isMayAlterAudit()))
          : Collections.emptyList();
    } catch (final IOException e) {
      LOGGER.warn(ManagerMessages.SERIALIZATION_FAILED_FOR_THE_DELETE_TIME_SERIES_PLAN_IN_PIPE, e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Collections.emptyList();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeAlterEncodingCompressor);
      }
    }
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = pipeDeactivateTemplatePlan.getTemplateSetInfo().toString();
    try {
      final Map<PartialPath, List<Template>> newTemplateSetInfo = new HashMap<>();
      for (final Map.Entry<PartialPath, List<Template>> templateEntry :
          pipeDeactivateTemplatePlan.getTemplateSetInfo().entrySet()) {
        for (final PartialPath intersectedPath :
            getAllIntersectedPatterns(
                templateEntry.getKey(), userEntity, pipeDeactivateTemplatePlan)) {
          // root.db.device2.measurement -> root.db.device.** = root.db
          // Note that we cannot take this circumstance into account
          if (intersectedPath.getNodeLength() == templateEntry.getKey().getNodeLength()) {
            newTemplateSetInfo.put(intersectedPath, templateEntry.getValue());
          }
        }
      }
      final boolean result = !newTemplateSetInfo.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return !newTemplateSetInfo.isEmpty()
          ? Collections.singletonList(new PipeDeactivateTemplatePlan(newTemplateSetInfo))
          : Collections.emptyList();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeDeactivateTemplatePlan);
      }
    }
  }

  @Override
  public List<ConfigPhysicalPlan> visitTTL(
      final SetTTLPlan setTTLPlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = Arrays.toString(setTTLPlan.getPathPattern());
    try {
      // Database TTL also applies to descendants for compatibility with device TTL.
      final PartialPath originalPath = new PartialPath(setTTLPlan.getPathPattern());
      final PartialPath effectivePath =
          setTTLPlan.isDataBase()
              ? originalPath.concatNode(MULTI_LEVEL_PATH_WILDCARD)
              : originalPath;
      final List<PartialPath> paths =
          getAllIntersectedPatterns(effectivePath, userEntity, setTTLPlan).stream()
              .filter(effectivePath::include)
              .collect(Collectors.toList());
      logger.recordObjectAuthenticationAuditLog(
          userEntity.setResult(!paths.isEmpty()), () -> auditObject);
      return paths.stream()
          .map(
              path -> {
                final boolean retainDatabase =
                    setTTLPlan.isDataBase() && path.equals(effectivePath);
                final SetTTLPlan result =
                    new SetTTLPlan(
                        retainDatabase ? originalPath.getNodes() : path.getNodes(),
                        setTTLPlan.getTTL());
                result.setDataBase(retainDatabase);
                return result;
              })
          .collect(Collectors.toList());
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + setTTLPlan);
      }
    }
  }

  @Override
  public List<ConfigPhysicalPlan> visitPipeAlterTimeSeries(
      final PipeAlterTimeSeriesPlan pipeAlterTimeSeriesPlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject =
        Arrays.toString(pipeAlterTimeSeriesPlan.getMeasurementPath().getNodes());
    try {
      final List<PartialPath> paths =
          getAllIntersectedPatterns(
              pipeAlterTimeSeriesPlan.getMeasurementPath(), userEntity, pipeAlterTimeSeriesPlan);
      // The intersectionList is either a singleton list or an empty list, because the pipe
      // pattern and TTL path are each either a prefix path or a full path
      final boolean result =
          !paths.isEmpty()
              && paths.get(0).getNodeLength()
                  == pipeAlterTimeSeriesPlan.getMeasurementPath().getNodeLength();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Collections.singletonList(
              new PipeAlterTimeSeriesPlan(
                  new MeasurementPath(paths.get(0).getNodes()),
                  pipeAlterTimeSeriesPlan.getOperationType(),
                  pipeAlterTimeSeriesPlan.getDataType()))
          : Collections.emptyList();
    } catch (final AuthException e) {
      if (skip) {
        return Collections.emptyList();
      } else {
        throw new AccessDeniedException(
            ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + pipeAlterTimeSeriesPlan);
      }
    }
  }

  private List<PartialPath> getAllIntersectedPatterns(
      final PartialPath partialPath, final IAuditEntity userEntity, final ConfigPhysicalPlan plan)
      throws AuthException {
    final PathPatternTree thisPatternTree = new PathPatternTree();
    thisPatternTree.appendPathPattern(partialPath);
    thisPatternTree.constructTree();
    final PathPatternTree intersectedTree = new PathPatternTree();
    // Intersect each authorization separately so an unmatched prefix cannot leave an ancestor
    // path in the result tree.
    for (final PartialPath authorizedPath : getAuthorizedPTree(userEntity).getAllPathPatterns()) {
      if (authorizedPath.endWithMultiLevelWildcard()) {
        partialPath
            .intersectWithPrefixPattern(authorizedPath)
            .forEach(intersectedTree::appendPathPattern);
      } else if (partialPath.matchFullPath(authorizedPath)) {
        intersectedTree.appendPathPattern(authorizedPath);
      }
    }
    intersectedTree.constructTree();
    if (!skip && !thisPatternTree.equals(intersectedTree)) {
      throw new AccessDeniedException(ManagerMessages.NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN + plan);
    }
    return intersectedTree.getAllPathPatterns();
  }

  private PathPatternTree getAuthorizedPTree(final IAuditEntity userEntity) throws AuthException {
    return ConfigNode.getInstance()
        .getConfigManager()
        .getPermissionManager()
        .fetchRawAuthorizedPTree(userEntity.getUsername(), PrivilegeType.READ_SCHEMA);
  }

  public static TSStatus checkGlobalStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String auditObject,
      final boolean isLastCheck) {
    return checkGlobalStatus(userEntity, privilegeType, auditObject, isLastCheck, false);
  }

  public static TSStatus checkGlobalStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String auditObject,
      final boolean isLastCheck,
      final boolean grantOption) {
    return checkGlobalOrAnyStatus(
        userEntity, privilegeType, auditObject, isLastCheck, grantOption, false);
  }

  public static TSStatus checkGlobalOrAnyStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String auditObject,
      final boolean isLastCheck,
      final boolean grantOption,
      final boolean isAny) {
    final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
    final CNAuditLogger logger = configManager.getAuditLogger();
    final TSStatus result =
        configManager
            .getPermissionManager()
            .checkUserPrivileges(
                userEntity.getUsername(), new PrivilegeUnion(privilegeType, grantOption, isAny))
            .getStatus();
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode() || isLastCheck) {
      logger.recordObjectAuthenticationAuditLog(
          userEntity
              .setPrivilegeType(privilegeType)
              .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          () -> auditObject);
    }
    return result;
  }

  public static boolean hasGlobalPrivilege(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final String auditObject,
      final boolean isLastCheck) {
    return checkGlobalStatus(userEntity, privilegeType, auditObject, isLastCheck).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public static TSStatus checkPathsStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final @Nonnull List<PartialPath> paths,
      final boolean isLastCheck) {
    return checkPathsStatus(userEntity, privilegeType, paths, isLastCheck, null);
  }

  public static TSStatus checkPathsStatus(
      final IAuditEntity userEntity,
      final PrivilegeType privilegeType,
      final @Nonnull List<PartialPath> paths,
      final boolean isLastCheck,
      final @Nullable String grantName) {
    final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
    final CNAuditLogger logger = configManager.getAuditLogger();
    final TSStatus result =
        ConfigNode.getInstance()
            .getConfigManager()
            .getPermissionManager()
            .checkUserPrivileges(
                userEntity.getUsername(),
                new PrivilegeUnion(paths, privilegeType, Objects.nonNull(grantName)))
            .getStatus();
    if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode() || isLastCheck) {
      logger.recordObjectAuthenticationAuditLog(
          userEntity
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          Objects.nonNull(grantName) ? () -> grantName : paths::toString);
    }
    return result;
  }

  private boolean hasReadPrivilege(
      final IAuditEntity userEntity,
      final String path,
      final boolean withWildcard,
      final boolean isLastCheck) {
    PartialPath partialPath;
    try {
      partialPath = new PartialPath(path);
    } catch (final IllegalPathException e) {
      LOGGER.warn(ManagerMessages.UNABLE_TO_PARSE_PATH_WHEN_CHECKING_READ_PRIVILEGE_PATH, path);
      return false;
    }
    if (withWildcard) {
      partialPath = partialPath.concatNode(MULTI_LEVEL_PATH_WILDCARD);
    }
    return checkPathsStatus(
                userEntity,
                PrivilegeType.READ_SCHEMA,
                Collections.singletonList(partialPath),
                isLastCheck)
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
