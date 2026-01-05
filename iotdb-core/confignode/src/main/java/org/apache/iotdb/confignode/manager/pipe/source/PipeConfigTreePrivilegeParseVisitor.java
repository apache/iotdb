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

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;

public class PipeConfigTreePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigTreePrivilegeParseVisitor.class);
  private final boolean skip;

  PipeConfigTreePrivilegeParseVisitor(final boolean skip) {
    this.skip = skip;
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final IAuditEntity context) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(createDatabasePlan.getSchema().getName(), userEntity, true)
        ? Optional.of(createDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(alterDatabasePlan.getSchema().getName(), userEntity, true)
        ? Optional.of(alterDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final IAuditEntity userEntity) {
    return canReadSysSchema(deleteDatabasePlan.getName(), userEntity, true)
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final IAuditEntity userEntity) {
    return canShowSchemaTemplate(createSchemaTemplatePlan.getTemplate().getName(), userEntity)
        ? Optional.of(createSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan,
      final IAuditEntity userEntity) {
    return canReadSysSchema(commitSetSchemaTemplatePlan.getPath(), userEntity, false)
        ? Optional.of(commitSetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan,
      final IAuditEntity userEntity) {
    return canReadSysSchema(pipeUnsetSchemaTemplatePlan.getPath(), userEntity, false)
        ? Optional.of(pipeUnsetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final IAuditEntity userEntity) {
    return canShowSchemaTemplate(
            extendSchemaTemplatePlan.getTemplateExtendInfo().getTemplateName(), userEntity)
        ? Optional.of(extendSchemaTemplatePlan)
        : Optional.empty();
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
          "Un-parse-able path name encountered during template privilege trimming, please check",
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
  public Optional<ConfigPhysicalPlan> visitGrantUser(
      final AuthorTreePlan grantUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(grantUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorTreePlan revokeUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(revokeUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(
      final AuthorTreePlan grantRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(grantRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorTreePlan revokeRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(revokeRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRoleToUser(
      final AuthorTreePlan grantRoleToUserPlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(grantRoleToUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRoleFromUser(
      final AuthorTreePlan revokeRoleFromUserPlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(revokeRoleFromUserPlan, userEntity);
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
  public Optional<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
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
            "Not has privilege to transfer plan: " + pipeDeleteTimeSeriesPlan);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Optional.of(new PipeDeleteTimeSeriesPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Optional.empty();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeleteTimeSeriesPlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteLogicalView(
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
            "Not has privilege to transfer plan: " + pipeDeleteLogicalViewPlan);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Optional.of(new PipeDeleteLogicalViewPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Optional.empty();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeleteLogicalViewPlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeAlterEncodingCompressor(
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
            "Not has privilege to transfer plan: " + pipeAlterEncodingCompressor);
      }
      final boolean result = !intersectedTree.isEmpty();
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Optional.of(
              new PipeAlterEncodingCompressorPlan(
                  intersectedTree.serialize(),
                  pipeAlterEncodingCompressor.getEncoding(),
                  pipeAlterEncodingCompressor.getCompressor(),
                  pipeAlterEncodingCompressor.isMayAlterAudit()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      return Optional.empty();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeAlterEncodingCompressor);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
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
          ? Optional.of(new PipeDeactivateTemplatePlan(newTemplateSetInfo))
          : Optional.empty();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeactivateTemplatePlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitTTL(
      final SetTTLPlan setTTLPlan, final IAuditEntity userEntity) {
    final CNAuditLogger logger = ConfigNode.getInstance().getConfigManager().getAuditLogger();
    userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA);
    final String auditObject = Arrays.toString(setTTLPlan.getPathPattern());
    try {
      final List<PartialPath> paths =
          getAllIntersectedPatterns(
              new PartialPath(setTTLPlan.getPathPattern()), userEntity, setTTLPlan);
      // The intersectionList is either a singleton list or an empty list, because the pipe
      // pattern and TTL path are each either a prefix path or a full path
      final boolean result =
          !paths.isEmpty() && paths.get(0).getNodeLength() == setTTLPlan.getPathPattern().length;
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(result), () -> auditObject);
      return result
          ? Optional.of(new SetTTLPlan(paths.get(0).getNodes(), setTTLPlan.getTTL()))
          : Optional.empty();
    } catch (final AuthException e) {
      logger.recordObjectAuthenticationAuditLog(userEntity.setResult(false), () -> auditObject);
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException("Not has privilege to transfer plan: " + setTTLPlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeAlterTimeSeries(
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
          ? Optional.of(
              new PipeAlterTimeSeriesPlan(
                  new MeasurementPath(paths.get(0).getNodes()),
                  pipeAlterTimeSeriesPlan.getOperationType(),
                  pipeAlterTimeSeriesPlan.getDataType()))
          : Optional.empty();
    } catch (final AuthException e) {
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeAlterTimeSeriesPlan);
      }
    }
  }

  private List<PartialPath> getAllIntersectedPatterns(
      final PartialPath partialPath, final IAuditEntity userEntity, final ConfigPhysicalPlan plan)
      throws AuthException {
    final PathPatternTree thisPatternTree = new PathPatternTree();
    thisPatternTree.appendPathPattern(partialPath);
    thisPatternTree.constructTree();
    final PathPatternTree intersectedTree =
        thisPatternTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userEntity));
    if (!skip && !thisPatternTree.equals(intersectedTree)) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + plan);
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
      LOGGER.warn("Unable to parse path when checking READ privilege, path: {}", path);
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
