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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;

public class PipeConfigTreePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, String> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigTreePrivilegeParseVisitor.class);
  private final boolean skip;

  PipeConfigTreePrivilegeParseVisitor(final boolean skip) {
    this.skip = skip;
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final String context) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final String userName) {
    return canReadSysSchema(createDatabasePlan.getSchema().getName(), userName, true)
        ? Optional.of(createDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final String userName) {
    return canReadSysSchema(alterDatabasePlan.getSchema().getName(), userName, true)
        ? Optional.of(alterDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final String userName) {
    return canReadSysSchema(deleteDatabasePlan.getName(), userName, true)
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final String userName) {
    return canShowSchemaTemplate(createSchemaTemplatePlan.getTemplate().getName(), userName)
        ? Optional.of(createSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan, final String userName) {
    return canReadSysSchema(commitSetSchemaTemplatePlan.getPath(), userName, false)
        ? Optional.of(commitSetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, final String userName) {
    return canReadSysSchema(pipeUnsetSchemaTemplatePlan.getPath(), userName, false)
        ? Optional.of(pipeUnsetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final String userName) {
    return canShowSchemaTemplate(
            extendSchemaTemplatePlan.getTemplateExtendInfo().getTemplateName(), userName)
        ? Optional.of(extendSchemaTemplatePlan)
        : Optional.empty();
  }

  public boolean canShowSchemaTemplate(final String templateName, final String userName) {
    try {
      return ConfigNode.getInstance()
                  .getConfigManager()
                  .getPermissionManager()
                  .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.SYSTEM))
                  .getStatus()
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || ConfigNode.getInstance()
              .getConfigManager()
              .getClusterSchemaManager()
              .getPathsSetTemplate(templateName, ALL_MATCH_SCOPE)
              .getPathList()
              .stream()
              .anyMatch(
                  path -> {
                    try {
                      return ConfigNode.getInstance()
                              .getConfigManager()
                              .getPermissionManager()
                              .checkUserPrivileges(
                                  userName,
                                  new PrivilegeUnion(
                                      Collections.singletonList(
                                          new PartialPath(path)
                                              .concatNode(MULTI_LEVEL_PATH_WILDCARD)),
                                      PrivilegeType.READ_SCHEMA))
                              .getStatus()
                              .getCode()
                          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
                    } catch (final IllegalPathException e) {
                      throw new RuntimeException(e);
                    }
                  });
    } catch (final Exception e) {
      LOGGER.warn(
          "Un-parse-able path name encountered during template privilege trimming, please check",
          e);
      return false;
    }
  }

  public boolean canReadSysSchema(
      final String path, final String userName, final boolean canSkipMulti) {
    try {
      return canSkipMulti
              && ConfigNode.getInstance()
                      .getConfigManager()
                      .getPermissionManager()
                      .checkUserPrivileges(
                          userName,
                          new PrivilegeUnion(
                              Collections.singletonList(new PartialPath(path)),
                              PrivilegeType.READ_SCHEMA))
                      .getStatus()
                      .getCode()
                  == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || ConfigNode.getInstance()
                  .getConfigManager()
                  .getPermissionManager()
                  .checkUserPrivileges(
                      userName,
                      new PrivilegeUnion(
                          Collections.singletonList(
                              new PartialPath(path).concatNode(MULTI_LEVEL_PATH_WILDCARD)),
                          PrivilegeType.READ_SCHEMA))
                  .getStatus()
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || ConfigNode.getInstance()
                  .getConfigManager()
                  .getPermissionManager()
                  .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.SYSTEM))
                  .getStatus()
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (final IllegalPathException e) {
      LOGGER.warn("Un-parse-able path name encountered during privilege trimming, please check", e);
      return false;
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantUser(
      final AuthorTreePlan grantUserPlan, final String userName) {
    return visitUserPlan(grantUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorTreePlan revokeUserPlan, final String userName) {
    return visitUserPlan(revokeUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(
      final AuthorTreePlan grantRolePlan, final String userName) {
    return visitRolePlan(grantRolePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorTreePlan revokeRolePlan, final String userName) {
    return visitRolePlan(revokeRolePlan, userName);
  }

  private Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorTreePlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_USER))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  private Optional<ConfigPhysicalPlan> visitRolePlan(
      final AuthorTreePlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final String userName) {
    try {
      final PathPatternTree originalTree =
          PathPatternTree.deserialize(pipeDeleteTimeSeriesPlan.getPatternTreeBytes());
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userName));
      if (!skip && !originalTree.equals(intersectedTree)) {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeleteTimeSeriesPlan);
      }
      return !intersectedTree.isEmpty()
          ? Optional.of(new PipeDeleteTimeSeriesPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      return Optional.empty();
    } catch (final AuthException e) {
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
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final String userName) {
    try {
      final PathPatternTree originalTree =
          PathPatternTree.deserialize(pipeDeleteLogicalViewPlan.getPatternTreeBytes());
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userName));
      if (!skip && !originalTree.equals(intersectedTree)) {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeleteLogicalViewPlan);
      }
      return !intersectedTree.isEmpty()
          ? Optional.of(new PipeDeleteLogicalViewPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      return Optional.empty();
    } catch (final AuthException e) {
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeleteLogicalViewPlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final String userName) {
    try {
      final Map<PartialPath, List<Template>> newTemplateSetInfo = new HashMap<>();
      for (final Map.Entry<PartialPath, List<Template>> templateEntry :
          pipeDeactivateTemplatePlan.getTemplateSetInfo().entrySet()) {
        for (final PartialPath intersectedPath :
            getAllIntersectedPatterns(
                templateEntry.getKey(), userName, pipeDeactivateTemplatePlan)) {
          // root.db.device2.measurement -> root.db.device.** = root.db
          // Note that we cannot take this circumstance into account
          if (intersectedPath.getNodeLength() == templateEntry.getKey().getNodeLength()) {
            newTemplateSetInfo.put(intersectedPath, templateEntry.getValue());
          }
        }
      }
      return !newTemplateSetInfo.isEmpty()
          ? Optional.of(new PipeDeactivateTemplatePlan(newTemplateSetInfo))
          : Optional.empty();
    } catch (final AuthException e) {
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException(
            "Not has privilege to transfer plan: " + pipeDeactivateTemplatePlan);
      }
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitTTL(final SetTTLPlan setTTLPlan, final String userName) {
    try {
      final List<PartialPath> paths =
          getAllIntersectedPatterns(
              new PartialPath(setTTLPlan.getPathPattern()), userName, setTTLPlan);
      // The intersectionList is either a singleton list or an empty list, because the pipe
      // pattern and TTL path are each either a prefix path or a full path
      return !paths.isEmpty() && paths.get(0).getNodeLength() == setTTLPlan.getPathPattern().length
          ? Optional.of(new SetTTLPlan(paths.get(0).getNodes(), setTTLPlan.getTTL()))
          : Optional.empty();
    } catch (final AuthException e) {
      if (skip) {
        return Optional.empty();
      } else {
        throw new AccessDeniedException("Not has privilege to transfer plan: " + setTTLPlan);
      }
    }
  }

  private List<PartialPath> getAllIntersectedPatterns(
      final PartialPath partialPath, final String userName, final ConfigPhysicalPlan plan)
      throws AuthException {
    final PathPatternTree thisPatternTree = new PathPatternTree();
    thisPatternTree.appendPathPattern(partialPath);
    thisPatternTree.constructTree();
    final PathPatternTree intersectedTree =
        thisPatternTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(userName));
    if (!skip && !thisPatternTree.equals(intersectedTree)) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + plan);
    }
    return intersectedTree.getAllPathPatterns();
  }

  private PathPatternTree getAuthorizedPTree(final String userName) throws AuthException {
    return ConfigNode.getInstance()
        .getConfigManager()
        .getPermissionManager()
        .fetchRawAuthorizedPTree(userName, PrivilegeType.READ_SCHEMA);
  }
}
