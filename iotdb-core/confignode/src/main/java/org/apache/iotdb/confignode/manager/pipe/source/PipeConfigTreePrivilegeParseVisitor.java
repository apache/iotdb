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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class PipeConfigTreePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, String> {

  private final boolean skipIfNoPrivileges;

  public PipeConfigTreePrivilegeParseVisitor(final boolean skipIfNoPrivileges) {
    this.skipIfNoPrivileges = skipIfNoPrivileges;
  }

  public List<ConfigPhysicalPlan> processAll(final ConfigPhysicalPlan plan, final String username) {
    if (plan instanceof SetTTLPlan) {
      return trimTTL((SetTTLPlan) plan, username);
    }
    return process(plan, username)
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }

  public boolean canSkipSnapshotPrivilegeCheck(
      final CNSnapshotFileType fileType, final String username) {
    switch (fileType) {
      case USER:
        return hasGlobalPrivilege(username, PrivilegeType.MANAGE_USER);
      case ROLE:
        return hasGlobalPrivilege(username, PrivilegeType.MANAGE_ROLE);
      case USER_ROLE:
        return hasGlobalPrivilege(username, PrivilegeType.MANAGE_USER)
            || hasGlobalPrivilege(username, PrivilegeType.MANAGE_ROLE);
      case SCHEMA:
        return hasReadPrivilege(username, PATH_ROOT, true);
      case TTL:
      default:
        return false;
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final String username) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan plan, final String username) {
    return canReadSysSchema(plan.getSchema().getName(), username, true)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan plan, final String username) {
    return canReadSysSchema(plan.getSchema().getName(), username, true)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan plan, final String username) {
    return canReadSysSchema(plan.getName(), username, true)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan plan, final String username) {
    return canShowSchemaTemplate(plan.getTemplate().getName(), username)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan plan, final String username) {
    return canReadSysSchema(plan.getPath(), username, false)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan plan, final String username) {
    return canReadSysSchema(plan.getPath(), username, false)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan plan, final String username) {
    return canShowSchemaTemplate(plan.getTemplateExtendInfo().getTemplateName(), username)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDropSchemaTemplate(
      final DropSchemaTemplatePlan plan, final String username) {
    return canShowSchemaTemplate(plan.getTemplateName(), username)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateUser(
      final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateRawUser(
      final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitUpdateUser(
      final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDropUser(final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantUser(final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorPlan plan, final String username) {
    return visitUserPlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateRole(
      final AuthorPlan plan, final String username) {
    return visitRolePlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDropRole(final AuthorPlan plan, final String username) {
    return visitRolePlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(final AuthorPlan plan, final String username) {
    return visitRolePlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorPlan plan, final String username) {
    return visitRolePlan(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRoleToUser(
      final AuthorPlan plan, final String username) {
    return canReadUserPlan(plan, username) || canReadRolePlan(plan, username)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRoleFromUser(
      final AuthorPlan plan, final String username) {
    return canReadUserPlan(plan, username) || canReadRolePlan(plan, username)
        ? Optional.of(plan)
        : noPrivilege(plan, username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan plan, final String username) {
    return trimPatternTree(
        plan, plan.getPatternTreeBytes(), bytes -> new PipeDeleteTimeSeriesPlan(bytes), username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan plan, final String username) {
    return trimPatternTree(
        plan, plan.getPatternTreeBytes(), bytes -> new PipeDeleteLogicalViewPlan(bytes), username);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan plan, final String username) {
    final Map<PartialPath, List<Template>> trimmedTemplateSetInfo = new HashMap<>();
    for (final Map.Entry<PartialPath, List<Template>> entry :
        plan.getTemplateSetInfo().entrySet()) {
      for (final PartialPath intersectedPath :
          getAllIntersectedPatterns(entry.getKey(), plan, username)) {
        if (intersectedPath.getNodeLength() == entry.getKey().getNodeLength()) {
          trimmedTemplateSetInfo.put(intersectedPath, entry.getValue());
        }
      }
    }
    return trimmedTemplateSetInfo.isEmpty()
        ? noPrivilege(plan, username)
        : Optional.of(new PipeDeactivateTemplatePlan(trimmedTemplateSetInfo));
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitTTL(final SetTTLPlan plan, final String username) {
    final List<ConfigPhysicalPlan> plans = trimTTL(plan, username);
    return plans.isEmpty() ? Optional.empty() : Optional.of(plans.get(0));
  }

  private List<ConfigPhysicalPlan> trimTTL(final SetTTLPlan plan, final String username) {
    final PartialPath originalPath = new PartialPath(plan.getPathPattern());
    final PartialPath effectivePath =
        plan.isDataBase() ? originalPath.concatNode(MULTI_LEVEL_PATH_WILDCARD) : originalPath;
    final List<PartialPath> paths =
        getAllIntersectedPatterns(effectivePath, plan, username).stream()
            .filter(effectivePath::include)
            .collect(Collectors.toList());
    if (paths.isEmpty()) {
      return noPrivilegeList(plan, username);
    }
    return createTTLPlans(paths, plan, originalPath, effectivePath);
  }

  static List<ConfigPhysicalPlan> createTTLPlans(
      final List<PartialPath> paths,
      final SetTTLPlan plan,
      final PartialPath originalPath,
      final PartialPath effectivePath) {
    return paths.stream()
        .<ConfigPhysicalPlan>map(
            path ->
                plan.isDataBase() && path.equals(effectivePath)
                    ? copyDatabaseTTL(plan, originalPath)
                    : new SetTTLPlan(path.getNodes(), plan.getTTL()))
        .collect(Collectors.toList());
  }

  private static ConfigPhysicalPlan copyDatabaseTTL(
      final SetTTLPlan plan, final PartialPath originalPath) {
    final SetTTLPlan copy = new SetTTLPlan(originalPath.getNodes(), plan.getTTL());
    copy.setDataBase(plan.isDataBase());
    return copy;
  }

  private List<ConfigPhysicalPlan> noPrivilegeList(
      final ConfigPhysicalPlan plan, final String username) {
    if (skipIfNoPrivileges) {
      return Collections.emptyList();
    }
    throw noPrivilegeException(plan, username);
  }

  private Optional<ConfigPhysicalPlan> trimPatternTree(
      final ConfigPhysicalPlan plan,
      final ByteBuffer serializedPatternTree,
      final Function<ByteBuffer, ConfigPhysicalPlan> planFactory,
      final String username) {
    try {
      final PathPatternTree originalTree = PathPatternTree.deserialize(serializedPatternTree);
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(getAuthorizedPTree(username));
      if (!skipIfNoPrivileges && !originalTree.equals(intersectedTree)) {
        return noPrivilege(plan, username);
      }
      return intersectedTree.isEmpty()
          ? noPrivilege(plan, username)
          : Optional.of(planFactory.apply(intersectedTree.serialize()));
    } catch (final IOException e) {
      return noPrivilege(plan, username);
    }
  }

  private List<PartialPath> getAllIntersectedPatterns(
      final PartialPath partialPath, final ConfigPhysicalPlan plan, final String username) {
    final PathPatternTree originalTree = new PathPatternTree();
    originalTree.appendPathPattern(partialPath);
    originalTree.constructTree();
    final PathPatternTree intersectedTree = new PathPatternTree();
    for (final PartialPath authorizedPath : getAuthorizedPTree(username).getAllPathPatterns()) {
      if (authorizedPath.endWithMultiLevelWildcard()) {
        partialPath
            .intersectWithPrefixPattern(authorizedPath)
            .forEach(intersectedTree::appendPathPattern);
      } else if (partialPath.matchFullPath(authorizedPath)) {
        intersectedTree.appendPathPattern(authorizedPath);
      }
    }
    intersectedTree.constructTree();
    if (!skipIfNoPrivileges && !originalTree.equals(intersectedTree)) {
      throw noPrivilegeException(plan, username);
    }
    return intersectedTree.getAllPathPatterns();
  }

  private PathPatternTree getAuthorizedPTree(final String username) {
    final PathPatternTree emptyTree = new PathPatternTree();
    if (isAdmin(username)) {
      emptyTree.appendPathPattern(
          new PartialPath(new String[] {PATH_ROOT, MULTI_LEVEL_PATH_WILDCARD}));
      emptyTree.constructTree();
      return emptyTree;
    }
    if (username == null) {
      return emptyTree;
    }
    final org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp response =
        ConfigNode.getInstance()
            .getConfigManager()
            .fetchAuthizedPatternTree(username, PrivilegeType.READ_SCHEMA.ordinal());
    if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (skipIfNoPrivileges) {
        return emptyTree;
      }
      throw noPrivilegeException(null, username);
    }
    return PathPatternTree.deserialize(ByteBuffer.wrap(response.getPathPatternTree()));
  }

  private boolean canReadSysSchema(
      final String path, final String username, final boolean canSkipMulti) {
    return canSkipMulti && hasReadPrivilege(username, path, false)
        || hasReadPrivilege(username, path, true);
  }

  private boolean canShowSchemaTemplate(final String templateName, final String username) {
    try {
      return ConfigNode.getInstance()
          .getConfigManager()
          .getClusterSchemaManager()
          .getPathsSetTemplate(templateName, SchemaConstant.ALL_MATCH_SCOPE)
          .getPathList()
          .stream()
          .anyMatch(path -> hasReadPrivilege(username, path, true));
    } catch (final Exception e) {
      return false;
    }
  }

  private boolean hasReadPrivilege(
      final String username, final String path, final boolean withWildcard) {
    if (isAdmin(username)) {
      return true;
    }
    if (username == null) {
      return false;
    }
    try {
      PartialPath partialPath = new PartialPath(path);
      if (withWildcard) {
        partialPath = partialPath.concatNode(MULTI_LEVEL_PATH_WILDCARD);
      }
      return ConfigNode.getInstance()
              .getConfigManager()
              .checkUserPrivileges(
                  username,
                  Collections.singletonList(partialPath),
                  PrivilegeType.READ_SCHEMA.ordinal())
              .getStatus()
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (final IllegalPathException e) {
      return false;
    }
  }

  private boolean hasGlobalPrivilege(final String username, final PrivilegeType privilegeType) {
    if (isAdmin(username)) {
      return true;
    }
    return username != null
        && ConfigNode.getInstance()
                .getConfigManager()
                .checkUserPrivileges(username, Collections.emptyList(), privilegeType.ordinal())
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private boolean canReadUserPlan(final AuthorPlan plan, final String username) {
    return username != null && username.equals(plan.getUserName())
        || hasGlobalPrivilege(username, PrivilegeType.MANAGE_USER);
  }

  private boolean canReadRolePlan(final AuthorPlan plan, final String username) {
    return username != null
            && ConfigNode.getInstance()
                    .getConfigManager()
                    .checkRoleOfUser(username, plan.getRoleName())
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || hasGlobalPrivilege(username, PrivilegeType.MANAGE_ROLE);
  }

  private Optional<ConfigPhysicalPlan> visitUserPlan(final AuthorPlan plan, final String username) {
    return canReadUserPlan(plan, username) ? Optional.of(plan) : noPrivilege(plan, username);
  }

  private Optional<ConfigPhysicalPlan> visitRolePlan(final AuthorPlan plan, final String username) {
    return canReadRolePlan(plan, username) ? Optional.of(plan) : noPrivilege(plan, username);
  }

  private Optional<ConfigPhysicalPlan> noPrivilege(
      final ConfigPhysicalPlan plan, final String username) {
    if (skipIfNoPrivileges) {
      return Optional.empty();
    }
    throw noPrivilegeException(plan, username);
  }

  private PipeException noPrivilegeException(final ConfigPhysicalPlan plan, final String username) {
    return new PipeException(
        String.format(
            "Source user %s does not have permission to transfer config-region plan %s.",
            username == null ? "<unknown>" : username, plan));
  }

  private boolean isAdmin(final String username) {
    return username != null
        && CommonDescriptor.getInstance().getConfig().getAdminName().equals(username);
  }
}
