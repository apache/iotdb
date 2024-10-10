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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
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
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The {@link PipeConfigPhysicalPlanPatternParseVisitor} will transform the schema {@link
 * ConfigPhysicalPlan}s using {@link IoTDBTreePattern}. Rule:
 *
 * <p>1. All patterns in the output {@link ConfigPhysicalPlan} will be the intersection of the
 * original {@link ConfigPhysicalPlan}'s patterns and the given {@link IoTDBTreePattern}.
 *
 * <p>2. If a pattern does not intersect with the {@link IoTDBTreePattern}, it's dropped.
 *
 * <p>3. If all the patterns in the {@link ConfigPhysicalPlan} is dropped, the {@link
 * ConfigPhysicalPlan} is dropped.
 *
 * <p>4. The output {@link PlanNode} shall be a copied form of the original one because the original
 * one is used in the {@link PipeConfigRegionWritePlanEvent} in {@link ConfigRegionListeningQueue}.
 */
public class PipeConfigPhysicalPlanPatternParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IoTDBTreePattern> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConfigPhysicalPlanPatternParseVisitor.class);

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final IoTDBTreePattern pattern) {
    return Optional.of(plan);
  }

  // Note: This will return true even if the pattern matches the database precisely,
  // like database is "root.db.a" and pattern is "root.db.a". In this case, none of
  // the data and time series under this database will be transferred, however we
  // interpret user's pattern as "send precisely the database" and transfer it
  // nonetheless.
  // Other matches using "matchPrefixPath" are with the same principle.
  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final IoTDBTreePattern pattern) {
    return pattern.matchPrefixPath(createDatabasePlan.getSchema().getName())
        ? Optional.of(createDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final IoTDBTreePattern pattern) {
    return pattern.matchPrefixPath(alterDatabasePlan.getSchema().getName())
        ? Optional.of(alterDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final IoTDBTreePattern pattern) {
    return pattern.matchPrefixPath(deleteDatabasePlan.getName())
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final IoTDBTreePattern pattern) {
    // This is a deserialized template and can be arbitrarily altered
    final Template template = createSchemaTemplatePlan.getTemplate();
    template.getSchemaMap().keySet().removeIf(measurement -> !pattern.matchTailNode(measurement));
    return !template.getSchemaMap().isEmpty()
        ? Optional.of(new CreateSchemaTemplatePlan(template.serialize().array()))
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan,
      final IoTDBTreePattern pattern) {
    return pattern.matchPrefixPath(commitSetSchemaTemplatePlan.getPath())
        ? Optional.of(commitSetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan,
      final IoTDBTreePattern pattern) {
    return pattern.matchPrefixPath(pipeUnsetSchemaTemplatePlan.getPath())
        ? Optional.of(pipeUnsetSchemaTemplatePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final IoTDBTreePattern pattern) {
    final TemplateExtendInfo extendInfo = extendSchemaTemplatePlan.getTemplateExtendInfo();
    final int[] filteredIndexes =
        IntStream.range(0, extendInfo.getMeasurements().size())
            .filter(index -> pattern.matchTailNode(extendInfo.getMeasurements().get(index)))
            .toArray();
    return filteredIndexes.length > 0
        ? Optional.of(
            new ExtendSchemaTemplatePlan(
                new TemplateExtendInfo(
                    extendInfo.getTemplateName(),
                    IoTDBTreePattern.applyIndexesOnList(
                        filteredIndexes, extendInfo.getMeasurements()),
                    IoTDBTreePattern.applyIndexesOnList(filteredIndexes, extendInfo.getDataTypes()),
                    IoTDBTreePattern.applyIndexesOnList(filteredIndexes, extendInfo.getEncodings()),
                    IoTDBTreePattern.applyIndexesOnList(
                        filteredIndexes, extendInfo.getCompressors()))))
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantUser(
      final AuthorPlan grantUserPlan, final IoTDBTreePattern pattern) {
    return visitPathRelatedAuthorPlan(grantUserPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorPlan revokeUserPlan, final IoTDBTreePattern pattern) {
    return visitPathRelatedAuthorPlan(revokeUserPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(
      final AuthorPlan revokeUserPlan, final IoTDBTreePattern pattern) {
    return visitPathRelatedAuthorPlan(revokeUserPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorPlan revokeUserPlan, final IoTDBTreePattern pattern) {
    return visitPathRelatedAuthorPlan(revokeUserPlan, pattern);
  }

  private Optional<ConfigPhysicalPlan> visitPathRelatedAuthorPlan(
      final AuthorPlan pathRelatedAuthorPlan, final IoTDBTreePattern pattern) {
    final List<PartialPath> intersectedPaths =
        pathRelatedAuthorPlan.getNodeNameList().stream()
            .map(pattern::getIntersection)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    return !intersectedPaths.isEmpty()
        ? Optional.of(
            new AuthorPlan(
                pathRelatedAuthorPlan.getAuthorType(),
                pathRelatedAuthorPlan.getUserName(),
                pathRelatedAuthorPlan.getRoleName(),
                pathRelatedAuthorPlan.getPassword(),
                pathRelatedAuthorPlan.getNewPassword(),
                pathRelatedAuthorPlan.getPermissions(),
                pathRelatedAuthorPlan.getGrantOpt(),
                intersectedPaths))
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final IoTDBTreePattern pattern) {
    try {
      final PathPatternTree intersectedTree =
          pattern.getIntersection(
              PathPatternTree.deserialize(pipeDeleteTimeSeriesPlan.getPatternTreeBytes()));
      return !intersectedTree.isEmpty()
          ? Optional.of(new PipeDeleteTimeSeriesPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete time series plan in pipe transmission, skip transfer",
          e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final IoTDBTreePattern pattern) {
    try {
      final PathPatternTree intersectedTree =
          pattern.getIntersection(
              PathPatternTree.deserialize(pipeDeleteLogicalViewPlan.getPatternTreeBytes()));
      return !intersectedTree.isEmpty()
          ? Optional.of(new PipeDeleteTimeSeriesPlan(intersectedTree.serialize()))
          : Optional.empty();
    } catch (final IOException e) {
      LOGGER.warn(
          "Serialization failed for the delete logical view plan in pipe transmission, skip transfer",
          e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final IoTDBTreePattern pattern) {
    final Map<PartialPath, List<Template>> newTemplateSetInfo =
        pipeDeactivateTemplatePlan.getTemplateSetInfo().entrySet().stream()
            .flatMap(
                entry ->
                    pattern.getIntersection(entry.getKey()).stream()
                        .map(partialPath -> new Pair<>(partialPath, entry.getValue())))
            .collect(
                Collectors.toMap(
                    Pair::getLeft,
                    Pair::getRight,
                    (oldTemplates, newTemplates) ->
                        Stream.of(oldTemplates, newTemplates)
                            .flatMap(Collection::stream)
                            .distinct()
                            .collect(Collectors.toList())));
    return !newTemplateSetInfo.isEmpty()
        ? Optional.of(new PipeDeactivateTemplatePlan(newTemplateSetInfo))
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitTTL(
      final SetTTLPlan setTTLPlan, final IoTDBTreePattern pattern) {
    final PartialPath partialPath = new PartialPath(setTTLPlan.getPathPattern());
    final List<PartialPath> intersectionList =
        pattern.matchPrefixPath(partialPath.getFullPath())
            ? Collections.singletonList(partialPath)
            : pattern.getIntersection(partialPath);
    // The intersectionList is either a singleton list or an empty list, because the pipe
    // pattern and TTL path are each either a prefix path or a full path
    return !intersectionList.isEmpty()
        ? Optional.of(new SetTTLPlan(intersectionList.get(0).getNodes(), setTTLPlan.getTTL()))
        : Optional.empty();
  }
}
