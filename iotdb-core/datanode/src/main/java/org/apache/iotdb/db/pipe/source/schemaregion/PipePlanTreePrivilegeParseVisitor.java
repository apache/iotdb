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

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipePlanTreePrivilegeParseVisitor
    extends PlanVisitor<Optional<PlanNode>, IAuditEntity> {

  private final boolean skip;

  public PipePlanTreePrivilegeParseVisitor(final boolean skip) {
    this.skip = skip;
  }

  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final IAuditEntity context) {
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitCreateTimeSeries(
      final CreateTimeSeriesNode node, final IAuditEntity auditEntity) {
    return AuthorityChecker.getAccessControl()
                .checkSeriesPrivilege4Pipe(
                    auditEntity,
                    Collections.singletonList(node.getPath()),
                    PrivilegeType.READ_SCHEMA)
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateAlignedTimeSeries(
      final CreateAlignedTimeSeriesNode node, final IAuditEntity auditEntity) {
    final List<Integer> failedIndexes =
        AuthorityChecker.getAccessControl()
            .checkSeriesPrivilegeWithIndexes4Pipe(
                auditEntity,
                node.getMeasurements().stream()
                    .map(measurement -> node.getDevicePath().concatAsMeasurementPath(measurement))
                    .collect(Collectors.toList()),
                PrivilegeType.READ_SCHEMA);
    if (!skip && !failedIndexes.isEmpty()) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
    }
    return failedIndexes.size() != node.getMeasurements().size()
        ? Optional.of(
            new CreateAlignedTimeSeriesNode(
                node.getPlanNodeId(),
                node.getDevicePath(),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getMeasurements()),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getDataTypes()),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getEncodings()),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getCompressors()),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getAliasList()),
                IoTDBTreePattern.applyReversedIndexesOnList(failedIndexes, node.getTagsList()),
                IoTDBTreePattern.applyReversedIndexesOnList(
                    failedIndexes, node.getAttributesList())))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, MeasurementGroup> filteredMeasurementGroupMap =
        node.getMeasurementGroupMap().entrySet().stream()
            .map(
                entry ->
                    new Pair<>(
                        entry.getKey(),
                        trimMeasurementGroup(entry.getKey(), entry.getValue(), auditEntity, node)))
            .filter(pair -> Objects.nonNull(pair.getRight()))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return !filteredMeasurementGroupMap.isEmpty()
        ? Optional.of(
            new CreateMultiTimeSeriesNode(node.getPlanNodeId(), filteredMeasurementGroupMap))
        : Optional.empty();
  }

  private MeasurementGroup trimMeasurementGroup(
      final PartialPath device,
      final MeasurementGroup group,
      final IAuditEntity entity,
      final PlanNode node) {
    final Set<Integer> failedIndexes =
        new HashSet<>(
            AuthorityChecker.getAccessControl()
                .checkSeriesPrivilegeWithIndexes4Pipe(
                    entity,
                    group.getMeasurements().stream()
                        .map(device::concatAsMeasurementPath)
                        .collect(Collectors.toList()),
                    PrivilegeType.READ_SCHEMA));
    if (!skip && !failedIndexes.isEmpty()) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
    }
    if (failedIndexes.size() == group.size()) {
      return null;
    }
    final MeasurementGroup targetMeasurementGroup = new MeasurementGroup();
    IntStream.range(0, group.size())
        .filter(index -> !failedIndexes.contains(index))
        .forEach(
            index -> {
              targetMeasurementGroup.addMeasurement(
                  group.getMeasurements().get(index),
                  group.getDataTypes().get(index),
                  group.getEncodings().get(index),
                  group.getCompressors().get(index));
              if (Objects.nonNull(group.getTagsList())) {
                targetMeasurementGroup.addTags(group.getTagsList().get(index));
              }
              if (Objects.nonNull(group.getAttributesList())) {
                targetMeasurementGroup.addAttributes(group.getAttributesList().get(index));
              }
              if (Objects.nonNull(group.getAliasList())) {
                targetMeasurementGroup.addAlias(group.getAliasList().get(index));
              }
              if (Objects.nonNull(group.getPropsList())) {
                targetMeasurementGroup.addProps(group.getPropsList().get(index));
              }
            });
    return targetMeasurementGroup;
  }

  @Override
  public Optional<PlanNode> visitAlterTimeSeries(
      final AlterTimeSeriesNode node, final IAuditEntity auditEntity) {
    return AuthorityChecker.getAccessControl()
                .checkSeriesPrivilege4Pipe(
                    auditEntity,
                    Collections.singletonList(node.getPath()),
                    PrivilegeType.READ_SCHEMA)
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final IAuditEntity auditEntity) {
    final MeasurementGroup group =
        trimMeasurementGroup(node.getDevicePath(), node.getMeasurementGroup(), auditEntity, node);
    return Objects.nonNull(group)
        ? Optional.of(
            new InternalCreateTimeSeriesNode(
                node.getPlanNodeId(), node.getDevicePath(), group, node.isAligned()))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitActivateTemplate(
      final ActivateTemplateNode node, final IAuditEntity auditEntity) {
    final List<Integer> failedPos =
        AuthorityChecker.getAccessControl()
            .checkSeriesPrivilegeWithIndexes4Pipe(
                auditEntity,
                ActivateTemplateStatement.getPaths(node.getActivatePath()),
                PrivilegeType.READ_SCHEMA);
    if (!failedPos.isEmpty()) {
      if (!skip) {
        throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
      }
      return Optional.empty();
    }
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitInternalBatchActivateTemplate(
      final InternalBatchActivateTemplateNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, Pair<Integer, Integer>> filteredMap = new HashMap<>();
    for (final Map.Entry<PartialPath, Pair<Integer, Integer>> pathEntry :
        node.getTemplateActivationMap().entrySet()) {
      final List<Integer> failedIndexes =
          AuthorityChecker.getAccessControl()
              .checkSeriesPrivilegeWithIndexes4Pipe(
                  auditEntity,
                  ActivateTemplateStatement.getPaths(pathEntry.getKey()),
                  PrivilegeType.READ_SCHEMA);
      if (failedIndexes.isEmpty()) {
        filteredMap.put(pathEntry.getKey(), pathEntry.getValue());
      } else if (!skip) {
        throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
      }
    }
    return !filteredMap.isEmpty()
        ? Optional.of(new InternalBatchActivateTemplateNode(node.getPlanNodeId(), filteredMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, Pair<Boolean, MeasurementGroup>> filteredDeviceMap =
        node.getDeviceMap().entrySet().stream()
            .map(
                entry ->
                    new Pair<>(
                        entry.getKey(),
                        new Pair<>(
                            entry.getValue().getLeft(),
                            trimMeasurementGroup(
                                entry.getKey(), entry.getValue().getRight(), auditEntity, node))))
            .filter(pair -> Objects.nonNull(pair.getRight().getRight()))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return !filteredDeviceMap.isEmpty()
        ? Optional.of(
            new InternalCreateMultiTimeSeriesNode(node.getPlanNodeId(), filteredDeviceMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitBatchActivateTemplate(
      final BatchActivateTemplateNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, Pair<Integer, Integer>> filteredMap = new HashMap<>();
    for (final Map.Entry<PartialPath, Pair<Integer, Integer>> pathEntry :
        node.getTemplateActivationMap().entrySet()) {
      final List<Integer> failedIndexes =
          AuthorityChecker.getAccessControl()
              .checkSeriesPrivilegeWithIndexes4Pipe(
                  auditEntity,
                  ActivateTemplateStatement.getPaths(pathEntry.getKey()),
                  PrivilegeType.READ_SCHEMA);
      if (failedIndexes.isEmpty()) {
        filteredMap.put(pathEntry.getKey(), pathEntry.getValue());
      } else if (!skip) {
        throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
      }
    }
    return !filteredMap.isEmpty()
        ? Optional.of(new BatchActivateTemplateNode(node.getPlanNodeId(), filteredMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateLogicalView(
      final CreateLogicalViewNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, ViewExpression> filteredMap =
        new HashMap<>(node.getViewPathToSourceExpressionMap());
    final List<PartialPath> viewPathList = node.getViewPathList();
    final List<Integer> failedIndexes =
        AuthorityChecker.getAccessControl()
            .checkSeriesPrivilegeWithIndexes4Pipe(
                auditEntity, viewPathList, PrivilegeType.READ_SCHEMA);
    if (!skip && !failedIndexes.isEmpty()) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
    }
    failedIndexes.forEach(index -> filteredMap.remove(viewPathList.get(index)));
    return !filteredMap.isEmpty()
        ? Optional.of(new CreateLogicalViewNode(node.getPlanNodeId(), filteredMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitAlterLogicalView(
      final AlterLogicalViewNode node, final IAuditEntity auditEntity) {
    final Map<PartialPath, ViewExpression> filteredMap =
        new HashMap<>(node.getViewPathToSourceMap());
    final List<PartialPath> viewPathList = new ArrayList<>(node.getViewPathToSourceMap().keySet());
    final List<Integer> failedIndexes =
        AuthorityChecker.getAccessControl()
            .checkSeriesPrivilegeWithIndexes4Pipe(
                auditEntity, viewPathList, PrivilegeType.READ_SCHEMA);
    if (!skip && !failedIndexes.isEmpty()) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
    }
    failedIndexes.forEach(index -> filteredMap.remove(viewPathList.get(index)));
    return !filteredMap.isEmpty()
        ? Optional.of(new AlterLogicalViewNode(node.getPlanNodeId(), filteredMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitDeleteData(
      final DeleteDataNode node, final IAuditEntity auditEntity) {
    final List<MeasurementPath> intersectedPaths =
        TreeAccessCheckVisitor.getIntersectedPaths4Pipe(
            node.getPathList(),
            new TreeAccessCheckContext(
                auditEntity.getUserId(), auditEntity.getUsername(), auditEntity.getCliHostname()));
    if (!skip && !intersectedPaths.equals(node.getPathList())) {
      throw new AccessDeniedException("Not has privilege to transfer plan: " + node);
    }
    return !intersectedPaths.isEmpty()
        ? Optional.of(
            new DeleteDataNode(
                node.getPlanNodeId(),
                intersectedPaths,
                node.getDeleteStartTime(),
                node.getDeleteEndTime()))
        : Optional.empty();
  }
}
