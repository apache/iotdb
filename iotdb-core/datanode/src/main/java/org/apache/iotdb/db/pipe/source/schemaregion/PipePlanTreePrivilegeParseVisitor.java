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

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipePlanTreePrivilegeParseVisitor
    extends PlanVisitor<Optional<PlanNode>, PipePlanTreePrivilegeParseVisitor.Context> {

  private final boolean skipIfNoPrivileges;

  public PipePlanTreePrivilegeParseVisitor(final boolean skipIfNoPrivileges) {
    this.skipIfNoPrivileges = skipIfNoPrivileges;
  }

  public Optional<PlanNode> process(final PlanNode node, final String username) {
    return super.process(node, new Context(username));
  }

  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final Context context) {
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitCreateTimeSeries(
      final CreateTimeSeriesNode node, final Context context) {
    return check(
        node, context, Collections.singletonList(node.getPath()), PrivilegeType.READ_SCHEMA);
  }

  @Override
  public Optional<PlanNode> visitCreateAlignedTimeSeries(
      final CreateAlignedTimeSeriesNode node, final Context context) {
    final List<PartialPath> paths = new ArrayList<>(node.getMeasurements().size());
    for (final String measurement : node.getMeasurements()) {
      paths.add(measurementPath(node.getDevicePath(), measurement));
    }
    final List<Integer> deniedIndexes =
        getDeniedIndexes(context, paths, PrivilegeType.READ_SCHEMA, node);
    if (deniedIndexes.size() == node.getMeasurements().size()) {
      return Optional.empty();
    }
    return Optional.of(
        new CreateAlignedTimeSeriesNode(
            node.getPlanNodeId(),
            node.getDevicePath(),
            trimList(node.getMeasurements(), deniedIndexes),
            trimList(node.getDataTypes(), deniedIndexes),
            trimList(node.getEncodings(), deniedIndexes),
            trimList(node.getCompressors(), deniedIndexes),
            trimList(node.getAliasList(), deniedIndexes),
            trimList(node.getTagsList(), deniedIndexes),
            trimList(node.getAttributesList(), deniedIndexes)));
  }

  @Override
  public Optional<PlanNode> visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesNode node, final Context context) {
    return trimMeasurementGroups(node, context, node.getMeasurementGroupMap())
        .map(
            measurementGroups ->
                new CreateMultiTimeSeriesNode(node.getPlanNodeId(), measurementGroups));
  }

  @Override
  public Optional<PlanNode> visitAlterTimeSeries(
      final AlterTimeSeriesNode node, final Context context) {
    return check(
        node, context, Collections.singletonList(node.getPath()), PrivilegeType.READ_SCHEMA);
  }

  @Override
  public Optional<PlanNode> visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final Context context) {
    final MeasurementGroup group =
        trimMeasurementGroup(node, context, node.getDevicePath(), node.getMeasurementGroup());
    return group == null
        ? Optional.empty()
        : Optional.of(
            new InternalCreateTimeSeriesNode(
                node.getPlanNodeId(), node.getDevicePath(), group, node.isAligned()));
  }

  @Override
  public Optional<PlanNode> visitActivateTemplate(
      final ActivateTemplateNode node, final Context context) {
    return check(
        node,
        context,
        new ActivateTemplateStatement(node.getActivatePath()).getPaths(),
        PrivilegeType.READ_SCHEMA);
  }

  @Override
  public Optional<PlanNode> visitBatchActivateTemplate(
      final BatchActivateTemplateNode node, final Context context) {
    return trimTemplateActivations(node, context, node.getTemplateActivationMap())
        .map(activations -> new BatchActivateTemplateNode(node.getPlanNodeId(), activations));
  }

  @Override
  public Optional<PlanNode> visitInternalBatchActivateTemplate(
      final InternalBatchActivateTemplateNode node, final Context context) {
    return trimTemplateActivations(node, context, node.getTemplateActivationMap())
        .map(
            activations ->
                new InternalBatchActivateTemplateNode(node.getPlanNodeId(), activations));
  }

  @Override
  public Optional<PlanNode> visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final Context context) {
    final Map<PartialPath, Pair<Boolean, MeasurementGroup>> filteredDeviceMap = new HashMap<>();
    for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry :
        node.getDeviceMap().entrySet()) {
      final MeasurementGroup group =
          trimMeasurementGroup(node, context, entry.getKey(), entry.getValue().getRight());
      if (group != null) {
        filteredDeviceMap.put(entry.getKey(), new Pair<>(entry.getValue().getLeft(), group));
      }
    }
    return filteredDeviceMap.isEmpty()
        ? Optional.empty()
        : Optional.of(
            new InternalCreateMultiTimeSeriesNode(node.getPlanNodeId(), filteredDeviceMap));
  }

  @Override
  public Optional<PlanNode> visitCreateLogicalView(
      final CreateLogicalViewNode node, final Context context) {
    final Map<PartialPath, ViewExpression> filteredMap =
        trimLogicalViewMap(node, context, node.getViewPathToSourceExpressionMap());
    return filteredMap.isEmpty()
        ? Optional.empty()
        : Optional.of(new CreateLogicalViewNode(node.getPlanNodeId(), filteredMap));
  }

  @Override
  public Optional<PlanNode> visitAlterLogicalView(
      final AlterLogicalViewNode node, final Context context) {
    final Map<PartialPath, ViewExpression> filteredMap =
        trimLogicalViewMap(node, context, node.getViewPathToSourceMap());
    return filteredMap.isEmpty()
        ? Optional.empty()
        : Optional.of(new AlterLogicalViewNode(node.getPlanNodeId(), filteredMap));
  }

  private Map<PartialPath, ViewExpression> trimLogicalViewMap(
      final PlanNode node,
      final Context context,
      final Map<PartialPath, ViewExpression> viewPathToSourceMap) {
    final Map<PartialPath, ViewExpression> filteredMap = new HashMap<>(viewPathToSourceMap);
    final List<PartialPath> targetPaths = new ArrayList<>(filteredMap.keySet());
    final Set<Integer> deniedTargetIndexes =
        new HashSet<>(getDeniedIndexes(context, targetPaths, PrivilegeType.READ_SCHEMA, node));

    final List<PartialPath> sourcePaths = new ArrayList<>();
    final List<Integer> sourcePathToTargetIndexes = new ArrayList<>();
    for (int targetIndex = 0; targetIndex < targetPaths.size(); targetIndex++) {
      for (final PartialPath sourcePath :
          GetSourcePathsVisitor.getSourcePaths(filteredMap.get(targetPaths.get(targetIndex)))) {
        sourcePaths.add(sourcePath);
        sourcePathToTargetIndexes.add(targetIndex);
      }
    }
    final List<Integer> deniedSourceIndexes =
        getDeniedIndexes(context, sourcePaths, PrivilegeType.READ_SCHEMA, node);
    for (final int deniedSourceIndex : deniedSourceIndexes) {
      deniedTargetIndexes.add(sourcePathToTargetIndexes.get(deniedSourceIndex));
    }
    deniedTargetIndexes.forEach(index -> filteredMap.remove(targetPaths.get(index)));
    return filteredMap;
  }

  @Override
  public Optional<PlanNode> visitDeleteData(final DeleteDataNode node, final Context context) {
    if (context.username == null) {
      return noPrivilege(node, PrivilegeType.READ_DATA, context);
    }
    if (AuthorityChecker.SUPER_USER.equals(context.username)) {
      return Optional.of(node);
    }
    final PathPatternTree originalTree = new PathPatternTree();
    node.getPathList().forEach(originalTree::appendPathPattern);
    originalTree.constructTree();
    try {
      final PathPatternTree authorizedTree =
          AuthorityChecker.getAuthorizedPathTree(
              context.username, PrivilegeType.READ_DATA.ordinal());
      final PathPatternTree intersectedTree =
          originalTree.intersectWithFullPathPrefixTree(authorizedTree);
      if (!skipIfNoPrivileges && !originalTree.equals(intersectedTree)) {
        throw noPrivilegeException(node, PrivilegeType.READ_DATA, context);
      }
      final List<PartialPath> authorizedPaths = intersectedTree.getAllPathPatterns();
      return authorizedPaths.isEmpty()
          ? Optional.empty()
          : Optional.of(
              new DeleteDataNode(
                  node.getPlanNodeId(),
                  authorizedPaths,
                  node.getDeleteStartTime(),
                  node.getDeleteEndTime()));
    } catch (final AuthException e) {
      if (skipIfNoPrivileges) {
        return Optional.empty();
      }
      throw new PipeException("Failed to check READ_DATA privilege for schema event.", e);
    }
  }

  private Optional<Map<PartialPath, MeasurementGroup>> trimMeasurementGroups(
      final PlanNode node,
      final Context context,
      final Map<PartialPath, MeasurementGroup> measurementGroups) {
    final Map<PartialPath, MeasurementGroup> filteredMeasurementGroups = new HashMap<>();
    for (final Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroups.entrySet()) {
      final MeasurementGroup group =
          trimMeasurementGroup(node, context, entry.getKey(), entry.getValue());
      if (group != null) {
        filteredMeasurementGroups.put(entry.getKey(), group);
      }
    }
    return filteredMeasurementGroups.isEmpty()
        ? Optional.empty()
        : Optional.of(filteredMeasurementGroups);
  }

  private MeasurementGroup trimMeasurementGroup(
      final PlanNode node,
      final Context context,
      final PartialPath devicePath,
      final MeasurementGroup measurementGroup) {
    final List<PartialPath> paths = new ArrayList<>(measurementGroup.size());
    for (final String measurement : measurementGroup.getMeasurements()) {
      paths.add(measurementPath(devicePath, measurement));
    }
    final List<Integer> deniedIndexes =
        getDeniedIndexes(context, paths, PrivilegeType.READ_SCHEMA, node);
    if (deniedIndexes.size() == measurementGroup.size()) {
      return null;
    }
    final Set<Integer> deniedIndexSet = new HashSet<>(deniedIndexes);
    final MeasurementGroup filteredGroup = new MeasurementGroup();
    for (int index = 0; index < measurementGroup.size(); index++) {
      if (deniedIndexSet.contains(index)) {
        continue;
      }
      filteredGroup.addMeasurement(
          measurementGroup.getMeasurements().get(index),
          measurementGroup.getDataTypes().get(index),
          measurementGroup.getEncodings().get(index),
          measurementGroup.getCompressors().get(index));
      if (measurementGroup.getAliasList() != null) {
        filteredGroup.addAlias(measurementGroup.getAliasList().get(index));
      }
      if (measurementGroup.getPropsList() != null) {
        filteredGroup.addProps(measurementGroup.getPropsList().get(index));
      }
      if (measurementGroup.getTagsList() != null) {
        filteredGroup.addTags(measurementGroup.getTagsList().get(index));
      }
      if (measurementGroup.getAttributesList() != null) {
        filteredGroup.addAttributes(measurementGroup.getAttributesList().get(index));
      }
    }
    return filteredGroup;
  }

  private Optional<Map<PartialPath, Pair<Integer, Integer>>> trimTemplateActivations(
      final PlanNode node,
      final Context context,
      final Map<PartialPath, Pair<Integer, Integer>> activations) {
    final Map<PartialPath, Pair<Integer, Integer>> filteredActivations = new HashMap<>();
    for (final Map.Entry<PartialPath, Pair<Integer, Integer>> entry : activations.entrySet()) {
      final List<PartialPath> paths = new ActivateTemplateStatement(entry.getKey()).getPaths();
      if (getDeniedIndexes(context, paths, PrivilegeType.READ_SCHEMA, node).isEmpty()) {
        filteredActivations.put(entry.getKey(), entry.getValue());
      }
    }
    return filteredActivations.isEmpty() ? Optional.empty() : Optional.of(filteredActivations);
  }

  private <T> List<T> trimList(final List<T> values, final List<Integer> deniedIndexes) {
    if (values == null) {
      return null;
    }
    final Set<Integer> deniedIndexSet = new HashSet<>(deniedIndexes);
    final List<T> filteredValues = new ArrayList<>(values.size() - deniedIndexes.size());
    for (int index = 0; index < values.size(); index++) {
      if (!deniedIndexSet.contains(index)) {
        filteredValues.add(values.get(index));
      }
    }
    return filteredValues;
  }

  private PartialPath measurementPath(final PartialPath devicePath, final String measurement) {
    return devicePath.concatAsMeasurementPath(measurement);
  }

  private Optional<PlanNode> check(
      final PlanNode node,
      final Context context,
      final List<PartialPath> paths,
      final PrivilegeType privilegeType) {
    return getDeniedIndexes(context, paths, privilegeType, node).isEmpty()
        ? Optional.of(node)
        : Optional.empty();
  }

  private List<Integer> getDeniedIndexes(
      final Context context,
      final List<PartialPath> paths,
      final PrivilegeType privilegeType,
      final PlanNode node) {
    if (paths.isEmpty() || AuthorityChecker.SUPER_USER.equals(context.username)) {
      return Collections.emptyList();
    }
    final List<Integer> deniedIndexes =
        context.username == null
            ? IntStream.range(0, paths.size()).boxed().collect(Collectors.toList())
            : AuthorityChecker.checkFullPathListPermission(
                context.username, paths, privilegeType.ordinal());
    if (!deniedIndexes.isEmpty() && !skipIfNoPrivileges) {
      throw noPrivilegeException(node, privilegeType, context);
    }
    return deniedIndexes;
  }

  private Optional<PlanNode> noPrivilege(
      final PlanNode node, final PrivilegeType privilegeType, final Context context) {
    if (skipIfNoPrivileges) {
      return Optional.empty();
    }
    throw noPrivilegeException(node, privilegeType, context);
  }

  private PipeException noPrivilegeException(
      final PlanNode node, final PrivilegeType privilegeType, final Context context) {
    return new PipeException(
        String.format(
            "Source user %s lacks %s privilege for schema event %s.",
            context.username == null ? "<unknown>" : context.username, privilegeType, node));
  }

  static class Context {
    private final String username;

    private Context(final String username) {
      this.username = username;
    }
  }
}
