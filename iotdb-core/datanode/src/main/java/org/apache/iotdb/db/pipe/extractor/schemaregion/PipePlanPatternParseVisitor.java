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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The {@link PipePlanPatternParseVisitor} will transform the schema {@link PlanNode}s using {@link
 * IoTDBTreePattern}. Rule:
 *
 * <p>1. All patterns in the output {@link PlanNode} will be the intersection of the original {@link
 * PlanNode}'s patterns and the given {@link IoTDBTreePattern}.
 *
 * <p>2. If a pattern does not intersect with the {@link IoTDBTreePattern}, it's dropped.
 *
 * <p>3. If all the patterns in the {@link PlanNode} is dropped, the {@link PlanNode} is dropped.
 *
 * <p>4. The output {@link PlanNode} shall be a copied form of the original one because the original
 * one is used in the {@link PipeSchemaRegionWritePlanEvent} in {@link SchemaRegionListeningQueue}.
 */
public class PipePlanPatternParseVisitor extends PlanVisitor<Optional<PlanNode>, IoTDBTreePattern> {
  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final IoTDBTreePattern pattern) {
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitCreateTimeSeries(
      final CreateTimeSeriesNode node, final IoTDBTreePattern pattern) {
    return pattern.matchesMeasurement(
            node.getPath().getIDeviceID(), node.getPath().getMeasurement())
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateAlignedTimeSeries(
      final CreateAlignedTimeSeriesNode node, final IoTDBTreePattern pattern) {
    final int[] filteredIndexes =
        IntStream.range(0, node.getMeasurements().size())
            .filter(
                index ->
                    pattern.matchesMeasurement(
                        node.getDevicePath().getIDeviceIDAsFullDevice(),
                        node.getMeasurements().get(index)))
            .toArray();
    return filteredIndexes.length > 0
        ? Optional.of(
            new CreateAlignedTimeSeriesNode(
                node.getPlanNodeId(),
                node.getDevicePath(),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getMeasurements()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getDataTypes()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getEncodings()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getCompressors()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getAliasList()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getTagsList()),
                IoTDBTreePattern.applyIndexesOnList(filteredIndexes, node.getAttributesList())))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, MeasurementGroup> filteredMeasurementGroupMap =
        node.getMeasurementGroupMap().entrySet().stream()
            .filter(entry -> pattern.matchPrefixPath(entry.getKey().getFullPath()))
            .map(
                entry ->
                    new Pair<>(
                        entry.getKey(),
                        trimMeasurementGroup(
                            entry.getKey().getIDeviceIDAsFullDevice(), entry.getValue(), pattern)))
            .filter(pair -> Objects.nonNull(pair.getRight()))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return !filteredMeasurementGroupMap.isEmpty()
        ? Optional.of(
            new CreateMultiTimeSeriesNode(node.getPlanNodeId(), filteredMeasurementGroupMap))
        : Optional.empty();
  }

  private static MeasurementGroup trimMeasurementGroup(
      final IDeviceID device, final MeasurementGroup group, final IoTDBTreePattern pattern) {
    final int[] filteredIndexes =
        IntStream.range(0, group.size())
            .filter(index -> pattern.matchesMeasurement(device, group.getMeasurements().get(index)))
            .toArray();
    if (filteredIndexes.length == 0) {
      return null;
    }
    final MeasurementGroup targetMeasurementGroup = new MeasurementGroup();
    Arrays.stream(filteredIndexes)
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
      final AlterTimeSeriesNode node, final IoTDBTreePattern pattern) {
    return pattern.matchesMeasurement(
            node.getPath().getIDeviceID(), node.getPath().getMeasurement())
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final IoTDBTreePattern pattern) {
    final MeasurementGroup group =
        pattern.matchPrefixPath(node.getDevicePath().getFullPath())
            ? trimMeasurementGroup(
                node.getDevicePath().getIDeviceIDAsFullDevice(),
                node.getMeasurementGroup(),
                pattern)
            : null;
    return Objects.nonNull(group)
        ? Optional.of(
            new InternalCreateTimeSeriesNode(
                node.getPlanNodeId(), node.getDevicePath(), group, node.isAligned()))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitActivateTemplate(
      final ActivateTemplateNode node, final IoTDBTreePattern pattern) {
    return pattern.matchDevice(node.getActivatePath().getFullPath())
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitInternalBatchActivateTemplate(
      final InternalBatchActivateTemplateNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, Pair<Integer, Integer>> filteredTemplateActivationMap =
        node.getTemplateActivationMap().entrySet().stream()
            .filter(entry -> pattern.matchDevice(entry.getKey().getFullPath()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return !filteredTemplateActivationMap.isEmpty()
        ? Optional.of(
            new InternalBatchActivateTemplateNode(
                node.getPlanNodeId(), filteredTemplateActivationMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, Pair<Boolean, MeasurementGroup>> filteredDeviceMap =
        node.getDeviceMap().entrySet().stream()
            .filter(entry -> pattern.matchPrefixPath(entry.getKey().getFullPath()))
            .map(
                entry ->
                    new Pair<>(
                        entry.getKey(),
                        new Pair<>(
                            entry.getValue().getLeft(),
                            trimMeasurementGroup(
                                entry.getKey().getIDeviceIDAsFullDevice(),
                                entry.getValue().getRight(),
                                pattern))))
            .filter(pair -> Objects.nonNull(pair.getRight().getRight()))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    return !filteredDeviceMap.isEmpty()
        ? Optional.of(
            new InternalCreateMultiTimeSeriesNode(node.getPlanNodeId(), filteredDeviceMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitBatchActivateTemplate(
      final BatchActivateTemplateNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, Pair<Integer, Integer>> filteredTemplateActivationMap =
        node.getTemplateActivationMap().entrySet().stream()
            .filter(entry -> pattern.matchDevice(entry.getKey().getFullPath()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return !filteredTemplateActivationMap.isEmpty()
        ? Optional.of(
            new BatchActivateTemplateNode(node.getPlanNodeId(), filteredTemplateActivationMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitCreateLogicalView(
      final CreateLogicalViewNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, ViewExpression> filteredViewPathToSourceMap =
        node.getViewPathToSourceExpressionMap().entrySet().stream()
            .filter(
                entry ->
                    pattern.matchesMeasurement(
                        entry.getKey().getIDeviceID(), entry.getKey().getMeasurement()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return !filteredViewPathToSourceMap.isEmpty()
        ? Optional.of(new CreateLogicalViewNode(node.getPlanNodeId(), filteredViewPathToSourceMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitAlterLogicalView(
      final AlterLogicalViewNode node, final IoTDBTreePattern pattern) {
    final Map<PartialPath, ViewExpression> filteredViewPathToSourceMap =
        node.getViewPathToSourceMap().entrySet().stream()
            .filter(
                entry ->
                    pattern.matchesMeasurement(
                        entry.getKey().getIDeviceID(), entry.getKey().getMeasurement()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return !filteredViewPathToSourceMap.isEmpty()
        ? Optional.of(new AlterLogicalViewNode(node.getPlanNodeId(), filteredViewPathToSourceMap))
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitDeleteData(
      final DeleteDataNode node, final IoTDBTreePattern pattern) {
    final List<MeasurementPath> intersectedPaths =
        node.getPathList().stream()
            .map(pattern::getIntersection)
            .flatMap(Collection::stream)
            .distinct()
            .map(d -> (MeasurementPath) d)
            .collect(Collectors.toList());
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
