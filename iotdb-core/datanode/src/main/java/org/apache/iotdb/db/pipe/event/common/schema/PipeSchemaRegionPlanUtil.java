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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedNonWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utility methods for schema-region pipe plan events. */
public class PipeSchemaRegionPlanUtil {

  public static boolean isRenameInternalPlan(final PlanNode node) {
    if (node == null) {
      return false;
    }
    if (node.getType() == PlanNodeType.PIPE_ENRICHED_WRITE) {
      return isRenameInternalPlan(((PipeEnrichedWritePlanNode) node).getWritePlanNode());
    }
    if (node.getType() == PlanNodeType.PIPE_ENRICHED_NON_WRITE) {
      return isRenameInternalPlan(((PipeEnrichedNonWritePlanNode) node).getNonWritePlanNode());
    }
    switch (node.getType()) {
      case LOCK_ALIAS:
      case CREATE_ALIAS_SERIES:
      case MARK_SERIES_INVALID:
      case UPDATE_PHYSICAL_ALIAS_REF:
      case DROP_ALIAS_SERIES:
      case MARK_SERIES_ENABLED:
      case UNLOCK_FOR_ALIAS:
        return true;
      case CREATE_TIME_SERIES:
        return hasRealtimeNonTransferableRenameInternalProps(
            ((CreateTimeSeriesNode) node).getProps());
      case CREATE_MULTI_TIME_SERIES:
        return allMeasurementGroupsAreRenameInternal(
            ((CreateMultiTimeSeriesNode) node).getMeasurementGroupMap().values());
      case INTERNAL_CREATE_TIME_SERIES:
        return allMeasurementsAreRenameInternal(
            ((InternalCreateTimeSeriesNode) node).getMeasurementGroup());
      case INTERNAL_CREATE_MULTI_TIMESERIES:
        return allMeasurementGroupsAreRenameInternal(
            ((InternalCreateMultiTimeSeriesNode) node)
                .getDeviceMap().values().stream().map(Pair::getRight).collect(Collectors.toList()));
      case ALTER_TIME_SERIES:
        return isRenameInternalAlterTimeSeriesNode((AlterTimeSeriesNode) node);
      default:
        return false;
    }
  }

  public static boolean isRenameInternalStatement(final Statement statement) {
    if (statement instanceof CreateTimeSeriesStatement) {
      return hasNonTransferableRenameInternalProps(
          ((CreateTimeSeriesStatement) statement).getProps());
    }
    if (statement instanceof CreateAlignedTimeSeriesStatement) {
      return allPropsAreSnapshotNonTransferableRenameInternal(
          ((CreateAlignedTimeSeriesStatement) statement).getPropsList());
    }
    if (statement instanceof CreateMultiTimeSeriesStatement) {
      return allPropsAreSnapshotNonTransferableRenameInternal(
          ((CreateMultiTimeSeriesStatement) statement).getPropsList());
    }
    if (statement instanceof AlterTimeSeriesStatement) {
      return isRenameInternalAlterTimeSeriesStatement((AlterTimeSeriesStatement) statement);
    }
    return false;
  }

  public static boolean isInvalidPhysicalSnapshotStatement(final Statement statement) {
    return statement instanceof CreateTimeSeriesStatement
        && MeasurementPropsUtils.isInvalid(((CreateTimeSeriesStatement) statement).getProps());
  }

  private static boolean isRenameInternalAlterTimeSeriesNode(final AlterTimeSeriesNode node) {
    return hasRenameInternalProperty(node.getAlterMap());
  }

  private static boolean isRenameInternalAlterTimeSeriesStatement(
      final AlterTimeSeriesStatement statement) {
    return hasRenameInternalProperty(statement.getAlterMap());
  }

  private static boolean hasRenameInternalProperty(final Map<String, String> properties) {
    return properties != null
        && properties.keySet().stream().anyMatch(MeasurementPropsUtils::isInternalPropertyKey);
  }

  public static boolean hasNonTransferableRenameInternalProps(final Map<String, String> props) {
    return MeasurementPropsUtils.isRenamed(props) || MeasurementPropsUtils.isRenaming(props);
  }

  public static boolean hasRealtimeNonTransferableRenameInternalProps(
      final Map<String, String> props) {
    return MeasurementPropsUtils.isRenamed(props)
        || MeasurementPropsUtils.isRenaming(props)
        || MeasurementPropsUtils.isInvalid(props);
  }

  public static Map<String, String> sanitizeRenameInternalProps(final Map<String, String> props) {
    if (Objects.isNull(props)) {
      return null;
    }
    if (props.keySet().stream().noneMatch(MeasurementPropsUtils::isInternalPropertyKey)) {
      return props;
    }
    final Map<String, String> sanitizedProps = new HashMap<>(props);
    MeasurementPropsUtils.removeInternalProperties(sanitizedProps);
    return sanitizedProps.isEmpty() ? null : sanitizedProps;
  }

  public static Optional<Statement> sanitizeRenameInternalStatement(final Statement statement) {
    if (statement instanceof CreateTimeSeriesStatement) {
      return sanitizeCreateTimeSeriesStatement((CreateTimeSeriesStatement) statement);
    }
    if (statement instanceof CreateAlignedTimeSeriesStatement) {
      return sanitizeCreateAlignedTimeSeriesStatement((CreateAlignedTimeSeriesStatement) statement);
    }
    if (statement instanceof CreateMultiTimeSeriesStatement) {
      return sanitizeCreateMultiTimeSeriesStatement((CreateMultiTimeSeriesStatement) statement);
    }
    return Optional.of(statement);
  }

  private static Optional<Statement> sanitizeCreateTimeSeriesStatement(
      final CreateTimeSeriesStatement statement) {
    if (hasNonTransferableRenameInternalProps(statement.getProps())) {
      return Optional.empty();
    }
    statement.setProps(sanitizeRenameInternalProps(statement.getProps()));
    return Optional.of(statement);
  }

  private static Optional<Statement> sanitizeCreateAlignedTimeSeriesStatement(
      final CreateAlignedTimeSeriesStatement statement) {
    final CreateAlignedTimeSeriesStatement sanitizedStatement =
        new CreateAlignedTimeSeriesStatement();
    sanitizedStatement.setDevicePath(statement.getDevicePath());
    for (int i = 0; i < statement.getMeasurements().size(); i++) {
      if (i < statement.getPropsList().size()
          && hasNonTransferableRenameInternalProps(statement.getPropsList().get(i))) {
        continue;
      }
      sanitizedStatement.addMeasurement(statement.getMeasurements().get(i));
      sanitizedStatement.addDataType(statement.getDataTypes().get(i));
      sanitizedStatement.addEncoding(statement.getEncodings().get(i));
      sanitizedStatement.addCompressor(statement.getCompressors().get(i));
      sanitizedStatement.addTagsList(statement.getTagsList().get(i));
      sanitizedStatement.addAttributesList(statement.getAttributesList().get(i));
      sanitizedStatement.addAliasList(statement.getAliasList().get(i));
      if (i < statement.getPropsList().size()) {
        sanitizedStatement.addPropsList(
            sanitizeRenameInternalProps(statement.getPropsList().get(i)));
      }
    }
    return sanitizedStatement.getMeasurements().isEmpty()
        ? Optional.empty()
        : Optional.of(sanitizedStatement);
  }

  private static Optional<Statement> sanitizeCreateMultiTimeSeriesStatement(
      final CreateMultiTimeSeriesStatement statement) {
    final CreateMultiTimeSeriesStatement sanitizedStatement = new CreateMultiTimeSeriesStatement();
    sanitizedStatement.setPaths(new ArrayList<>());
    sanitizedStatement.setDataTypes(new ArrayList<>());
    sanitizedStatement.setEncodings(new ArrayList<>());
    sanitizedStatement.setCompressors(new ArrayList<>());
    sanitizedStatement.setPropsList(
        Objects.nonNull(statement.getPropsList()) ? new ArrayList<>() : null);
    sanitizedStatement.setAliasList(
        Objects.nonNull(statement.getAliasList()) ? new ArrayList<>() : null);
    sanitizedStatement.setTagsList(
        Objects.nonNull(statement.getTagsList()) ? new ArrayList<>() : null);
    sanitizedStatement.setAttributesList(
        Objects.nonNull(statement.getAttributesList()) ? new ArrayList<>() : null);
    for (int i = 0; i < statement.getPaths().size(); i++) {
      if (i < size(statement.getPropsList())
          && hasNonTransferableRenameInternalProps(statement.getPropsList().get(i))) {
        continue;
      }
      sanitizedStatement.getPaths().add(statement.getPaths().get(i));
      sanitizedStatement.getDataTypes().add(statement.getDataTypes().get(i));
      sanitizedStatement.getEncodings().add(statement.getEncodings().get(i));
      sanitizedStatement.getCompressors().add(statement.getCompressors().get(i));
      if (Objects.nonNull(statement.getPropsList())) {
        sanitizedStatement
            .getPropsList()
            .add(
                i < statement.getPropsList().size()
                    ? sanitizeRenameInternalProps(statement.getPropsList().get(i))
                    : null);
      }
      if (Objects.nonNull(statement.getAliasList())) {
        sanitizedStatement.getAliasList().add(getOrNull(statement.getAliasList(), i));
      }
      if (Objects.nonNull(statement.getTagsList())) {
        sanitizedStatement.getTagsList().add(getOrNull(statement.getTagsList(), i));
      }
      if (Objects.nonNull(statement.getAttributesList())) {
        sanitizedStatement.getAttributesList().add(getOrNull(statement.getAttributesList(), i));
      }
    }
    return sanitizedStatement.getPaths().isEmpty()
        ? Optional.empty()
        : Optional.of(sanitizedStatement);
  }

  private static int size(final List<?> list) {
    return Objects.isNull(list) ? 0 : list.size();
  }

  private static <T> T getOrNull(final List<T> list, final int index) {
    return index < list.size() ? list.get(index) : null;
  }

  private static boolean allMeasurementGroupsAreRenameInternal(
      final Collection<MeasurementGroup> measurementGroups) {
    return !measurementGroups.isEmpty()
        && measurementGroups.stream()
            .allMatch(PipeSchemaRegionPlanUtil::allMeasurementsAreRenameInternal);
  }

  private static boolean allMeasurementsAreRenameInternal(final MeasurementGroup measurementGroup) {
    final List<Map<String, String>> propsList = measurementGroup.getPropsList();
    return propsList != null
        && propsList.size() == measurementGroup.size()
        && allPropsAreRenameInternal(propsList);
  }

  private static boolean allPropsAreRenameInternal(final List<Map<String, String>> propsList) {
    return propsList != null
        && !propsList.isEmpty()
        && propsList.stream()
            .allMatch(PipeSchemaRegionPlanUtil::hasRealtimeNonTransferableRenameInternalProps);
  }

  private static boolean allPropsAreSnapshotNonTransferableRenameInternal(
      final List<Map<String, String>> propsList) {
    return propsList != null
        && !propsList.isEmpty()
        && propsList.stream()
            .allMatch(PipeSchemaRegionPlanUtil::hasNonTransferableRenameInternalProps);
  }

  private PipeSchemaRegionPlanUtil() {
    // Utility class
  }
}
