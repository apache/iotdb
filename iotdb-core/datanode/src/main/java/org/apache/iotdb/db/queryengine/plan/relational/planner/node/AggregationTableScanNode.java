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
package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationTableScanNode extends TableScanNode {
  // if there is date_bin function of time, we should use this field to transform time input
  private final Assignments projection;

  private final Map<Symbol, AggregationNode.Aggregation> aggregations;
  private final AggregationNode.GroupingSetDescriptor groupingSets;
  private final List<Symbol> preGroupedSymbols;
  private AggregationNode.Step step;
  private final Optional<Symbol> groupIdSymbol;

  public AggregationTableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> idAndAttributeIndexMap,
      Ordering scanOrder,
      Expression timePredicate,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice,
      Assignments projection,
      Map<Symbol, AggregationNode.Aggregation> aggregations,
      AggregationNode.GroupingSetDescriptor groupingSets,
      List<Symbol> preGroupedSymbols,
      AggregationNode.Step step,
      Optional<Symbol> groupIdSymbol) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice);
    this.projection = projection;

    this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
    aggregations.values().forEach(aggregation -> aggregation.verifyArguments(step));

    requireNonNull(groupingSets, "groupingSets is null");
    groupIdSymbol.ifPresent(
        symbol ->
            checkArgument(
                groupingSets.getGroupingKeys().contains(symbol),
                "Grouping columns does not contain groupId column"));
    this.groupingSets = groupingSets;

    this.groupIdSymbol = requireNonNull(groupIdSymbol);

    boolean noOrderBy =
        aggregations.values().stream()
            .map(AggregationNode.Aggregation::getOrderingScheme)
            .noneMatch(Optional::isPresent);
    checkArgument(
        noOrderBy || step == AggregationNode.Step.SINGLE,
        "ORDER BY does not support distributed aggregation");

    this.step = step;

    requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
    checkArgument(
        preGroupedSymbols.isEmpty()
            || groupingSets.getGroupingKeys().containsAll(preGroupedSymbols),
        "Pre-grouped symbols must be a subset of the grouping keys");
    this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    outputs.addAll(groupingSets.getGroupingKeys());
    outputs.addAll(aggregations.keySet());

    this.setOutputSymbols(outputs.build());
  }

  public Assignments getProjection() {
    return projection;
  }

  public List<Symbol> getGroupingKeys() {
    return groupingSets.getGroupingKeys();
  }

  public AggregationNode.GroupingSetDescriptor getGroupingSets() {
    return groupingSets;
  }

  public Map<Symbol, AggregationNode.Aggregation> getAggregations() {
    return aggregations;
  }

  public List<Symbol> getPreGroupedSymbols() {
    return preGroupedSymbols;
  }

  public boolean isStreamable() {
    return !preGroupedSymbols.isEmpty()
        && groupingSets.getGroupingSetCount() == 1
        && groupingSets.getGlobalGroupingSets().isEmpty();
  }

  public AggregationNode.Step getStep() {
    return step;
  }

  public void setStep(AggregationNode.Step step) {
    this.step = step;
  }

  public int getGroupingSetCount() {
    return groupingSets.getGroupingSetCount();
  }

  public Set<Integer> getGlobalGroupingSets() {
    return groupingSets.getGlobalGroupingSets();
  }

  public Optional<Symbol> getGroupIdSymbol() {
    return groupIdSymbol;
  }

  @Override
  public AggregationTableScanNode clone() {
    return new AggregationTableScanNode(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        projection,
        aggregations,
        groupingSets,
        preGroupedSymbols,
        step,
        groupIdSymbol);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregationTableScan(this, context);
  }

  public static AggregationTableScanNode combineAggregationAndTableScan(
      PlanNodeId id,
      AggregationNode aggregationNode,
      ProjectNode projectNode,
      TableScanNode tableScanNode) {
    return new AggregationTableScanNode(
        id,
        tableScanNode.getQualifiedObjectName(),
        tableScanNode.getOutputSymbols(),
        tableScanNode.getAssignments(),
        tableScanNode.getDeviceEntries(),
        tableScanNode.getIdAndAttributeIndexMap(),
        tableScanNode.getScanOrder(),
        tableScanNode.getTimePredicate().orElse(null),
        tableScanNode.getPushDownPredicate(),
        tableScanNode.getPushDownLimit(),
        tableScanNode.getPushDownOffset(),
        tableScanNode.isPushLimitToEachDevice(),
        projectNode == null ? null : projectNode.getAssignments(),
        aggregationNode.getAggregations(),
        aggregationNode.getGroupingSets(),
        aggregationNode.getPreGroupedSymbols(),
        aggregationNode.getStep(),
        aggregationNode.getGroupIdSymbol());
  }

  public static AggregationTableScanNode combineAggregationAndTableScan(
      PlanNodeId id,
      AggregationNode aggregationNode,
      ProjectNode projectNode,
      TableScanNode tableScanNode,
      AggregationNode.Step step) {
    return new AggregationTableScanNode(
        id,
        tableScanNode.getQualifiedObjectName(),
        tableScanNode.getOutputSymbols(),
        tableScanNode.getAssignments(),
        tableScanNode.getDeviceEntries(),
        tableScanNode.getIdAndAttributeIndexMap(),
        tableScanNode.getScanOrder(),
        tableScanNode.getTimePredicate().orElse(null),
        tableScanNode.getPushDownPredicate(),
        tableScanNode.getPushDownLimit(),
        tableScanNode.getPushDownOffset(),
        tableScanNode.isPushLimitToEachDevice(),
        projectNode == null ? null : projectNode.getAssignments(),
        aggregationNode.getAggregations(),
        aggregationNode.getGroupingSets(),
        aggregationNode.getPreGroupedSymbols(),
        step,
        aggregationNode.getGroupIdSymbol());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), projection, aggregations, groupingSets, step, groupIdSymbol);
  }

  @Override
  public String toString() {
    return "AggregationTableScanNode-" + this.getPlanNodeId();
  }
}
