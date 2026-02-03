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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.LAST_BY;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator.DATE_BIN_PREFIX;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.iotdb.db.utils.constant.SqlConstant.TABLE_TIME_COLUMN_NAME;

public class AggregationTableScanNode extends DeviceTableScanNode {
  // if there is date_bin function of time, we should use this field to transform time input
  protected Assignments projection;

  protected Map<Symbol, AggregationNode.Aggregation> aggregations;
  protected AggregationNode.GroupingSetDescriptor groupingSets;
  protected List<Symbol> preGroupedSymbols;
  protected AggregationNode.Step step;
  protected Optional<Symbol> groupIdSymbol;

  private Map<DeviceEntry, Integer> deviceCountMap;

  public AggregationTableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> tagAndAttributeIndexMap,
      Ordering scanOrder,
      Expression timePredicate,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice,
      boolean containsNonAlignedDevice,
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
        tagAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        containsNonAlignedDevice);
    this.projection = projection;

    this.aggregations = transformCountStar(aggregations, assignments);
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

    List<Symbol> groupingKeys = groupingSets.getGroupingKeys();
    for (int i = 0; i < groupingKeys.size(); i++) {
      if (groupingKeys.get(i).getName().startsWith(DATE_BIN_PREFIX)) {
        checkArgument(
            i == groupingKeys.size() - 1, "date_bin function must be the last GroupingKey");
      }
    }
    requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
    checkArgument(
        preGroupedSymbols.isEmpty() || groupingKeys.containsAll(preGroupedSymbols),
        "Pre-grouped symbols must be a subset of the grouping keys");
    this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

    this.setOutputSymbols(constructOutputSymbols(groupingSets, aggregations));
  }

  protected AggregationTableScanNode() {}

  private static List<Symbol> constructOutputSymbols(
      AggregationNode.GroupingSetDescriptor groupingSets,
      Map<Symbol, AggregationNode.Aggregation> aggregations) {
    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    outputs.addAll(groupingSets.getGroupingKeys());
    outputs.addAll(aggregations.keySet());
    return outputs.build();
  }

  // transform count() to count(time)
  private static Map<Symbol, AggregationNode.Aggregation> transformCountStar(
      Map<Symbol, AggregationNode.Aggregation> aggregations,
      Map<Symbol, ColumnSchema> assignments) {
    ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> resultBuilder =
        ImmutableMap.builder();

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
      Symbol symbol = entry.getKey();
      AggregationNode.Aggregation aggregation = entry.getValue();
      if (aggregation.getArguments().isEmpty()) {
        AggregationNode.Aggregation countStarAggregation;
        Optional<Symbol> timeSymbol = getTimeColumn(assignments);
        if (!timeSymbol.isPresent()) {
          assignments.put(
              Symbol.of(TABLE_TIME_COLUMN_NAME),
              new ColumnSchema(
                  TABLE_TIME_COLUMN_NAME,
                  TimestampType.TIMESTAMP,
                  false,
                  TsTableColumnCategory.TIME));
          countStarAggregation = getCountStarAggregation(aggregation, TABLE_TIME_COLUMN_NAME);
        } else {
          countStarAggregation = getCountStarAggregation(aggregation, timeSymbol.get().getName());
        }
        resultBuilder.put(symbol, countStarAggregation);
      } else {
        resultBuilder.put(symbol, aggregation);
      }
    }

    return resultBuilder.build();
  }

  private static AggregationNode.Aggregation getCountStarAggregation(
      AggregationNode.Aggregation aggregation, String timeSymbolName) {
    ResolvedFunction resolvedFunction = aggregation.getResolvedFunction();
    ResolvedFunction countStarFunction =
        new ResolvedFunction(
            new BoundSignature(
                COUNT, LongType.INT64, Collections.singletonList(TimestampType.TIMESTAMP)),
            resolvedFunction.getFunctionId(),
            resolvedFunction.getFunctionKind(),
            resolvedFunction.isDeterministic(),
            resolvedFunction.getFunctionNullability());
    return new AggregationNode.Aggregation(
        countStarFunction,
        Collections.singletonList(new SymbolReference(timeSymbolName)),
        aggregation.isDistinct(),
        aggregation.getFilter(),
        aggregation.getOrderingScheme(),
        aggregation.getMask());
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
    return true;
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
        tagAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        containsNonAlignedDevice,
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
      DeviceTableScanNode tableScanNode) {
    if (tableScanNode instanceof TreeDeviceViewScanNode) {
      TreeDeviceViewScanNode treeDeviceViewScanNode = (TreeDeviceViewScanNode) tableScanNode;
      return new AggregationTreeDeviceViewScanNode(
          id,
          tableScanNode.getQualifiedObjectName(),
          tableScanNode.getOutputSymbols(),
          tableScanNode.getAssignments(),
          tableScanNode.getDeviceEntries(),
          tableScanNode.getTagAndAttributeIndexMap(),
          tableScanNode.getScanOrder(),
          tableScanNode.getTimePredicate().orElse(null),
          tableScanNode.getPushDownPredicate(),
          tableScanNode.getPushDownLimit(),
          tableScanNode.getPushDownOffset(),
          tableScanNode.isPushLimitToEachDevice(),
          tableScanNode.containsNonAlignedDevice(),
          projectNode == null ? null : projectNode.getAssignments(),
          aggregationNode.getAggregations(),
          aggregationNode.getGroupingSets(),
          aggregationNode.getPreGroupedSymbols(),
          aggregationNode.getStep(),
          aggregationNode.getGroupIdSymbol(),
          treeDeviceViewScanNode.getTreeDBName(),
          treeDeviceViewScanNode.getMeasurementColumnNameMap());
    }

    return new AggregationTableScanNode(
        id,
        tableScanNode.getQualifiedObjectName(),
        tableScanNode.getOutputSymbols(),
        tableScanNode.getAssignments(),
        tableScanNode.getDeviceEntries(),
        tableScanNode.getTagAndAttributeIndexMap(),
        tableScanNode.getScanOrder(),
        tableScanNode.getTimePredicate().orElse(null),
        tableScanNode.getPushDownPredicate(),
        tableScanNode.getPushDownLimit(),
        tableScanNode.getPushDownOffset(),
        tableScanNode.isPushLimitToEachDevice(),
        tableScanNode.containsNonAlignedDevice(),
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
      DeviceTableScanNode tableScanNode,
      AggregationNode.Step step) {
    if (tableScanNode instanceof TreeDeviceViewScanNode) {
      TreeDeviceViewScanNode treeDeviceViewScanNode = (TreeDeviceViewScanNode) tableScanNode;
      return new AggregationTreeDeviceViewScanNode(
          id,
          tableScanNode.getQualifiedObjectName(),
          tableScanNode.getOutputSymbols(),
          tableScanNode.getAssignments(),
          tableScanNode.getDeviceEntries(),
          tableScanNode.getTagAndAttributeIndexMap(),
          tableScanNode.getScanOrder(),
          tableScanNode.getTimePredicate().orElse(null),
          tableScanNode.getPushDownPredicate(),
          tableScanNode.getPushDownLimit(),
          tableScanNode.getPushDownOffset(),
          tableScanNode.isPushLimitToEachDevice(),
          tableScanNode.containsNonAlignedDevice(),
          projectNode == null ? null : projectNode.getAssignments(),
          aggregationNode.getAggregations(),
          aggregationNode.getGroupingSets(),
          aggregationNode.getPreGroupedSymbols(),
          step,
          aggregationNode.getGroupIdSymbol(),
          treeDeviceViewScanNode.getTreeDBName(),
          treeDeviceViewScanNode.getMeasurementColumnNameMap());
    }

    return new AggregationTableScanNode(
        id,
        tableScanNode.getQualifiedObjectName(),
        tableScanNode.getOutputSymbols(),
        tableScanNode.getAssignments(),
        tableScanNode.getDeviceEntries(),
        tableScanNode.getTagAndAttributeIndexMap(),
        tableScanNode.getScanOrder(),
        tableScanNode.getTimePredicate().orElse(null),
        tableScanNode.getPushDownPredicate(),
        tableScanNode.getPushDownLimit(),
        tableScanNode.getPushDownOffset(),
        tableScanNode.isPushLimitToEachDevice(),
        tableScanNode.containsNonAlignedDevice(),
        projectNode == null ? null : projectNode.getAssignments(),
        aggregationNode.getAggregations(),
        aggregationNode.getGroupingSets(),
        aggregationNode.getPreGroupedSymbols(),
        step,
        aggregationNode.getGroupIdSymbol());
  }

  public boolean mayUseLastCache() {
    // Only made a simple judgment is here, if Aggregations is not empty and all of them are LAST or
    // LAST_BY
    if (aggregations.isEmpty()) {
      return false;
    }

    for (AggregationNode.Aggregation aggregation : aggregations.values()) {
      String functionName = aggregation.getResolvedFunction().getSignature().getName();
      if (!LAST_BY.getFunctionName().equals(functionName)
          && !LAST.getFunctionName().equals(functionName)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AggregationTableScanNode that = (AggregationTableScanNode) o;
    return Objects.equals(qualifiedObjectName, that.qualifiedObjectName)
        && Objects.equals(outputSymbols, that.outputSymbols)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet)
        && Objects.equals(projection, that.projection)
        && Objects.equals(aggregations, that.aggregations)
        && Objects.equals(groupingSets, that.groupingSets)
        && Objects.equals(step, that.step)
        && Objects.equals(groupIdSymbol, that.groupIdSymbol);
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

  protected static void serializeMemberVariables(
      AggregationTableScanNode node, ByteBuffer byteBuffer) {
    DeviceTableScanNode.serializeMemberVariables(node, byteBuffer, false);

    if (node.projection != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(node.projection.getMap().size(), byteBuffer);
      for (Map.Entry<Symbol, Expression> entry : node.projection.getMap().entrySet()) {
        Symbol.serialize(entry.getKey(), byteBuffer);
        Expression.serialize(entry.getValue(), byteBuffer);
      }
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(node.aggregations.size(), byteBuffer);
    for (Map.Entry<Symbol, AggregationNode.Aggregation> aggregation :
        node.aggregations.entrySet()) {
      Symbol.serialize(aggregation.getKey(), byteBuffer);
      aggregation.getValue().serialize(byteBuffer);
    }

    node.groupingSets.serialize(byteBuffer);

    ReadWriteIOUtils.write(node.preGroupedSymbols.size(), byteBuffer);
    for (Symbol preGroupedSymbol : node.preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, byteBuffer);
    }

    node.step.serialize(byteBuffer);

    ReadWriteIOUtils.write(node.groupIdSymbol.isPresent(), byteBuffer);
    if (node.groupIdSymbol.isPresent()) {
      Symbol.serialize(node.groupIdSymbol.get(), byteBuffer);
    }

    if (node.deviceCountMap != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(node.deviceCountMap.size(), byteBuffer);
      for (Map.Entry<DeviceEntry, Integer> entry : node.deviceCountMap.entrySet()) {
        entry.getKey().serialize(byteBuffer);
        ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
      }
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
  }

  protected static void serializeMemberVariables(
      AggregationTableScanNode node, DataOutputStream stream) throws IOException {
    DeviceTableScanNode.serializeMemberVariables(node, stream, false);

    if (node.projection != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(node.projection.getMap().size(), stream);
      for (Map.Entry<Symbol, Expression> entry : node.projection.getMap().entrySet()) {
        Symbol.serialize(entry.getKey(), stream);
        Expression.serialize(entry.getValue(), stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(node.aggregations.size(), stream);
    for (Map.Entry<Symbol, AggregationNode.Aggregation> aggregation :
        node.aggregations.entrySet()) {
      Symbol.serialize(aggregation.getKey(), stream);
      aggregation.getValue().serialize(stream);
    }

    node.groupingSets.serialize(stream);

    ReadWriteIOUtils.write(node.preGroupedSymbols.size(), stream);
    for (Symbol preGroupedSymbol : node.preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, stream);
    }

    node.step.serialize(stream);

    ReadWriteIOUtils.write(node.groupIdSymbol.isPresent(), stream);
    if (node.groupIdSymbol.isPresent()) {
      Symbol.serialize(node.groupIdSymbol.get(), stream);
    }

    if (node.deviceCountMap != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(node.deviceCountMap.size(), stream);
      for (Map.Entry<DeviceEntry, Integer> entry : node.deviceCountMap.entrySet()) {
        entry.getKey().serialize(stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  protected static void deserializeMemberVariables(
      ByteBuffer byteBuffer, AggregationTableScanNode node) {
    DeviceTableScanNode.deserializeMemberVariables(byteBuffer, node, false);

    int size;

    Assignments.Builder projection = Assignments.builder();
    boolean hasProjection = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasProjection) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      while (size-- > 0) {
        projection.put(Symbol.deserialize(byteBuffer), Expression.deserialize(byteBuffer));
      }
    }
    node.projection = projection.build();

    size = ReadWriteIOUtils.readInt(byteBuffer);
    final Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(size);
    while (size-- > 0) {
      aggregations.put(
          Symbol.deserialize(byteBuffer), AggregationNode.Aggregation.deserialize(byteBuffer));
    }

    node.aggregations = transformCountStar(aggregations, node.getAssignments());

    node.groupingSets = AggregationNode.GroupingSetDescriptor.deserialize(byteBuffer);

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> preGroupedSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      preGroupedSymbols.add(Symbol.deserialize(byteBuffer));
    }
    node.preGroupedSymbols = preGroupedSymbols;

    node.step = AggregationNode.Step.deserialize(byteBuffer);

    Optional<Symbol> groupIdSymbol = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      groupIdSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    }
    node.groupIdSymbol = groupIdSymbol;

    node.outputSymbols = constructOutputSymbols(node.getGroupingSets(), node.getAggregations());

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      Map<DeviceEntry, Integer> deviceRegionCountMap = new HashMap<>(size);
      while (size-- > 0) {
        DeviceEntry deviceEntry = DeviceEntry.deserialize(byteBuffer);
        deviceRegionCountMap.put(deviceEntry, ReadWriteIOUtils.readInt(byteBuffer));
      }
      node.setDeviceCountMap(deviceRegionCountMap);
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_AGGREGATION_TABLE_SCAN_NODE.serialize(byteBuffer);

    AggregationTableScanNode.serializeMemberVariables(this, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_AGGREGATION_TABLE_SCAN_NODE.serialize(stream);

    AggregationTableScanNode.serializeMemberVariables(this, stream);
  }

  public static AggregationTableScanNode deserialize(ByteBuffer byteBuffer) {
    AggregationTableScanNode node = new AggregationTableScanNode();
    AggregationTableScanNode.deserializeMemberVariables(byteBuffer, node);

    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  public void setDeviceCountMap(Map<DeviceEntry, Integer> deviceCountMap) {
    this.deviceCountMap = deviceCountMap;
  }

  public Map<DeviceEntry, Integer> getDeviceCountMap() {
    return deviceCountMap;
  }

  @Override
  public void setPushDownLimit(long pushDownLimit) {
    throw new IllegalStateException("Should never push down limit to AggregationTableScanNode.");
  }

  @Override
  public void setPushDownOffset(long pushDownOffset) {
    throw new IllegalStateException("Should never push down offset to AggregationTableScanNode.");
  }
}
