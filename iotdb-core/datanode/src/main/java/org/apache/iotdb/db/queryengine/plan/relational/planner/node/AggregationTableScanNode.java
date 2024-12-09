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
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.iotdb.db.utils.constant.SqlConstant.TABLE_TIME_COLUMN_NAME;

public class AggregationTableScanNode extends DeviceTableScanNode {
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

    this.aggregations = new LinkedHashMap<>(aggregations.size());
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
      Symbol symbol = entry.getKey();
      AggregationNode.Aggregation aggregation = entry.getValue();
      if (aggregation.getArguments().isEmpty()
          && COUNT.equals(aggregation.getResolvedFunction().getSignature().getName())) {
        AggregationNode.Aggregation countStarAggregation = getCountStarAggregation(aggregation);
        if (!assignments.containsKey(Symbol.of(TABLE_TIME_COLUMN_NAME))) {
          assignments.put(
              Symbol.of(TABLE_TIME_COLUMN_NAME),
              new ColumnSchema(
                  TABLE_TIME_COLUMN_NAME,
                  TimestampType.TIMESTAMP,
                  false,
                  TsTableColumnCategory.TIME));
        }
        this.aggregations.put(symbol, countStarAggregation);
      } else {
        this.aggregations.put(symbol, aggregation);
      }
    }
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

  private AggregationNode.Aggregation getCountStarAggregation(
      AggregationNode.Aggregation aggregation) {
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
        Collections.singletonList(new SymbolReference(TABLE_TIME_COLUMN_NAME)),
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
      DeviceTableScanNode tableScanNode) {
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
      DeviceTableScanNode tableScanNode,
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

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_AGGREGATION_TABLE_SCAN_NODE.serialize(byteBuffer);

    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), byteBuffer);

    ReadWriteIOUtils.write(assignments.size(), byteBuffer);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ColumnSchema.serialize(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), byteBuffer);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(byteBuffer);
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), byteBuffer);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(timePredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(pushDownLimit, byteBuffer);
    ReadWriteIOUtils.write(pushDownOffset, byteBuffer);
    ReadWriteIOUtils.write(pushLimitToEachDevice, byteBuffer);

    if (projection != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(projection.getMap().size(), byteBuffer);
      for (Map.Entry<Symbol, Expression> entry : projection.getMap().entrySet()) {
        Symbol.serialize(entry.getKey(), byteBuffer);
        Expression.serialize(entry.getValue(), byteBuffer);
      }
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    ReadWriteIOUtils.write(aggregations.size(), byteBuffer);
    for (Map.Entry<Symbol, AggregationNode.Aggregation> aggregation : aggregations.entrySet()) {
      Symbol.serialize(aggregation.getKey(), byteBuffer);
      aggregation.getValue().serialize(byteBuffer);
    }

    groupingSets.serialize(byteBuffer);

    ReadWriteIOUtils.write(preGroupedSymbols.size(), byteBuffer);
    for (Symbol preGroupedSymbol : preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, byteBuffer);
    }

    step.serialize(byteBuffer);

    ReadWriteIOUtils.write(groupIdSymbol.isPresent(), byteBuffer);
    if (groupIdSymbol.isPresent()) {
      Symbol.serialize(groupIdSymbol.get(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_AGGREGATION_TABLE_SCAN_NODE.serialize(stream);
    if (qualifiedObjectName.getDatabaseName() != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(qualifiedObjectName.getDatabaseName(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    ReadWriteIOUtils.write(qualifiedObjectName.getObjectName(), stream);

    ReadWriteIOUtils.write(assignments.size(), stream);
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ColumnSchema.serialize(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(deviceEntries.size(), stream);
    for (DeviceEntry entry : deviceEntries) {
      entry.serialize(stream);
    }

    ReadWriteIOUtils.write(idAndAttributeIndexMap.size(), stream);
    for (Map.Entry<Symbol, Integer> entry : idAndAttributeIndexMap.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }

    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);

    if (timePredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(timePredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (pushDownPredicate != null) {
      ReadWriteIOUtils.write(true, stream);
      Expression.serialize(pushDownPredicate, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(pushDownLimit, stream);
    ReadWriteIOUtils.write(pushDownOffset, stream);
    ReadWriteIOUtils.write(pushLimitToEachDevice, stream);

    if (projection != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(projection.getMap().size(), stream);
      for (Map.Entry<Symbol, Expression> entry : projection.getMap().entrySet()) {
        Symbol.serialize(entry.getKey(), stream);
        Expression.serialize(entry.getValue(), stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    ReadWriteIOUtils.write(aggregations.size(), stream);
    for (Map.Entry<Symbol, AggregationNode.Aggregation> aggregation : aggregations.entrySet()) {
      Symbol.serialize(aggregation.getKey(), stream);
      aggregation.getValue().serialize(stream);
    }

    groupingSets.serialize(stream);

    ReadWriteIOUtils.write(preGroupedSymbols.size(), stream);
    for (Symbol preGroupedSymbol : preGroupedSymbols) {
      Symbol.serialize(preGroupedSymbol, stream);
    }

    step.serialize(stream);

    ReadWriteIOUtils.write(groupIdSymbol.isPresent(), stream);
    if (groupIdSymbol.isPresent()) {
      Symbol.serialize(groupIdSymbol.get(), stream);
    }
  }

  public static AggregationTableScanNode deserialize(ByteBuffer byteBuffer) {
    boolean hasDatabaseName = ReadWriteIOUtils.readBool(byteBuffer);
    String databaseName = null;
    if (hasDatabaseName) {
      databaseName = ReadWriteIOUtils.readString(byteBuffer);
    }
    String tableName = ReadWriteIOUtils.readString(byteBuffer);
    QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(databaseName, tableName);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      assignments.put(Symbol.deserialize(byteBuffer), ColumnSchema.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DeviceEntry> deviceEntries = new ArrayList<>(size);
    while (size-- > 0) {
      deviceEntries.add(DeviceEntry.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>(size);
    while (size-- > 0) {
      idAndAttributeIndexMap.put(
          Symbol.deserialize(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    Expression timePredicate = null;
    boolean hasTimePredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasTimePredicate) {
      timePredicate = Expression.deserialize(byteBuffer);
    }

    Expression pushDownPredicate = null;
    boolean hasPushDownPredicate = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasPushDownPredicate) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }

    long pushDownLimit = ReadWriteIOUtils.readLong(byteBuffer);
    long pushDownOffset = ReadWriteIOUtils.readLong(byteBuffer);
    boolean pushLimitToEachDevice = ReadWriteIOUtils.readBool(byteBuffer);

    Assignments.Builder projection = Assignments.builder();
    boolean hasProjection = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasProjection) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      while (size-- > 0) {
        projection.put(Symbol.deserialize(byteBuffer), Expression.deserialize(byteBuffer));
      }
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    final Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(size);
    while (size-- > 0) {
      aggregations.put(
          Symbol.deserialize(byteBuffer), AggregationNode.Aggregation.deserialize(byteBuffer));
    }

    AggregationNode.GroupingSetDescriptor groupingSetDescriptor =
        AggregationNode.GroupingSetDescriptor.deserialize(byteBuffer);

    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> preGroupedSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      preGroupedSymbols.add(Symbol.deserialize(byteBuffer));
    }

    AggregationNode.Step step = AggregationNode.Step.deserialize(byteBuffer);

    Optional<Symbol> groupIdSymbol = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      groupIdSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new AggregationTableScanNode(
        planNodeId,
        qualifiedObjectName,
        null,
        assignments,
        deviceEntries,
        idAndAttributeIndexMap,
        scanOrder,
        timePredicate,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        pushLimitToEachDevice,
        projection.build(),
        aggregations,
        groupingSetDescriptor,
        preGroupedSymbols,
        step,
        groupIdSymbol);
  }
}
