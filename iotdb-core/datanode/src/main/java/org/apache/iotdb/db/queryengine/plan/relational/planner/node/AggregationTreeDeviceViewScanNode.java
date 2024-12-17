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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AggregationTreeDeviceViewScanNode extends AggregationTableScanNode {
  private final String treeDBName;
  private final Map<String, String> measurementColumnNameMap;

  public AggregationTreeDeviceViewScanNode(
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
      boolean containsNonAlignedDevice,
      Assignments projection,
      Map<Symbol, AggregationNode.Aggregation> aggregations,
      AggregationNode.GroupingSetDescriptor groupingSets,
      List<Symbol> preGroupedSymbols,
      AggregationNode.Step step,
      Optional<Symbol> groupIdSymbol,
      String treeDBName,
      Map<String, String> measurementColumnNameMap) {
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
        pushLimitToEachDevice,
        containsNonAlignedDevice,
        projection,
        aggregations,
        groupingSets,
        preGroupedSymbols,
        step,
        groupIdSymbol);
    this.treeDBName = treeDBName;
    this.measurementColumnNameMap = measurementColumnNameMap;
  }

  public String getTreeDBName() {
    return treeDBName;
  }

  public Map<String, String> getMeasurementColumnNameMap() {
    return measurementColumnNameMap;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregationTreeDeviceViewScanNode(this, context);
  }

  @Override
  public AggregationTreeDeviceViewScanNode clone() {
    return new AggregationTreeDeviceViewScanNode(
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
        containsNonAlignedDevice,
        projection,
        aggregations,
        groupingSets,
        preGroupedSymbols,
        step,
        groupIdSymbol,
        treeDBName,
        measurementColumnNameMap);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE.serialize(byteBuffer);

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

    ReadWriteIOUtils.write(containsNonAlignedDevice, byteBuffer);

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

    ReadWriteIOUtils.write(treeDBName, byteBuffer);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), byteBuffer);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE.serialize(stream);
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

    ReadWriteIOUtils.write(containsNonAlignedDevice, stream);

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

    ReadWriteIOUtils.write(treeDBName, stream);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), stream);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
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

    boolean containsNonAlignedDevice = ReadWriteIOUtils.readBool(byteBuffer);

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

    String treeDBName = ReadWriteIOUtils.readString(byteBuffer);
    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, String> measurementColumnNameMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      measurementColumnNameMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    return new AggregationTreeDeviceViewScanNode(
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
        containsNonAlignedDevice,
        projection.build(),
        aggregations,
        groupingSetDescriptor,
        preGroupedSymbols,
        step,
        groupIdSymbol,
        treeDBName,
        measurementColumnNameMap);
  }
}
