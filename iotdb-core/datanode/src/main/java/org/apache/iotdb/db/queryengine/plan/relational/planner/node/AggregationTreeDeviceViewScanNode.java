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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AggregationTreeDeviceViewScanNode extends AggregationTableScanNode {
  private String treeDBName;
  private Map<String, String> measurementColumnNameMap;

  public AggregationTreeDeviceViewScanNode(
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
      Optional<Symbol> groupIdSymbol,
      String treeDBName,
      Map<String, String> measurementColumnNameMap) {
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

  protected AggregationTreeDeviceViewScanNode() {}

  public String getTreeDBName() {
    return treeDBName;
  }

  public Map<String, String> getMeasurementColumnNameMap() {
    return measurementColumnNameMap;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitAggregationTreeDeviceViewScan(this, context);
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
    AggregationTreeDeviceViewScanNode that = (AggregationTreeDeviceViewScanNode) o;
    return Objects.equals(treeDBName, that.treeDBName)
        && Objects.equals(measurementColumnNameMap, that.measurementColumnNameMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), treeDBName, measurementColumnNameMap);
  }

  @Override
  public String toString() {
    return "AggregationTreeDeviceViewTableScanNode-" + this.getPlanNodeId();
  }

  @Override
  public AggregationTreeDeviceViewScanNode clone() {
    return new AggregationTreeDeviceViewScanNode(
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
        groupIdSymbol,
        treeDBName,
        measurementColumnNameMap);
  }

  protected PlanNodeType getPlanNodeType() {
    // This node is not supported to serde
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getPlanNodeType().serialize(byteBuffer);

    AggregationTableScanNode.serializeMemberVariables(this, byteBuffer);

    ReadWriteIOUtils.write(treeDBName, byteBuffer);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), byteBuffer);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getPlanNodeType().serialize(stream);

    AggregationTableScanNode.serializeMemberVariables(this, stream);

    ReadWriteIOUtils.write(treeDBName, stream);
    ReadWriteIOUtils.write(measurementColumnNameMap.size(), stream);
    for (Map.Entry<String, String> entry : measurementColumnNameMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  protected static AggregationTreeDeviceViewScanNode deserialize(
      ByteBuffer byteBuffer, AggregationTreeDeviceViewScanNode node) {
    AggregationTableScanNode.deserializeMemberVariables(byteBuffer, node);

    node.treeDBName = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, String> measurementColumnNameMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      measurementColumnNameMap.put(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    node.measurementColumnNameMap = measurementColumnNameMap;

    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }
}
