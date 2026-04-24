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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AlignedAggregationTreeDeviceViewScanNode extends AggregationTreeDeviceViewScanNode {

  public AlignedAggregationTreeDeviceViewScanNode(
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
        groupIdSymbol,
        treeDBName,
        measurementColumnNameMap);
  }

  private AlignedAggregationTreeDeviceViewScanNode() {}

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitAlignedAggregationTreeDeviceViewScan(this, context);
  }

  @Override
  public AlignedAggregationTreeDeviceViewScanNode clone() {
    return new AlignedAggregationTreeDeviceViewScanNode(
        getPlanNodeId(),
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
        getTreeDBName(),
        getMeasurementColumnNameMap());
  }

  protected PlanNodeType getPlanNodeType() {
    return PlanNodeType.ALIGNED_AGGREGATION_TREE_DEVICE_VIEW_SCAN_NODE;
  }

  public static AggregationTreeDeviceViewScanNode deserialize(ByteBuffer byteBuffer) {
    return AggregationTreeDeviceViewScanNode.deserialize(
        byteBuffer, new AlignedAggregationTreeDeviceViewScanNode());
  }

  @Override
  public String toString() {
    return "AlignedAggregationTreeDeviceViewScanNode-" + this.getPlanNodeId();
  }
}
