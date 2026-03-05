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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ChangePointTableScanNode combines DeviceTableScanNode with change-point detection. It pushes
 * change-point logic (last-of-run with next column) into the scan level so that TsFile statistics
 * can be leveraged to skip uniform segments.
 */
public class ChangePointTableScanNode extends DeviceTableScanNode {

  private Symbol measurementSymbol;
  private Symbol nextSymbol;

  public ChangePointTableScanNode(
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
      Symbol measurementSymbol,
      Symbol nextSymbol) {
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
    this.measurementSymbol = measurementSymbol;
    this.nextSymbol = nextSymbol;
  }

  protected ChangePointTableScanNode() {}

  public Symbol getMeasurementSymbol() {
    return measurementSymbol;
  }

  public Symbol getNextSymbol() {
    return nextSymbol;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder().addAll(super.getOutputSymbols()).add(nextSymbol).build();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitChangePointTableScan(this, context);
  }

  @Override
  public ChangePointTableScanNode clone() {
    return new ChangePointTableScanNode(
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
        measurementSymbol,
        nextSymbol);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CHANGE_POINT_TABLE_SCAN_NODE.serialize(byteBuffer);
    DeviceTableScanNode.serializeMemberVariables(this, byteBuffer, true);
    Symbol.serialize(measurementSymbol, byteBuffer);
    Symbol.serialize(nextSymbol, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CHANGE_POINT_TABLE_SCAN_NODE.serialize(stream);
    DeviceTableScanNode.serializeMemberVariables(this, stream, true);
    Symbol.serialize(measurementSymbol, stream);
    Symbol.serialize(nextSymbol, stream);
  }

  public static ChangePointTableScanNode deserialize(ByteBuffer byteBuffer) {
    ChangePointTableScanNode node = new ChangePointTableScanNode();
    DeviceTableScanNode.deserializeMemberVariables(byteBuffer, node, true);
    node.measurementSymbol = Symbol.deserialize(byteBuffer);
    node.nextSymbol = Symbol.deserialize(byteBuffer);
    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  /**
   * Factory method: merges a ChangePointNode with a DeviceTableScanNode into a single
   * ChangePointTableScanNode.
   */
  public static ChangePointTableScanNode combineChangePointAndTableScan(
      ChangePointNode changePointNode, DeviceTableScanNode scanNode) {
    return new ChangePointTableScanNode(
        changePointNode.getPlanNodeId(),
        scanNode.getQualifiedObjectName(),
        scanNode.getOutputSymbols(),
        scanNode.getAssignments(),
        scanNode.getDeviceEntries(),
        scanNode.getTagAndAttributeIndexMap(),
        scanNode.getScanOrder(),
        scanNode.getTimePredicate().orElse(null),
        scanNode.getPushDownPredicate(),
        scanNode.getPushDownLimit(),
        scanNode.getPushDownOffset(),
        scanNode.isPushLimitToEachDevice(),
        scanNode.containsNonAlignedDevice(),
        changePointNode.getMeasurementSymbol(),
        changePointNode.getNextSymbol());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ChangePointTableScanNode that = (ChangePointTableScanNode) o;
    return Objects.equals(measurementSymbol, that.measurementSymbol)
        && Objects.equals(nextSymbol, that.nextSymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), measurementSymbol, nextSymbol);
  }

  @Override
  public String toString() {
    return "ChangePointTableScan-" + this.getPlanNodeId();
  }
}
