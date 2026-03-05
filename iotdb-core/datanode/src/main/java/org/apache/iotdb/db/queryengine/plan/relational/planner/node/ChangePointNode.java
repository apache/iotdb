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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * ChangePointNode detects value changes in a monitored column within sorted, partitioned data. It
 * emits the last row of each consecutive run of identical values (plus the final row), replacing a
 * Filter(Window(LEAD(...))) pattern.
 */
public class ChangePointNode extends SingleChildProcessNode {

  private final Symbol measurementSymbol;
  private final Symbol nextSymbol;

  public ChangePointNode(
      PlanNodeId id, PlanNode child, Symbol measurementSymbol, Symbol nextSymbol) {
    super(id, child);
    this.measurementSymbol = measurementSymbol;
    this.nextSymbol = nextSymbol;
  }

  public ChangePointNode(PlanNodeId id, Symbol measurementSymbol, Symbol nextSymbol) {
    super(id);
    this.measurementSymbol = measurementSymbol;
    this.nextSymbol = nextSymbol;
  }

  public Symbol getMeasurementSymbol() {
    return measurementSymbol;
  }

  public Symbol getNextSymbol() {
    return nextSymbol;
  }

  @Override
  public PlanNode clone() {
    return new ChangePointNode(getPlanNodeId(), measurementSymbol, nextSymbol);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitChangePoint(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(getChild().getOutputSymbols())
        .add(nextSymbol)
        .build();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 1, "wrong number of new children");
    return new ChangePointNode(id, newChildren.get(0), measurementSymbol, nextSymbol);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_CHANGE_POINT_NODE.serialize(byteBuffer);
    Symbol.serialize(measurementSymbol, byteBuffer);
    Symbol.serialize(nextSymbol, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_CHANGE_POINT_NODE.serialize(stream);
    Symbol.serialize(measurementSymbol, stream);
    Symbol.serialize(nextSymbol, stream);
  }

  public static ChangePointNode deserialize(ByteBuffer buffer) {
    Symbol measurementSymbol = Symbol.deserialize(buffer);
    Symbol nextSymbol = Symbol.deserialize(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ChangePointNode(planNodeId, measurementSymbol, nextSymbol);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ChangePointNode that = (ChangePointNode) o;
    return Objects.equals(measurementSymbol, that.measurementSymbol)
        && Objects.equals(nextSymbol, that.nextSymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), measurementSymbol, nextSymbol);
  }

  @Override
  public String toString() {
    return "ChangePoint-" + this.getPlanNodeId();
  }
}
