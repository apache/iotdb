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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.base.Objects;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MergeSortNode extends MultiChildProcessNode {
  private final OrderingScheme orderingScheme;

  private final List<Symbol> outputSymbols;

  public MergeSortNode(PlanNodeId id, OrderingScheme orderingScheme, List<Symbol> outputSymbols) {
    super(id);
    this.orderingScheme = orderingScheme;
    this.outputSymbols = outputSymbols;
  }

  public MergeSortNode(
      PlanNodeId id,
      List<PlanNode> children,
      OrderingScheme orderingScheme,
      List<Symbol> outputSymbols) {
    super(id, children);
    this.orderingScheme = orderingScheme;
    this.outputSymbols = outputSymbols;
  }

  @Override
  public PlanNode clone() {
    return new MergeSortNode(getPlanNodeId(), orderingScheme, outputSymbols);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitMergeSort(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_MERGESORT_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    for (Symbol symbol : outputSymbols) {
      Symbol.serialize(symbol, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_MERGESORT_NODE.serialize(stream);
    orderingScheme.serialize(stream);
    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      Symbol.serialize(symbol, stream);
    }
  }

  public static MergeSortNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      outputSymbols.add(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new MergeSortNode(planNodeId, orderingScheme, outputSymbols);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(children.size() == newChildren.size(), "wrong number of new children");
    return new MergeSortNode(id, newChildren, orderingScheme, outputSymbols);
  }

  public OrderingScheme getOrderingScheme() {
    return orderingScheme;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MergeSortNode sortNode = (MergeSortNode) o;
    return Objects.equal(orderingScheme, sortNode.orderingScheme)
        && Objects.equal(outputSymbols, sortNode.outputSymbols);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), orderingScheme, outputSymbols);
  }

  @Override
  public String toString() {
    return "MergeSortNode-" + this.getPlanNodeId();
  }
}
