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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class RowNumberNode extends SingleChildProcessNode {
  private final List<Symbol> partitionBy;
  /*
   * This flag indicates that the node depends on the row order established by the subplan.
   * It is taken into account while adding local exchanges to the plan, ensuring that sorted order
   * of data will be respected.
   * Note: if the subplan doesn't produce sorted output, this flag doesn't change the resulting plan.
   * Note: this flag is used for planning of queries involving ORDER BY and OFFSET.
   */
  private final boolean orderSensitive;
  private final Optional<Integer> maxRowCountPerPartition;
  private final Symbol rowNumberSymbol;

  public RowNumberNode(
      PlanNodeId id,
      List<Symbol> partitionBy,
      boolean orderSensitive,
      Symbol rowNumberSymbol,
      Optional<Integer> maxRowCountPerPartition) {
    super(id);

    this.partitionBy = ImmutableList.copyOf(partitionBy);
    this.orderSensitive = orderSensitive;
    this.rowNumberSymbol = rowNumberSymbol;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
  }

  public RowNumberNode(
      PlanNodeId id,
      PlanNode child,
      List<Symbol> partitionBy,
      boolean orderSensitive,
      Symbol rowNumberSymbol,
      Optional<Integer> maxRowCountPerPartition) {
    super(id, child);

    this.partitionBy = ImmutableList.copyOf(partitionBy);
    this.orderSensitive = orderSensitive;
    this.rowNumberSymbol = rowNumberSymbol;
    this.maxRowCountPerPartition = maxRowCountPerPartition;
  }

  public Symbol getRowNumberSymbol() {
    return rowNumberSymbol;
  }

  public Optional<Integer> getMaxRowCountPerPartition() {
    return maxRowCountPerPartition;
  }

  public boolean isOrderSensitive() {
    return orderSensitive;
  }

  @Override
  public PlanNode clone() {
    return new RowNumberNode(
        getPlanNodeId(), partitionBy, orderSensitive, rowNumberSymbol, maxRowCountPerPartition);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRowNumber(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_ROW_NUMBER_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(partitionBy.size(), byteBuffer);
    for (Symbol symbol : partitionBy) {
      Symbol.serialize(symbol, byteBuffer);
    }
    ReadWriteIOUtils.write(orderSensitive, byteBuffer);
    Symbol.serialize(rowNumberSymbol, byteBuffer);
    if (maxRowCountPerPartition.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(maxRowCountPerPartition.get(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
  }

  public List<Symbol> getPartitionBy() {
    return partitionBy;
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_ROW_NUMBER_NODE.serialize(stream);
    ReadWriteIOUtils.write(partitionBy.size(), stream);
    for (Symbol symbol : partitionBy) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(orderSensitive, stream);
    Symbol.serialize(rowNumberSymbol, stream);
    if (maxRowCountPerPartition.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(maxRowCountPerPartition.get(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  public static RowNumberNode deserialize(ByteBuffer buffer) {
    int partitionBySize = ReadWriteIOUtils.readInt(buffer);
    ImmutableList.Builder<Symbol> partitionBy = ImmutableList.builder();
    for (int i = 0; i < partitionBySize; i++) {
      partitionBy.add(Symbol.deserialize(buffer));
    }
    boolean orderSensitive = ReadWriteIOUtils.readBoolean(buffer);
    Symbol rowNumberSymbol = Symbol.deserialize(buffer);
    Optional<Integer> maxRowCountPerPartition;
    if (ReadWriteIOUtils.readBoolean(buffer)) {
      maxRowCountPerPartition = Optional.of(ReadWriteIOUtils.readInt(buffer));
    } else {
      maxRowCountPerPartition = Optional.empty();
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new RowNumberNode(
        planNodeId, partitionBy.build(), orderSensitive, rowNumberSymbol, maxRowCountPerPartition);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(getChild().getOutputSymbols())
        .add(rowNumberSymbol)
        .build();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 1, "wrong number of new children");
    return new RowNumberNode(
        id,
        newChildren.get(0),
        partitionBy,
        orderSensitive,
        rowNumberSymbol,
        maxRowCountPerPartition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RowNumberNode node = (RowNumberNode) o;

    if (node.partitionBy.size() != partitionBy.size()) return false;
    for (int i = 0; i < partitionBy.size(); i++) {
      if (!node.partitionBy.get(i).equals(partitionBy.get(i))) return false;
    }
    return Objects.equal(orderSensitive, node.orderSensitive)
        && Objects.equal(rowNumberSymbol, node.rowNumberSymbol)
        && Objects.equal(maxRowCountPerPartition, node.maxRowCountPerPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), partitionBy, orderSensitive, rowNumberSymbol, maxRowCountPerPartition);
  }

  @Override
  public String toString() {
    return "RowNumber-" + this.getPlanNodeId();
  }
}
