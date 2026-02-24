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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ValuesNode extends SourceNode {
  private final List<Symbol> outputSymbols;
  private final int rowCount;
  // If ValuesNode produces output symbols, each row in ValuesNode is represented by a single
  // expression in `rows` list.
  // It can be an expression of type Row or any other expression that evaluates to RowType.
  // In case when output symbols are present but ValuesNode does not have any rows, `rows` is an
  // Optional with empty list.
  // If ValuesNode does not produce any output symbols, `rows` is Optional.empty().
  private final Optional<List<Expression>> rows;

  protected TRegionReplicaSet regionReplicaSet;

  /** Constructor of ValuesNode with non-empty output symbols list */
  public ValuesNode(PlanNodeId id, List<Symbol> outputSymbols, List<Expression> rows) {
    this(id, outputSymbols, rows.size(), Optional.of(rows));
  }

  /** Constructor of ValuesNode with empty output symbols list */
  public ValuesNode(PlanNodeId id, int rowCount) {
    this(id, ImmutableList.of(), rowCount, Optional.empty());
  }

  public ValuesNode(
      PlanNodeId id, List<Symbol> outputSymbols, int rowCount, Optional<List<Expression>> rows) {
    super(id);
    this.outputSymbols =
        ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
    this.rowCount = rowCount;

    requireNonNull(rows, "rows is null");
    if (rows.isPresent()) {
      checkArgument(
          rowCount == rows.get().size(),
          "declared and actual row counts don't match: %s vs %s",
          rowCount,
          rows.get().size());

      // check row size consistency (only for rows specified as Row)
      List<Integer> rowSizes =
          rows.get().stream()
              .map(row -> requireNonNull(row, "row is null"))
              .filter(expression -> expression instanceof Row)
              .map(expression -> ((Row) expression).getItems().size())
              .distinct()
              .collect(toImmutableList());
      checkState(rowSizes.size() <= 1, "mismatched rows. All rows must be the same size");

      // check if row size matches the number of output symbols (only for rows specified as Row)
      if (rowSizes.size() == 1) {
        checkState(
            getOnlyElement(rowSizes).equals(outputSymbols.size()),
            "row size doesn't match the number of output symbols: %s vs %s",
            getOnlyElement(rowSizes),
            outputSymbols.size());
      }
    } else {
      checkArgument(
          outputSymbols.isEmpty(),
          "missing rows specification for Values with non-empty output symbols");
    }

    if (outputSymbols.isEmpty()) {
      this.rows = Optional.empty();
    } else {
      this.rows = rows.map(ImmutableList::copyOf);
    }
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitValuesNode(this, context);
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new ValuesNode(
        getPlanNodeId(), outputSymbols, rowCount, rows.map(ImmutableList::copyOf));
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputSymbols.stream().map(Symbol::getName).collect(toImmutableList());
  }

  @Override
  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    outputSymbols.forEach(symbol -> ReadWriteIOUtils.write(symbol.getName(), byteBuffer));
    ReadWriteIOUtils.write(rowCount, byteBuffer);
    if (rows.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(rows.get().size(), byteBuffer);
      for (Expression expression : rows.get()) {
        Expression.serialize(expression, byteBuffer);
      }
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
  }

  @Override
  public void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_VALUES_NODE.serialize(stream);

    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      ReadWriteIOUtils.write(symbol.getName(), stream);
    }
    ReadWriteIOUtils.write(rowCount, stream);
    if (rows.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(rows.get().size(), stream);
      for (Expression expression : rows.get()) {
        Expression.serialize(expression, stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  public static ValuesNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_VALUES_NODE.serialize(byteBuffer);

    int size = ReadWriteIOUtils.read(byteBuffer);
    List<Symbol> outputSymbols = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      outputSymbols.add(new Symbol(ReadWriteIOUtils.readString(byteBuffer)));
    }

    int rowCount = ReadWriteIOUtils.read(byteBuffer);

    List<Expression> rows = new ArrayList<>();
    boolean flag = ReadWriteIOUtils.readBool(byteBuffer);
    if (flag) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; i++) {
        rows.add(Expression.deserialize(byteBuffer));
      }
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ValuesNode(
        planNodeId, outputSymbols, rowCount, flag ? Optional.of(rows) : Optional.empty());
  }

  public int getRowCount() {
    return rowCount;
  }

  public Optional<List<Expression>> getRows() {
    return rows;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.isEmpty(), "newChildren is not empty");
    return this;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public void close() throws Exception {}
}
