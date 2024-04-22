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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AggregationMergeSortNode extends MultiChildProcessNode {

  private final OrderByParameter mergeOrderParameter;

  private final List<String> outputColumns;

  private final Set<Expression> selectExpressions;

  private boolean hasGroupBy;

  public AggregationMergeSortNode(
      PlanNodeId id,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns,
      Set<Expression> selectExpressions,
      boolean hasGroupBy) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
    this.selectExpressions = selectExpressions;
    this.hasGroupBy = hasGroupBy;
  }

  public AggregationMergeSortNode(
      PlanNodeId id,
      List<PlanNode> children,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns,
      Set<Expression> selectExpressions,
      boolean hasGroupBy) {
    super(id, children);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
    this.selectExpressions = selectExpressions;
    this.hasGroupBy = hasGroupBy;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  public Set<Expression> getSelectExpressions() {
    return this.selectExpressions;
  }

  public boolean isHasGroupBy() {
    return this.hasGroupBy;
  }

  @Override
  public PlanNode clone() {
    return new AggregationMergeSortNode(
        getPlanNodeId(), getMergeOrderParameter(), outputColumns, selectExpressions, hasGroupBy);
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new AggregationMergeSortNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)),
        getMergeOrderParameter(),
        outputColumns,
        selectExpressions,
        hasGroupBy);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumns;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregationMergeSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGG_MERGE_SORT.serialize(byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
    ReadWriteIOUtils.write(selectExpressions.size(), byteBuffer);
    for (Expression expression : selectExpressions) {
      Expression.serialize(expression, byteBuffer);
    }
    ReadWriteIOUtils.write(hasGroupBy, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.AGG_MERGE_SORT.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(outputColumns.size(), stream);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, stream);
    }
    ReadWriteIOUtils.write(selectExpressions.size(), stream);
    for (Expression expression : selectExpressions) {
      Expression.serialize(expression, stream);
    }
    ReadWriteIOUtils.write(hasGroupBy, stream);
  }

  public static AggregationMergeSortNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumns = new ArrayList<>();
    while (columnSize > 0) {
      outputColumns.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }
    Set<Expression> expressions = new LinkedHashSet<>();
    int expressionSize = ReadWriteIOUtils.readInt(byteBuffer);
    while (expressionSize > 0) {
      expressions.add(Expression.deserialize(byteBuffer));
      expressionSize--;
    }
    boolean hasGroupBy = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AggregationMergeSortNode(
        planNodeId, orderByParameter, outputColumns, expressions, hasGroupBy);
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
    AggregationMergeSortNode that = (AggregationMergeSortNode) o;
    return Objects.equals(mergeOrderParameter, that.getMergeOrderParameter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrderParameter);
  }

  @Override
  public String toString() {
    return "AggregationMergeSort-" + this.getPlanNodeId();
  }
}
