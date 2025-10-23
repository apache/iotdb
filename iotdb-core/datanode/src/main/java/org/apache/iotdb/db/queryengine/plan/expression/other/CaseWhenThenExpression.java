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

package org.apache.iotdb.db.queryengine.plan.expression.other;

import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CaseWhenThenExpression extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CaseWhenThenExpression.class);
  protected List<WhenThenExpression> whenThenExpressions = new ArrayList<>();
  protected Expression elseExpression;

  public CaseWhenThenExpression(
      List<WhenThenExpression> whenThenExpressions, Expression elseExpression) {
    this.whenThenExpressions = whenThenExpressions;
    this.elseExpression = elseExpression;
    if (this.elseExpression == null) {
      this.elseExpression = new NullOperand();
    }
  }

  public CaseWhenThenExpression(ByteBuffer byteBuffer) {
    int len = ReadWriteIOUtils.readInt(byteBuffer);
    Validate.isTrue(
        len > 0, "the length of CaseWhenThenExpression's whenThenList must greater than 0");
    for (int i = 0; i < len; i++) {
      Expression expression = Expression.deserialize(byteBuffer);
      this.whenThenExpressions.add((WhenThenExpression) expression);
    }
    this.elseExpression = Expression.deserialize(byteBuffer);
  }

  public void setElseExpression(Expression expression) {
    this.elseExpression = expression;
  }

  public List<WhenThenExpression> getWhenThenExpressions() {
    return whenThenExpressions;
  }

  public Expression getElseExpression() {
    return elseExpression;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.CASE_WHEN_THEN;
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    for (Expression expression : this.getExpressions()) {
      if (!expression.isMappable(expressionTypes)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean isConstantOperandInternal() {
    for (Expression expression : this.getExpressions()) {
      if (!expression.isConstantOperand()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    for (Expression expression : this.getExpressions()) {
      expression.constructUdfExecutors(expressionName2Executor, zoneId);
    }
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    this.getExpressions()
        .forEach(expression -> expression.bindInputLayerColumnIndexWithExpression(inputLocations));
    final String digest = getExpressionString();

    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    this.getExpressions()
        .forEach(expression -> expression.updateStatisticsForMemoryAssigner(memoryAssigner));
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  protected String getExpressionStringInternal() {
    StringBuilder builder = new StringBuilder();
    builder.append("CASE ");
    for (Expression expression : this.whenThenExpressions) {
      builder.append(expression.getExpressionString()).append(" ");
    }
    if (!(this.elseExpression instanceof NullOperand)) {
      builder.append("ELSE ").append(this.elseExpression.getExpressionString()).append(" ");
    }
    builder.append("END");
    return builder.toString();
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    int len = this.whenThenExpressions.size();
    ReadWriteIOUtils.write(len, byteBuffer);
    getExpressions().forEach(child -> Expression.serialize(child, byteBuffer));
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.whenThenExpressions.size(), stream);
    for (Expression expression : this.getExpressions()) {
      Expression.serialize(expression, stream);
    }
  }

  @Override
  public List<Expression> getExpressions() {
    List<Expression> result = new ArrayList<>(whenThenExpressions);
    result.add(elseExpression);
    return result;
  }

  @Override
  public String getOutputSymbolInternal() {
    StringBuilder builder = new StringBuilder();
    builder.append("CASE ");
    for (Expression expression : this.whenThenExpressions) {
      builder.append(expression.getOutputSymbol()).append(" ");
    }
    if (!(this.elseExpression instanceof NullOperand)) {
      builder.append("ELSE ").append(this.elseExpression.getOutputSymbol()).append(" ");
    }
    builder.append("END");
    return builder.toString();
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitCaseWhenThenExpression(this, context);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(elseExpression)
        + (whenThenExpressions == null
            ? 0
            : whenThenExpressions.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum());
  }
}
