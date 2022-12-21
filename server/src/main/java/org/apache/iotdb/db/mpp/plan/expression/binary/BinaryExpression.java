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

package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class BinaryExpression extends Expression {

  protected Expression leftExpression;
  protected Expression rightExpression;

  protected BinaryExpression(Expression leftExpression, Expression rightExpression) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
  }

  protected BinaryExpression(ByteBuffer byteBuffer) {
    this.leftExpression = Expression.deserialize(byteBuffer);
    this.rightExpression = Expression.deserialize(byteBuffer);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitBinaryExpression(this, context);
  }

  public Expression getLeftExpression() {
    return leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  public void setLeftExpression(Expression leftExpression) {
    this.leftExpression = leftExpression;
  }

  public void setRightExpression(Expression rightExpression) {
    this.rightExpression = rightExpression;
  }

  @Override
  public boolean isConstantOperandInternal() {
    return leftExpression.isConstantOperand() && rightExpression.isConstantOperand();
  }

  @Override
  public List<Expression> getExpressions() {
    return Arrays.asList(leftExpression, rightExpression);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    leftExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    rightExpression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    leftExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    rightExpression.bindInputLayerColumnIndexWithExpression(inputLocations);

    final String digest = toString();

    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    leftExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    rightExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    return leftExpression.isMappable(expressionTypes)
        && rightExpression.isMappable(expressionTypes);
  }

  @Override
  public final String getExpressionStringInternal() {
    StringBuilder builder = new StringBuilder();
    if (leftExpression.getExpressionType().getPriority() < this.getExpressionType().getPriority()) {
      builder.append("(").append(leftExpression.getExpressionString()).append(")");
    } else {
      builder.append(leftExpression.getExpressionString());
    }
    builder.append(" ").append(operator()).append(" ");
    if (rightExpression.getExpressionType().getPriority()
        < this.getExpressionType().getPriority()) {
      builder.append("(").append(rightExpression.getExpressionString()).append(")");
    } else {
      builder.append(rightExpression.getExpressionString());
    }

    return builder.toString();
  }

  protected abstract String operator();

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(leftExpression, byteBuffer);
    Expression.serialize(rightExpression, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(leftExpression, stream);
    Expression.serialize(rightExpression, stream);
  }
}
