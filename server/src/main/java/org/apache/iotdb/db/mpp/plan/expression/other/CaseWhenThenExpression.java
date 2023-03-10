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

package org.apache.iotdb.db.mpp.plan.expression.other;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CaseWhenThenExpression extends Expression {
  protected List<WhenThenExpression> whenThenExpressions = new ArrayList<>();
  protected Expression elseExpression;

  public CaseWhenThenExpression(
      List<WhenThenExpression> whenThenExpressions, Expression elseExpression) {
    this.whenThenExpressions = whenThenExpressions;
    if (elseExpression != null) {
      this.elseExpression = elseExpression;
    } else {
      this.elseExpression = new NullOperand();
    }
  }

  public CaseWhenThenExpression(ByteBuffer byteBuffer) {
    while (true) {
      Expression expression = Expression.deserialize(byteBuffer);
      if (expression.getExpressionType() == ExpressionType.WHEN_THEN) {
        this.whenThenExpressions.add((WhenThenExpression) expression);
      } else {
        this.elseExpression = expression;
        break;
      }
    }
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
      if (!expression.isMappable(expressionTypes)) return false;
    }
    return true;
  }

  @Override
  protected boolean isConstantOperandInternal() {
    for (Expression expression : this.getExpressions()) {
      if (!expression.isConstantOperand()) return false;
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
    final String digest = toString();

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
      builder.append(expression.toString()).append(" ");
    }
    builder.append("ELSE ").append(this.elseExpression.toString());
    return builder.toString();
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    getExpressions().forEach(child -> Expression.serialize(child, byteBuffer));
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
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
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitCaseWhenThenExpression(this, context);
  }

  // TODO: constructUdfExecutors

}
