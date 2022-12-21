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

package org.apache.iotdb.db.mpp.plan.expression.unary;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class UnaryExpression extends Expression {

  protected final Expression expression;

  protected UnaryExpression(Expression expression) {
    this.expression = expression;
  }

  public final Expression getExpression() {
    return expression;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitUnaryExpression(this, context);
  }

  @Override
  public final boolean isConstantOperandInternal() {
    return expression.isConstantOperand();
  }

  @Override
  public final List<Expression> getExpressions() {
    return Collections.singletonList(expression);
  }

  @Override
  public final void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    expression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    expression.bindInputLayerColumnIndexWithExpression(inputLocations);

    final String digest = toString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public final void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    return expression.isMappable(expressionTypes);
  }

  protected abstract Expression constructExpression(Expression childExpression);

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(expression, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(expression, stream);
  }
}
