/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.mpp.plan.expression.ternary;

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

public abstract class TernaryExpression extends Expression {
  protected final Expression firstExpression;
  protected final Expression secondExpression;
  protected final Expression thirdExpression;

  public Expression getFirstExpression() {
    return firstExpression;
  }

  public Expression getSecondExpression() {
    return secondExpression;
  }

  public Expression getThirdExpression() {
    return thirdExpression;
  }

  protected TernaryExpression(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    this.firstExpression = firstExpression;
    this.secondExpression = secondExpression;
    this.thirdExpression = thirdExpression;
  }

  protected TernaryExpression(ByteBuffer byteBuffer) {
    this.firstExpression = Expression.deserialize(byteBuffer);
    this.secondExpression = Expression.deserialize(byteBuffer);
    this.thirdExpression = Expression.deserialize(byteBuffer);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTernaryExpression(this, context);
  }

  @Override
  public boolean isConstantOperandInternal() {
    return firstExpression.isConstantOperand()
        && secondExpression.isConstantOperand()
        && thirdExpression.isConstantOperand();
  }

  @Override
  public List<Expression> getExpressions() {
    return Arrays.asList(firstExpression, secondExpression, thirdExpression);
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    return firstExpression.isMappable(expressionTypes)
        && secondExpression.isMappable(expressionTypes)
        && thirdExpression.isMappable(expressionTypes);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    firstExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    secondExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    thirdExpression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    firstExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    secondExpression.bindInputLayerColumnIndexWithExpression(inputLocations);
    thirdExpression.bindInputLayerColumnIndexWithExpression(inputLocations);

    final String digest = toString();
    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    firstExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    secondExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    thirdExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  protected abstract String operator();

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(firstExpression, byteBuffer);
    Expression.serialize(secondExpression, byteBuffer);
    Expression.serialize(thirdExpression, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    Expression.serialize(firstExpression, stream);
    Expression.serialize(secondExpression, stream);
    Expression.serialize(thirdExpression, stream);
  }
}
