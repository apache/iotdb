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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class TernaryExpression extends Expression {
  protected final Expression firstExpression;
  protected final Expression secondExpression;
  protected final Expression thirdExpression;
  // protected final boolean isNot = false;

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
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public boolean isUserDefinedAggregationFunctionExpression() {
    return firstExpression.isBuiltInAggregationFunctionExpression()
        || secondExpression.isBuiltInAggregationFunctionExpression()
        || thirdExpression.isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public List<Expression> getExpressions() {
    return Arrays.asList(firstExpression, secondExpression, thirdExpression);
  }

  @Override
  public final void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> firstExpressions = new ArrayList<>();
    firstExpression.concat(prefixPaths, firstExpressions);

    List<Expression> secondExpressions = new ArrayList<>();
    secondExpression.concat(prefixPaths, secondExpressions);

    List<Expression> thirdExpressions = new ArrayList<>();
    secondExpression.concat(prefixPaths, thirdExpressions);

    reconstruct(firstExpressions, secondExpressions, thirdExpressions, resultExpressions);
  }

  @Override
  public final void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> firstExpressions = new ArrayList<>();
    firstExpression.removeWildcards(wildcardsRemover, firstExpressions);

    List<Expression> secondExpressions = new ArrayList<>();
    secondExpression.removeWildcards(wildcardsRemover, secondExpressions);

    List<Expression> thirdExpressions = new ArrayList<>();
    thirdExpression.removeWildcards(wildcardsRemover, secondExpressions);
    reconstruct(firstExpressions, secondExpressions, thirdExpressions, resultExpressions);
  }

  private void reconstruct(
      List<Expression> firstExpressions,
      List<Expression> secondExpressions,
      List<Expression> thirdExpressions,
      List<Expression> resultExpressions) {
    for (Expression fe : firstExpressions) {
      for (Expression se : secondExpressions)
        for (Expression te : thirdExpressions) {
          switch (operator()) {
            case "between":
              resultExpressions.add(new BetweenExpression(fe, se, te));
              break;
            default:
              throw new UnsupportedOperationException();
          }
        }
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    firstExpression.collectPaths(pathSet);
    secondExpression.collectPaths(pathSet);
    thirdExpression.collectPaths(pathSet);
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
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    firstExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    secondExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    thirdExpression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
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
