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

package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class UnaryExpression extends Expression {

  protected final Expression expression;

  protected UnaryExpression(Expression expression) {
    this.expression = expression;
  }

  public final Expression getExpression() {
    return expression;
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
  public final boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public final boolean isUserDefinedAggregationFunctionExpression() {
    return expression.isUserDefinedAggregationFunctionExpression()
        || expression.isBuiltInAggregationFunctionExpression();
  }

  @Override
  public final void collectPaths(Set<PartialPath> pathSet) {
    expression.collectPaths(pathSet);
  }

  @Override
  public final void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    expression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public final void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    expression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public final void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public final IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFContext udtfContext,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();

      IntermediateLayer parentLayerPointReader =
          expression.constructIntermediateLayer(
              queryId,
              udtfContext,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      Transformer transformer = constructTransformer(parentLayerPointReader.constructPointReader());
      expressionDataTypeMap.put(this, transformer.getDataType());

      // SingleInputColumnMultiReferenceIntermediateLayer doesn't support ConstantLayerPointReader
      // yet. And since a ConstantLayerPointReader won't produce too much IO,
      // SingleInputColumnSingleReferenceIntermediateLayer could be a better choice.
      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1 || isConstantOperand()
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  protected abstract Transformer constructTransformer(LayerPointReader pointReader);

  @Override
  public final void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(constructExpression(resultExpression));
    }
  }

  @Override
  public final void removeWildcards(
      WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(constructExpression(resultExpression));
    }
  }

  protected abstract Expression constructExpression(Expression childExpression);

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(expression, byteBuffer);
  }
}
