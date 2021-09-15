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

package org.apache.iotdb.db.query.expression.binary;

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticBinaryTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BinaryExpression extends Expression {

  protected final Expression leftExpression;
  protected final Expression rightExpression;

  protected BinaryExpression(Expression leftExpression, Expression rightExpression) {
    this.leftExpression = leftExpression;
    this.rightExpression = rightExpression;
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return true;
  }

  @Override
  public final void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> leftExpressions = new ArrayList<>();
    leftExpression.concat(prefixPaths, leftExpressions);

    List<Expression> rightExpressions = new ArrayList<>();
    rightExpression.concat(prefixPaths, rightExpressions);

    reconstruct(leftExpressions, rightExpressions, resultExpressions);
  }

  @Override
  public final void removeWildcards(
      WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> leftExpressions = new ArrayList<>();
    leftExpression.removeWildcards(wildcardsRemover, leftExpressions);

    List<Expression> rightExpressions = new ArrayList<>();
    rightExpression.removeWildcards(wildcardsRemover, rightExpressions);

    reconstruct(leftExpressions, rightExpressions, resultExpressions);
  }

  private void reconstruct(
      List<Expression> leftExpressions,
      List<Expression> rightExpressions,
      List<Expression> resultExpressions) {
    for (Expression le : leftExpressions) {
      for (Expression re : rightExpressions) {
        switch (operator()) {
          case "+":
            resultExpressions.add(new AdditionExpression(le, re));
            break;
          case "-":
            resultExpressions.add(new SubtractionExpression(le, re));
            break;
          case "*":
            resultExpressions.add(new MultiplicationExpression(le, re));
            break;
          case "/":
            resultExpressions.add(new DivisionExpression(le, re));
            break;
          case "%":
            resultExpressions.add(new ModuloExpression(le, re));
            break;
          default:
            throw new UnsupportedOperationException();
        }
      }
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    leftExpression.collectPaths(pathSet);
    rightExpression.collectPaths(pathSet);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    leftExpression.constructUdfExecutors(expressionName2Executor, zoneId);
    rightExpression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    leftExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    rightExpression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFPlan udtfPlan,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();

      IntermediateLayer leftParentIntermediateLayer =
          leftExpression.constructIntermediateLayer(
              queryId,
              udtfPlan,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      IntermediateLayer rightParentIntermediateLayer =
          rightExpression.constructIntermediateLayer(
              queryId,
              udtfPlan,
              rawTimeSeriesInputLayer,
              expressionIntermediateLayerMap,
              expressionDataTypeMap,
              memoryAssigner);
      Transformer transformer =
          constructTransformer(
              leftParentIntermediateLayer.constructPointReader(),
              rightParentIntermediateLayer.constructPointReader());
      expressionDataTypeMap.put(this, transformer.getDataType());

      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, transformer));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  protected abstract ArithmeticBinaryTransformer constructTransformer(
      LayerPointReader leftParentLayerPointReader, LayerPointReader rightParentLayerPointReader);

  @Override
  public final String toString() {
    return String.format(
        "%s %s %s", leftExpression.toString(), operator(), rightExpression.toString());
  }

  protected abstract String operator();
}
