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
import org.apache.iotdb.db.query.udf.core.layer.InputLayer;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticBinaryTransformer;

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
  public IntermediateLayer constructIntermediateLayer(
      UDTFPlan udtfPlan,
      InputLayer inputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap)
      throws QueryProcessException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      IntermediateLayer leftParentIntermediateLayer =
          leftExpression.constructIntermediateLayer(
              udtfPlan, inputLayer, expressionIntermediateLayerMap);
      IntermediateLayer rightParentIntermediateLayer =
          rightExpression.constructIntermediateLayer(
              udtfPlan, inputLayer, expressionIntermediateLayerMap);

      expressionIntermediateLayerMap.put(
          this,
          new IntermediateLayer(
              constructTransformer(
                  leftParentIntermediateLayer.constructPointReader(),
                  rightParentIntermediateLayer.constructPointReader()),
              -1,
              -1));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  protected abstract ArithmeticBinaryTransformer constructTransformer(
      LayerPointReader leftParentLayerPointReader, LayerPointReader rightParentLayerPointReader);

  public Expression getLeftExpression() {
    return leftExpression;
  }

  public Expression getRightExpression() {
    return rightExpression;
  }

  @Override
  public final String toString() {
    return String.format(
        "%s %s %s", leftExpression.toString(), operator(), rightExpression.toString());
  }

  protected abstract String operator();
}
