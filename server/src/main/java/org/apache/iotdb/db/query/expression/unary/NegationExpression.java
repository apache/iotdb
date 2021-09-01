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

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.layer.InputLayer;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticNegationTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NegationExpression extends Expression {

  protected Expression expression;

  public NegationExpression(Expression expression) {
    this.expression = expression;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return true;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new NegationExpression(resultExpression));
    }
  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new NegationExpression(resultExpression));
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    expression.collectPaths(pathSet);
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      UDTFPlan udtfPlan,
      InputLayer inputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap)
      throws QueryProcessException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      IntermediateLayer parentIntermediateLayer =
          expression.constructIntermediateLayer(
              udtfPlan, inputLayer, expressionIntermediateLayerMap);

      expressionIntermediateLayerMap.put(
          this,
          new IntermediateLayer(
              new ArithmeticNegationTransformer(parentIntermediateLayer.constructPointReader()),
              -1,
              -1));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  @Override
  public String toString() {
    return "-" + expression.toString();
  }
}
