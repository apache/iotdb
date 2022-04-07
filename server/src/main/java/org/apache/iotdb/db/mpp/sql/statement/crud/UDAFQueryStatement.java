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

package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.List;

/**
 * For a UDAF Statement, we construct an inner AggregationStatement for it. Example: select
 * count(a)/count(b),count(a)+sum(b) from root.sg To init inner AggregationStatement, we will
 * convert it to statement: select count(a),count(b),count(a),sum(b) from root.sg
 * innerResultColumnsCache will be [count(a),count(b),sum(b)]
 */
public class UDAFQueryStatement extends QueryStatement {

  private List<ResultColumn> innerResultColumnsCache;

  private AggregationQueryStatement innerAggregationQueryStatement;

  public UDAFQueryStatement(AggregationQueryStatement querySatement) {
    super(querySatement);
    this.innerAggregationQueryStatement = querySatement;
  }

  @Override
  public void selfCheck() {
    super.selfCheck();

    if (!DisableAlign()) {
      throw new SemanticException("AGGREGATION doesn't support disable align clause.");
    }
    checkSelectComponent(selectComponent);
    if (isGroupByLevel()) {
      throw new SemanticException(
          "UDF nesting aggregations in GROUP BY query does not support grouping by level now.");
    }
    if (innerAggregationQueryStatement instanceof GroupByQueryStatement) {
      throw new SemanticException(
          "UDF nesting aggregations in GROUP BY query does not support FILL now.");
    }
  }

  private void checkSelectComponent(SelectComponent selectComponent) throws SemanticException {
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      checkEachExpression(expression);
    }
  }

  private void checkEachExpression(Expression expression) throws SemanticException {
    if (expression instanceof TimeSeriesOperand) {
      throw new SemanticException(
          "Common queries and aggregated queries are not allowed to appear at the same time.");
    }
    // Currently, the aggregation function expression can only contain a timeseries operand.
    if (expression.isBuiltInAggregationFunctionExpression()) {
      if (expression.getExpressions().size() == 1
          && expression.getExpressions().get(0) instanceof TimeSeriesOperand) {
        return;
      }
      throw new SemanticException(
          "The argument of the aggregation function must be a time series.");
    }

    for (Expression childExp : expression.getExpressions()) {
      checkEachExpression(childExp);
    }
  }
}
