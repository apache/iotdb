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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * For a UDAFPlan, we construct an inner AggregationPlan for it. Example: select
 * count(a)/count(b),count(a)+sum(b) from root.sg To init inner AggregationPlan, we will convert it
 * to statement: select count(a),count(b),count(a),sum(b) from root.sg innerResultColumnsCache will
 * be [count(a),count(b),sum(b)]
 */
public class UDAFQueryOperator extends AggregationQueryOperator {

  private List<ResultColumn> innerResultColumnsCache;

  private AggregationQueryOperator innerAggregationQueryOperator;

  public UDAFQueryOperator(AggregationQueryOperator queryOperator) {
    super(queryOperator);
    this.innerAggregationQueryOperator = queryOperator;
  }

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    if (!isAlignByTime()) {
      throw new LogicalOperatorException("AGGREGATION doesn't support disable align clause.");
    }
    checkSelectComponent(selectComponent);
    if (isGroupByLevel()) {
      throw new LogicalOperatorException(
          "UDF nesting aggregations in GROUP BY query does not support grouping by level now.");
    }
    if (innerAggregationQueryOperator instanceof GroupByFillQueryOperator) {
      throw new LogicalOperatorException(
          "UDF nesting aggregations in GROUP BY query does not support FILL now.");
    }
  }

  private void checkSelectComponent(SelectComponent selectComponent)
      throws LogicalOperatorException {
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      checkEachExpression(expression);
    }
  }

  public List<ResultColumn> getInnerResultColumnsCache() {
    if (innerResultColumnsCache == null) {
      innerResultColumnsCache = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        addInnerResultColumn(expression);
      }
    }
    return innerResultColumnsCache;
  }

  private void addInnerResultColumn(Expression expression) {
    for (Iterator<Expression> it = expression.iterator(); it.hasNext(); ) {
      Expression currentExp = it.next();
      if (currentExp.isPlainAggregationFunctionExpression()) {
        innerResultColumnsCache.add(new ResultColumn(currentExp));
      }
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AggregationPlan innerAggregationPlan = initInnerAggregationPlan(generator);
    PhysicalPlan physicalPlan;
    if (!isAlignByDevice()) {
      physicalPlan =
          super.generateRawDataQueryPlan(generator, new UDAFPlan(selectComponent.getZoneId()));
      UDAFPlan udafPlan = (UDAFPlan) physicalPlan;
      udafPlan.setInnerAggregationPlan(innerAggregationPlan);
      udafPlan.constructUdfExecutors(selectComponent.getResultColumns());
      return udafPlan;
    } else {
      // todo: align by device
      physicalPlan = new AggregationPlan();
    }
    return physicalPlan;
  }

  private AggregationPlan initInnerAggregationPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    SelectComponent copiedSelectComponent = new SelectComponent(getSelectComponent());
    copiedSelectComponent.setHasPlainAggregationFunction(true);
    copiedSelectComponent.setResultColumns(getInnerResultColumnsCache());
    innerAggregationQueryOperator.setSelectComponent(copiedSelectComponent);
    innerAggregationQueryOperator.setFromComponent(getFromComponent());
    innerAggregationQueryOperator.setWhereComponent(getWhereComponent());
    innerAggregationQueryOperator.setSpecialClauseComponent(getSpecialClauseComponent());
    innerAggregationQueryOperator.setProps(getProps());
    innerAggregationQueryOperator.setIndexType(getIndexType());
    innerAggregationQueryOperator.setEnableTracing(isEnableTracing());
    return (AggregationPlan) innerAggregationQueryOperator.generatePhysicalPlan(generator);
  }

  private void checkEachExpression(Expression expression) throws LogicalOperatorException {
    if (expression instanceof BinaryExpression) {
      checkEachExpression(((BinaryExpression) expression).getLeftExpression());
      checkEachExpression(((BinaryExpression) expression).getRightExpression());
      return;
    }
    if (expression instanceof TimeSeriesOperand) {
      throw new LogicalOperatorException(
          "Common queries and aggregated queries are not allowed to appear at the same time");
    }
    // Currently, the aggregation function expression can only contain a timeseries operand.
    if (expression instanceof FunctionExpression
        && (((FunctionExpression) expression).getExpressions().size() != 1
            || !(((FunctionExpression) expression).getExpressions().get(0)
                instanceof TimeSeriesOperand))) {
      throw new LogicalOperatorException(ERROR_MESSAGE1);
    }
  }
}
