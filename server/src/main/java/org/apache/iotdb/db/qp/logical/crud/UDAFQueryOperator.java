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
import org.apache.iotdb.db.metadata.path.*;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.*;

/**
 * For a UDAFPlan, we construct an inner AggregationPlan for it. Example: select
 * count(a)/count(b),count(a)+sum(b) from root.sg To init inner AggregationPlan, we will convert it
 * to statement: select count(a),count(b),count(a),sum(b) from root.sg innerResultColumnsCache will
 * be [count(a),count(b),sum(b)]
 */
public class UDAFQueryOperator extends QueryOperator {

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
      Map<Expression, Integer> expressionToInnerResultIndexMap = new HashMap<>();
      // The following codes are used to establish a link from AggregationResult to expression tree
      // input.
      // For example:
      // Expressions           [   avg(s1) + 1, avg(s1) + avg(s2), sin(avg(s2)) ]
      //                                  |       |          |           |
      // Inner result columns  [      avg(s1) , avg(s1)  , avg(s2)     avg(s2)  ]
      //                                   \      /            \         /
      // deduplicated          [            avg(s1),             avg(s2)        ]
      //                                         \                /
      // expressionToInnerResultIndexMap        {avg(s1): 0, avg(s2): 1}
      Map<String, Integer> aggrIndexMap = new HashMap<>();
      for (int i = 0; i < innerAggregationPlan.getDeduplicatedPaths().size(); i++) {
        aggrIndexMap.put(
            innerAggregationPlan.getDeduplicatedAggregations().get(i)
                + "("
                + innerAggregationPlan.getDeduplicatedPaths().get(i)
                + ")",
            i);
        PartialPath partialPath = innerAggregationPlan.getDeduplicatedPaths().get(i);
        if (partialPath instanceof MeasurementPath) {
          MeasurementPath measurementPath = (MeasurementPath) partialPath;
          String alias = measurementPath.getMeasurementAlias();
          if (Objects.nonNull(alias) && !"".equals(alias)) {
            String fullPath = innerAggregationPlan.getDeduplicatedPaths().get(i).getFullPath();
            fullPath =
                fullPath.replace(
                    measurementPath.getTailNode(), measurementPath.getMeasurementAlias());
            aggrIndexMap.put(
                innerAggregationPlan.getDeduplicatedAggregations().get(i) + "(" + fullPath + ")",
                i);
          }
        }
      }
      for (ResultColumn rc : getInnerResultColumnsCache()) {
        expressionToInnerResultIndexMap.put(
            rc.getExpression(),
            aggrIndexMap.get(
                ((FunctionExpression) rc.getExpression()).getExpressionStringInternal()));
      }
      udafPlan.setExpressionToInnerResultIndexMap(expressionToInnerResultIndexMap);
      udafPlan.constructUdfExecutors(selectComponent.getResultColumns());
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
    if (expression instanceof TimeSeriesOperand) {
      throw new LogicalOperatorException(AggregationQueryOperator.ERROR_MESSAGE1);
    }
    // Currently, the aggregation function expression can only contain a timeseries operand.
    if (expression.isPlainAggregationFunctionExpression()) {
      if (expression.getExpressions().size() == 1
          && expression.getExpressions().get(0) instanceof TimeSeriesOperand) {
        return;
      }
      throw new LogicalOperatorException(
          "The argument of the aggregation function must be a time series.");
    }

    for (Expression childExp : expression.getExpressions()) {
      checkEachExpression(childExp);
    }
  }
}
