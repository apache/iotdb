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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * For a UDAFPlan, we construct an inner AggregationPlan for it. Example: select
 * count(a)/count(b),count(a)+sum(b) from root.sg To init inner AggregationPlan, we will convert it
 * to statement: select count(a),count(b),count(a),sum(b) from root.sg innerResultColumnsCache will
 * be [count(a),count(b),sum(b)] innerPathCathe will be [root.sg.a,root.sg.b,root.sg.b]
 * innerAggregationsCache will be [count,count,sum]
 */
public class UDAFQueryOperator extends QueryOperator {

  private ArrayList<ResultColumn> innerResultColumnsCache;

  private ArrayList<PartialPath> innerPathsCache;

  private ArrayList<String> innerAggregationsCache;

  private AggregationQueryOperator aggrOp;

  public UDAFQueryOperator(AggregationQueryOperator queryOperator) {
    super(queryOperator);
    this.aggrOp = queryOperator;
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
          "UDF nesting aggregations in GROUP BY LEVEL query does not support now.");
    }
  }

  private void checkSelectComponent(SelectComponent selectComponent)
      throws LogicalOperatorException {
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      checkEachExpression(expression);
    }
  }

  public ArrayList<PartialPath> getInnerPathCathe() {
    if (innerPathsCache == null) {
      innerPathsCache = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        addInnerPath(expression);
      }
    }
    return innerPathsCache;
  }

  public ArrayList<ResultColumn> getInnerResultColumnsCache() {
    if (innerResultColumnsCache == null) {
      innerResultColumnsCache = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        addInnerResultColumn(expression);
      }
    }
    return innerResultColumnsCache;
  }

  public ArrayList<String> getInnerAggregationsCache() {
    if (innerAggregationsCache == null) {
      innerAggregationsCache = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        addInnerAggregations(expression);
      }
    }
    return innerAggregationsCache;
  }

  private void addInnerAggregations(Expression expression) {
    for (Iterator<Expression> it = expression.iterator(); it.hasNext(); ) {
      Expression currentExp = it.next();
      if (currentExp.isAggregationFunctionExpression()) {
        innerAggregationsCache.add(((FunctionExpression) currentExp).getFunctionName());
      }
    }
  }

  private void addInnerPath(Expression expression) {
    for (Iterator<Expression> it = expression.iterator(); it.hasNext(); ) {
      Expression currentExp = it.next();
      if (currentExp.isAggregationFunctionExpression()) {
        innerPathsCache.add(((TimeSeriesOperand) currentExp.getExpressions().get(0)).getPath());
      }
    }
  }

  private void addInnerResultColumn(Expression expression) {
    for (Iterator<Expression> it = expression.iterator(); it.hasNext(); ) {
      Expression currentExp = it.next();
      if (currentExp.isAggregationFunctionExpression()) {
        innerResultColumnsCache.add(new ResultColumn(currentExp));
      }
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    SelectComponent copiedSelectComponent = new SelectComponent(getSelectComponent());
    copiedSelectComponent.setResultColumns(getInnerResultColumnsCache());
    aggrOp.setSelectComponent(copiedSelectComponent);
    aggrOp.setFromComponent(getFromComponent());
    aggrOp.setWhereComponent(getWhereComponent());
    aggrOp.setSpecialClauseComponent(getSpecialClauseComponent());
    aggrOp.setProps(getProps());
    aggrOp.setIndexType(getIndexType());
    aggrOp.setEnableTracing(isEnableTracing());
    AggregationPlan innerAggregationPlan = (AggregationPlan) aggrOp.generatePhysicalPlan(generator);
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

  private void checkEachExpression(Expression expression) throws LogicalOperatorException {
    if (expression instanceof TimeSeriesOperand) {
      throw new LogicalOperatorException(AggregationQueryOperator.ERROR_MESSAGE1);
    }
    // Currently, the aggregation function expression can only contain a timeseries operand.
    if (expression.isAggregationFunctionExpression()) {
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
