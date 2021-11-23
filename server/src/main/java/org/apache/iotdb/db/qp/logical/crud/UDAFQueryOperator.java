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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * For a UDAFPlan, we construct an inner AggregationPlan for it. Example: select
 * count(a)/count(b),count(a)+sum(b) from root.sg To init inner AggregationPlan, we will convert it
 * to statement: select count(a),count(b),count(a),sum(b) from root.sg innerResultColumnsCache will
 * be [count(a),count(b),sum(b)] innerPathCathe will be [root.sg.a,root.sg.b,root.sg.b]
 * innerAggregationsCache will be [count,count,sum]
 */
public class UDAFQueryOperator extends AggregationQueryOperator {

  private ArrayList<ResultColumn> innerResultColumnsCache;

  private ArrayList<PartialPath> innerPathsCache;

  private ArrayList<String> innerAggregationsCache;

  private Map<Expression, Integer> expressionToInnerResultIndexMap = new HashMap<>();

  public UDAFQueryOperator() {
    super();
  }

  public UDAFQueryOperator(QueryOperator queryOperator) {
    super(queryOperator);
  }

  @Override
  public void check() throws LogicalOperatorException {
    super.check();
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
    if (expression instanceof BinaryExpression) {
      addInnerAggregations(((BinaryExpression) expression).getLeftExpression());
      addInnerAggregations(((BinaryExpression) expression).getRightExpression());
      return;
    }
    if (expression instanceof FunctionExpression && expression.isAggregationFunctionExpression()) {
      innerAggregationsCache.add(((FunctionExpression) expression).getFunctionName());
    }
  }

  private void addInnerPath(Expression expression) {
    if (expression instanceof BinaryExpression) {
      addInnerPath(((BinaryExpression) expression).getLeftExpression());
      addInnerPath(((BinaryExpression) expression).getRightExpression());
      return;
    }
    if (expression instanceof FunctionExpression && expression.isAggregationFunctionExpression()) {
      innerPathsCache.add(
          ((TimeSeriesOperand) ((FunctionExpression) expression).getExpressions().get(0))
              .getPath());
    }
  }

  private void addInnerResultColumn(Expression expression) {
    if (expression instanceof BinaryExpression) {
      addInnerResultColumn(((BinaryExpression) expression).getLeftExpression());
      addInnerResultColumn(((BinaryExpression) expression).getRightExpression());
      return;
    }
    if (expression.isAggregationFunctionExpression()) {
      if (!expressionToInnerResultIndexMap.containsKey(expression)) {
        expressionToInnerResultIndexMap.put(expression, expressionToInnerResultIndexMap.size());
      }
      innerResultColumnsCache.add(new ResultColumn(expression));
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AggregationPlan innerAggregationPlan =
        initInnerAggregationPlan(generator, new AggregationPlan());
    PhysicalPlan physicalPlan;
    if (!isAlignByDevice()) {
      physicalPlan = super.generateRawDataQueryPlan(generator, new UDAFPlan());
      ((UDAFPlan) (physicalPlan)).setInnerAggregationPlan(innerAggregationPlan);
      ((UDAFPlan) (physicalPlan))
          .setExpressionToInnerResultIndexMap(this.expressionToInnerResultIndexMap);
    } else {
      // todo: align by device
      physicalPlan = new AggregationPlan();
    }
    return physicalPlan;
  }

  private AggregationPlan initInnerAggregationPlan(PhysicalGenerator generator, QueryPlan queryPlan)
      throws QueryProcessException {
    AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
    aggregationPlan.setAggregations(getInnerAggregationsCache());
    aggregationPlan.setResultColumns(getInnerResultColumnsCache());
    aggregationPlan.setPaths(getInnerPathCathe());
    aggregationPlan.setEnableTracing(enableTracing);
    // transform filter operator to expression
    if (whereComponent != null) {
      transformFilterOperatorToExpression(generator, aggregationPlan);
    }
    if (isGroupByLevel()) {
      super.initGroupByLevel(aggregationPlan);
    }
    try {
      queryPlan.deduplicate(generator);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    convertSpecialClauseValues(aggregationPlan);
    return aggregationPlan;
  }

  private void checkEachExpression(Expression expression) throws LogicalOperatorException {
    if (expression instanceof BinaryExpression) {
      checkEachExpression(((BinaryExpression) expression).getLeftExpression());
      checkEachExpression(((BinaryExpression) expression).getRightExpression());
      return;
    }
    if (expression instanceof TimeSeriesOperand) {
      throw new LogicalOperatorException(ERROR_MESSAGE1);
    }
    // Currently, the aggregation function expression can only contain a timeseries operand.
    if (expression instanceof FunctionExpression
        && (((FunctionExpression) expression).getExpressions().size() != 1
            || !(((FunctionExpression) expression).getExpressions().get(0)
                instanceof TimeSeriesOperand))) {
      throw new LogicalOperatorException(
          "The argument of the aggregation function must be a time series.");
    }
  }
}
