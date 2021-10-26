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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class AggregationQueryOperator extends QueryOperator {

  public static final String ERROR_MESSAGE1 =
      "Common queries and aggregated queries are not allowed to appear at the same time";

  public AggregationQueryOperator() {
    super();
  }

  public AggregationQueryOperator(QueryOperator queryOperator) {
    super(queryOperator);
  }

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    if (!isAlignByTime()) {
      throw new LogicalOperatorException("AGGREGATION doesn't support disable align clause.");
    }

    if (hasTimeSeriesGeneratingFunction()) {
      throw new LogicalOperatorException(
          "User-defined and built-in hybrid aggregation is not supported together.");
    }

    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
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

    if (isGroupByLevel() && isAlignByDevice()) {
      throw new LogicalOperatorException("group by level does not support align by device now.");
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : super.generateRawDataQueryPlan(generator, initAggregationPlan(new AggregationPlan()));
  }

  private boolean verifyAllAggregationDataTypesEqual() throws MetadataException {
    List<String> aggregations = selectComponent.getAggregationFunctions();
    if (aggregations.isEmpty()) {
      return true;
    }

    List<PartialPath> paths = selectComponent.getPaths();
    List<TSDataType> dataTypes = SchemaUtils.getSeriesTypesByPaths(paths);
    String aggType = aggregations.get(0);
    switch (aggType) {
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return dataTypes.stream().allMatch(dataTypes.get(0)::equals);
      default:
        return true;
    }
  }

  @Override
  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan alignByDevicePlan = super.generateAlignByDevicePlan(generator);
    alignByDevicePlan.setAggregationPlan(initAggregationPlan(new AggregationPlan()));

    return alignByDevicePlan;
  }

  protected AggregationPlan initAggregationPlan(QueryPlan queryPlan) throws QueryProcessException {
    AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
    aggregationPlan.setAggregations(selectComponent.getAggregationFunctions());
    if (isGroupByLevel()) {
      aggregationPlan.setLevels(specialClauseComponent.getLevels());
      aggregationPlan.setGroupByLevelController(specialClauseComponent.groupByLevelController);
      try {
        if (!verifyAllAggregationDataTypesEqual()) {
          throw new LogicalOperatorException("Aggregate among unmatched data types");
        }
      } catch (MetadataException e) {
        throw new LogicalOperatorException(e);
      }
    }
    return aggregationPlan;
  }
}
