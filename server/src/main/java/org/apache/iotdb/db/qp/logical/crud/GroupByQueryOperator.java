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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan.getTimeExpression;

public class GroupByQueryOperator extends AggregationQueryOperator {

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    PhysicalPlan plan =
        isAlignByDevice()
            ? this.generateAlignByDevicePlan(generator)
            : super.generateRawDataQueryPlan(generator, initGroupByTimePlan(new GroupByTimePlan()));

    AggregationPlan aggregationPlan =
        isAlignByDevice()
            ? ((AlignByDevicePlan) plan).getAggregationPlan()
            : (AggregationPlan) plan;
    aggregationPlan.verifyAllAggregationDataTypesMatched();

    return plan;
  }

  @Override
  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan alignByDevicePlan = super.generateAlignByDevicePlan(generator);
    alignByDevicePlan.setGroupByTimePlan(initGroupByTimePlan(new GroupByTimePlan()));
    return alignByDevicePlan;
  }

  protected GroupByTimePlan initGroupByTimePlan(QueryPlan queryPlan) throws QueryProcessException {
    GroupByTimePlan groupByTimePlan = (GroupByTimePlan) initAggregationPlan(queryPlan);
    GroupByClauseComponent groupByClauseComponent = (GroupByClauseComponent) specialClauseComponent;

    groupByTimePlan.setInterval(groupByClauseComponent.getUnit());
    groupByTimePlan.setIntervalByMonth(groupByClauseComponent.isIntervalByMonth());
    groupByTimePlan.setSlidingStep(groupByClauseComponent.getSlidingStep());
    groupByTimePlan.setSlidingStepByMonth(groupByClauseComponent.isSlidingStepByMonth());
    groupByTimePlan.setLeftCRightO(groupByClauseComponent.isLeftCRightO());

    if (!groupByClauseComponent.isLeftCRightO()) {
      groupByTimePlan.setStartTime(groupByClauseComponent.getStartTime() + 1);
      groupByTimePlan.setEndTime(groupByClauseComponent.getEndTime() + 1);
    } else {
      groupByTimePlan.setStartTime(groupByClauseComponent.getStartTime());
      groupByTimePlan.setEndTime(groupByClauseComponent.getEndTime());
    }

    return groupByTimePlan;
  }

  @Override
  protected IExpression optimizeExpression(IExpression expression, RawDataQueryPlan queryPlan)
      throws QueryProcessException {
    GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
    List<PartialPath> selectedSeries = groupByTimePlan.getDeduplicatedPaths();
    GlobalTimeExpression timeExpression = getTimeExpression(groupByTimePlan);

    if (expression == null) {
      expression = timeExpression;
    } else {
      expression = BinaryExpression.and(expression, timeExpression);
    }

    // optimize expression to an executable one
    try {
      return ExpressionOptimizer.getInstance()
          .optimize(expression, new ArrayList<>(selectedSeries));
    } catch (QueryFilterOptimizationException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }
}
