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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.read.expression.IExpression;

public class GroupByFillQueryOperator extends GroupByQueryOperator {

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    PhysicalPlan plan =
        isAlignByDevice()
            ? this.generateAlignByDevicePlan(generator)
            : super.generateRawDataQueryPlan(
                generator, initGroupByTimeFillPlan(new GroupByTimeFillPlan()));

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
    alignByDevicePlan.setGroupByFillPlan(initGroupByTimeFillPlan(new GroupByTimeFillPlan()));
    return alignByDevicePlan;
  }

  protected GroupByTimeFillPlan initGroupByTimeFillPlan(QueryPlan queryPlan)
      throws QueryProcessException {
    GroupByTimeFillPlan groupByTimeFillPlan =
        (GroupByTimeFillPlan) super.initGroupByTimePlan(queryPlan);
    GroupByFillClauseComponent groupByFillClauseComponent =
        (GroupByFillClauseComponent) specialClauseComponent;
    groupByTimeFillPlan.setSingleFill(groupByFillClauseComponent.getSingleFill());
    // old type fill logic
    groupByTimeFillPlan.setFillType(groupByFillClauseComponent.getFillTypes());

    return groupByTimeFillPlan;
  }

  @Override
  protected IExpression optimizeExpression(IExpression expression, RawDataQueryPlan queryPlan)
      throws QueryProcessException {
    GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) queryPlan;
    groupByFillPlan.initFillRange();
    return super.optimizeExpression(expression, queryPlan);
  }
}
