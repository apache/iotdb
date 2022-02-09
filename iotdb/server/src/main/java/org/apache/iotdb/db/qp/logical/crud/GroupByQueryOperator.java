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
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class GroupByQueryOperator extends AggregationQueryOperator {

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : super.generateRawDataQueryPlan(generator, initGroupByTimePlan(new GroupByTimePlan()));
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
}
