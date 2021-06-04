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
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class GroupByFillQueryOperator extends GroupByQueryOperator {

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    for (String aggregation : selectComponent.getAggregationFunctions()) {
      if (!SQLConstant.LAST_VALUE.equals(aggregation)) {
        throw new LogicalOperatorException("Group By Fill only support last_value function");
      }
    }
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize, PhysicalGenerator generator)
      throws QueryProcessException {
    GroupByTimeFillPlan queryPlan = new GroupByTimeFillPlan();

    GroupByFillClauseComponent groupByFillClauseComponent =
        (GroupByFillClauseComponent) specialClauseComponent;
    queryPlan.setFillType(groupByFillClauseComponent.getFillTypes());

    return queryPlan;
  }
}
