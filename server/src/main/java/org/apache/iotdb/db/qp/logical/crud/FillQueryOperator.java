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
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class FillQueryOperator extends QueryOperator {

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    if (!isAlignByTime()) {
      throw new LogicalOperatorException("FILL doesn't support disable align clause.");
    }

    if (hasTimeSeriesGeneratingFunction()) {
      throw new LogicalOperatorException("Fill functions are not supported in UDF queries.");
    }

    FilterOperator filterOperator = whereComponent.getFilterOperator();
    if (!filterOperator.isLeaf() || filterOperator.getFilterType() != FilterType.EQUAL) {
      throw new LogicalOperatorException("Only \"=\" can be used in fill function");
    } else if (!filterOperator.isSingle()) {
      throw new LogicalOperatorException("Slice query must select a single time point");
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : super.generateRawDataQueryPlan(generator, initFillQueryPlan());
  }

  @Override
  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan alignByDevicePlan = super.generateAlignByDevicePlan(generator);
    alignByDevicePlan.setFillQueryPlan(initFillQueryPlan());

    return alignByDevicePlan;
  }

  private FillQueryPlan initFillQueryPlan() {
    FillQueryPlan fillQueryPlan = new FillQueryPlan();
    FilterOperator timeFilter = whereComponent.getFilterOperator();
    long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
    fillQueryPlan.setQueryTime(time);
    fillQueryPlan.setFillType(((FillClauseComponent) specialClauseComponent).getFillTypes());
    return fillQueryPlan;
  }
}
