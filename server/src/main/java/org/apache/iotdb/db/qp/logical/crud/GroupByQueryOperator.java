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
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class GroupByQueryOperator extends QueryOperator {

  // time interval
  private long unit;
  // sliding step
  private long slidingStep;
  // if it is left close and right open interval
  private boolean leftCRightO;


  public GroupByQueryOperator() {
    super(SQLConstant.TOK_QUERY);
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize, PhysicalGenerator generator)
      throws QueryProcessException {

    GroupByTimePlan plan = new GroupByTimePlan();
    setPlanValues(plan);

    return doOptimization(plan, generator, fetchSize);
  }

  protected void setPlanValues(GroupByTimePlan plan) {
    super.setPlanValues(plan);
    plan.setInterval(getUnit());
    plan.setIntervalByMonth(isIntervalByMonth());
    plan.setSlidingStep(getSlidingStep());
    plan.setSlidingStepByMonth(isSlidingStepByMonth());
    plan.setLeftCRightO(isLeftCRightO());
    if (!isLeftCRightO()) {
      plan.setStartTime(getStartTime() + 1);
      plan.setEndTime(getEndTime() + 1);
    } else {
      plan.setStartTime(getStartTime());
      plan.setEndTime(getEndTime());
    }
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public void setUnit(long unit) {
    this.unit = unit;
  }

  public long getUnit() {
    return unit;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public long getSlidingStep() {
    return slidingStep;
  }
}
