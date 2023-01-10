/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import io.airlift.units.Duration;

import java.util.Objects;

/**
 * Contains information about {@link Operator} execution.
 *
 * <p>Not thread-safe.
 */
public class OperatorContext {

  private final int operatorId;
  // It seems it's never used.
  private final PlanNodeId planNodeId;
  private final String operatorType;
  private DriverContext driverContext;
  private Duration maxRunTime;

  private long totalExecutionTimeInNanos = 0L;
  private long nextCalledCount = 0L;

  public OperatorContext(
      int operatorId, PlanNodeId planNodeId, String operatorType, DriverContext driverContext) {
    this.operatorId = operatorId;
    this.planNodeId = planNodeId;
    this.operatorType = operatorType;
    this.driverContext = driverContext;
  }

  @TestOnly
  public OperatorContext(
      int operatorId,
      PlanNodeId planNodeId,
      String operatorType,
      FragmentInstanceContext fragmentInstanceContext) {
    this.operatorId = operatorId;
    this.planNodeId = planNodeId;
    this.operatorType = operatorType;
    this.driverContext = new DriverContext(fragmentInstanceContext, 0);
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public void setDriverContext(DriverContext driverContext) {
    this.driverContext = driverContext;
  }

  // TODO forbid get instance context from operator directly
  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public Duration getMaxRunTime() {
    return maxRunTime;
  }

  public void setMaxRunTime(Duration maxRunTime) {
    this.maxRunTime = maxRunTime;
  }

  public SessionInfo getSessionInfo() {
    return getInstanceContext().getSessionInfo();
  }

  public void recordExecutionTime(long executionTimeInNanos) {
    this.totalExecutionTimeInNanos += executionTimeInNanos;
  }

  public void recordNextCalled() {
    this.nextCalledCount++;
  }

  public long getTotalExecutionTimeInNanos() {
    return totalExecutionTimeInNanos;
  }

  public long getNextCalledCount() {
    return nextCalledCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperatorContext that = (OperatorContext) o;
    return operatorId == that.operatorId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(operatorId);
  }
}
