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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Objects;

/** DataNode-specific operator context. */
public class OperatorContext extends CommonOperatorContext {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OperatorContext.class);
  private DriverContext driverContext;

  public OperatorContext(
      int operatorId, PlanNodeId planNodeId, String operatorType, DriverContext driverContext) {
    super(operatorId, planNodeId, operatorType);
    this.driverContext = driverContext;
  }

  @TestOnly
  public OperatorContext(
      int operatorId,
      PlanNodeId planNodeId,
      String operatorType,
      FragmentInstanceContext fragmentInstanceContext) {
    super(operatorId, planNodeId, operatorType);
    this.driverContext = new DriverContext(fragmentInstanceContext, 0);
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public void setDriverContext(DriverContext driverContext) {
    this.driverContext = driverContext;
  }

  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public SessionInfo getSessionInfo() {
    return getInstanceContext().getSessionInfo();
  }

  @Override
  public MemoryReservationManager getMemoryReservationContext() {
    return getInstanceContext().getMemoryReservationContext();
  }

  @Override
  public int getFragmentId() {
    return getInstanceContext().getId().getFragmentId().getId();
  }

  @Override
  public int getPipelineId() {
    return driverContext.getPipelineId();
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

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(operatorType)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(planNodeId);
  }
}
