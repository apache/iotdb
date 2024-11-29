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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import io.airlift.units.Duration;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Contains information about {@link Operator} execution.
 *
 * <p>Not thread-safe.
 */
public class OperatorContext implements Accountable {

  private static Duration maxRunTime =
      new Duration(
          IoTDBDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs(),
          TimeUnit.MILLISECONDS);

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OperatorContext.class);

  private final int operatorId;
  // It seems it's never used.
  private final PlanNodeId planNodeId;
  private final String operatorType;
  private DriverContext driverContext;

  private long totalExecutionTimeInNanos = 0L;
  private long nextCalledCount = 0L;
  private long hasNextCalledCount = 0L;

  // SpecifiedInfo is used to record some custom information for the operator,
  // which will be shown in the result of EXPLAIN ANALYZE to analyze the query.
  private Map<String, String> specifiedInfo = null;
  private long output = 0;
  private long estimatedMemorySize;

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

  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public Duration getMaxRunTime() {
    return maxRunTime;
  }

  public static void setMaxRunTime(Duration maxRunTime) {
    OperatorContext.maxRunTime = maxRunTime;
  }

  public SessionInfo getSessionInfo() {
    return getInstanceContext().getSessionInfo();
  }

  public PlanNodeId getPlanNodeId() {
    return planNodeId;
  }

  public void recordExecutionTime(long executionTimeInNanos) {
    this.totalExecutionTimeInNanos += executionTimeInNanos;
  }

  public void recordNextCalled() {
    this.nextCalledCount++;
  }

  public void recordHasNextCalled() {
    this.hasNextCalledCount++;
  }

  public long getTotalExecutionTimeInNanos() {
    return totalExecutionTimeInNanos;
  }

  public long getNextCalledCount() {
    return nextCalledCount;
  }

  public long getHasNextCalledCount() {
    return hasNextCalledCount;
  }

  public void setEstimatedMemorySize(long estimatedMemorySize) {
    this.estimatedMemorySize = estimatedMemorySize;
  }

  public long getEstimatedMemorySize() {
    return estimatedMemorySize;
  }

  public void addOutputRows(long outputRows) {
    this.output += outputRows;
  }

  public long getOutputRows() {
    return output;
  }

  public void recordSpecifiedInfo(String key, String value) {
    if (specifiedInfo == null) {
      // explain analyze operator fetching and current operator updating may be concurrently
      specifiedInfo = new ConcurrentHashMap<>();
    }
    specifiedInfo.put(key, value);
  }

  public Map<String, String> getSpecifiedInfo() {
    return specifiedInfo == null ? new HashMap<>() : specifiedInfo;
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
