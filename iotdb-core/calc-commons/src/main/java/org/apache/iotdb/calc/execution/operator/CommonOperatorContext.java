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

package org.apache.iotdb.calc.execution.operator;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.TestOnly;

import io.airlift.units.Duration;
import org.apache.tsfile.utils.Accountable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Contains common information about {@link Operator} execution.
 *
 * <p>Not thread-safe.
 */
public abstract class CommonOperatorContext implements Accountable {

  protected static Duration maxRunTime =
      new Duration(
          CommonDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs(),
          TimeUnit.MILLISECONDS);

  protected final int operatorId;
  // It seems it's never used.
  protected final PlanNodeId planNodeId;
  protected String operatorType;

  protected long totalExecutionTimeInNanos = 0L;
  protected long nextCalledCount = 0L;
  protected long hasNextCalledCount = 0L;

  // SpecifiedInfo is used to record some custom information for the operator,
  // which will be shown in the result of EXPLAIN ANALYZE to analyze the query.
  protected final Map<String, Object> specifiedInfo = new ConcurrentHashMap<>();
  protected long output = 0;
  protected long estimatedMemorySize;

  protected CommonOperatorContext(int operatorId, PlanNodeId planNodeId, String operatorType) {
    this.operatorId = operatorId;
    this.planNodeId = planNodeId;
    this.operatorType = operatorType;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  public void setOperatorType(String operatorType) {
    this.operatorType = operatorType;
  }

  public static Duration getMaxRunTime() {
    return maxRunTime;
  }

  @TestOnly
  public Duration getMaxRunTimeForTest() {
    return maxRunTime;
  }

  public static void setMaxRunTime(Duration maxRunTime) {
    CommonOperatorContext.maxRunTime = maxRunTime;
  }

  public PlanNodeId getPlanNodeId() {
    return planNodeId;
  }

  public abstract MemoryReservationManager getMemoryReservationContext();

  public abstract int getFragmentId();

  public abstract int getPipelineId();

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
    specifiedInfo.put(key, value);
  }

  public Map<String, Object> getSpecifiedInfo() {
    return specifiedInfo;
  }
}
