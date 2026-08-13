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
import org.apache.iotdb.calc.utils.sort.TempDiskSpillQuotaGate;
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

  /**
   * Immutable userId of the session owning this operator, used for per-user resource quota
   * accounting (e.g. TEMP_DISK spill). calc-commons operators can run in tests or standalone
   * contexts without a session; the default of -1 disables quota accounting. DataNode's
   * OperatorContext overrides this with the real session userId.
   */
  public long getSessionUserId() {
    return -1;
  }

  /**
   * TEMP_DISK spill quota gate for sort operators; null disables accounting. DataNode {@link
   * org.apache.iotdb.db.queryengine.execution.operator.OperatorContext} overrides this when user
   * resource quota is enabled.
   */
  public TempDiskSpillQuotaGate getTempDiskSpillQuotaGate() {
    return null;
  }

  public void recordExecutionTime(long executionTimeInNanos) {
    this.totalExecutionTimeInNanos += executionTimeInNanos;
  }

  public void recordScanAggregationFromRawDataCost(long costTimeInNanos) {
    // calc-commons operators can run in tests or standalone contexts that are not backed by a
    // DataNode FragmentInstanceContext. DataNode OperatorContext overrides this to forward costs.
  }

  public void recordScanAggregationFromStatisticsCost(long costTimeInNanos) {
    // calc-commons operators can run in tests or standalone contexts that are not backed by a
    // DataNode FragmentInstanceContext. DataNode OperatorContext overrides this to forward costs.
  }

  public void recordAggregationOperatorFromRawDataCost(long costTimeInNanos) {
    // calc-commons operators can run in tests or standalone contexts that are not backed by a
    // DataNode FragmentInstanceContext. DataNode OperatorContext overrides this to forward costs.
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
