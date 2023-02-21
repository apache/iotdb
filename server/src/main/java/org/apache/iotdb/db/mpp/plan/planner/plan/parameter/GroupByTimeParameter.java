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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The parameter of `GROUP BY TIME`.
 *
 * <p>Remember: interval and slidingStep is always in timestamp unit before transforming to
 * timeRangeIterator even if it's by month unit.
 */
public class GroupByTimeParameter {

  // [startTime, endTime)
  private long startTime;
  private long endTime;

  // time interval
  private long interval;

  // sliding step
  private long slidingStep;

  // if it is expressed by natural month. eg, 1mo
  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;

  // if it is left close and right open interval
  private boolean leftCRightO;

  public GroupByTimeParameter() {}

  public GroupByTimeParameter(
      long startTime, long endTime, long interval, long slidingStep, boolean leftCRightO) {
    this(startTime, endTime, interval, slidingStep, false, false, leftCRightO);
  }

  public GroupByTimeParameter(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isIntervalByMonth,
      boolean isSlidingStepByMonth,
      boolean leftCRightO) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isIntervalByMonth = isIntervalByMonth;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    this.leftCRightO = leftCRightO;
  }

  public GroupByTimeParameter(GroupByTimeComponent groupByTimeComponent) {
    this.startTime = groupByTimeComponent.getStartTime();
    this.endTime = groupByTimeComponent.getEndTime();
    this.interval = groupByTimeComponent.getInterval();
    this.slidingStep = groupByTimeComponent.getSlidingStep();
    this.isIntervalByMonth = groupByTimeComponent.isIntervalByMonth();
    this.isSlidingStepByMonth = groupByTimeComponent.isSlidingStepByMonth();
    this.leftCRightO = groupByTimeComponent.isLeftCRightO();
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getInterval() {
    return interval;
  }

  public void setInterval(long interval) {
    this.interval = interval;
  }

  public long getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean intervalByMonth) {
    isIntervalByMonth = intervalByMonth;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public void setSlidingStepByMonth(boolean slidingStepByMonth) {
    isSlidingStepByMonth = slidingStepByMonth;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public boolean hasOverlap() {
    return interval > slidingStep;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(startTime, buffer);
    ReadWriteIOUtils.write(endTime, buffer);
    ReadWriteIOUtils.write(interval, buffer);
    ReadWriteIOUtils.write(slidingStep, buffer);
    ReadWriteIOUtils.write(isIntervalByMonth, buffer);
    ReadWriteIOUtils.write(isSlidingStepByMonth, buffer);
    ReadWriteIOUtils.write(leftCRightO, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(startTime, stream);
    ReadWriteIOUtils.write(endTime, stream);
    ReadWriteIOUtils.write(interval, stream);
    ReadWriteIOUtils.write(slidingStep, stream);
    ReadWriteIOUtils.write(isIntervalByMonth, stream);
    ReadWriteIOUtils.write(isSlidingStepByMonth, stream);
    ReadWriteIOUtils.write(leftCRightO, stream);
  }

  public static GroupByTimeParameter deserialize(ByteBuffer buffer) {
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter();
    groupByTimeParameter.setStartTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setEndTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setInterval(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setSlidingStep(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setIntervalByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimeParameter.setSlidingStepByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimeParameter.setLeftCRightO(ReadWriteIOUtils.readBool(buffer));
    return groupByTimeParameter;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByTimeParameter)) {
      return false;
    }
    GroupByTimeParameter other = (GroupByTimeParameter) obj;
    return this.startTime == other.startTime
        && this.endTime == other.endTime
        && this.interval == other.interval
        && this.slidingStep == other.slidingStep
        && this.isSlidingStepByMonth == other.isSlidingStepByMonth
        && this.isIntervalByMonth == other.isIntervalByMonth
        && this.leftCRightO == other.leftCRightO;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        startTime,
        endTime,
        interval,
        slidingStep,
        isIntervalByMonth,
        isSlidingStepByMonth,
        leftCRightO);
  }
}
