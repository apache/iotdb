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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter;

import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;

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
  private TimeDuration interval;

  // sliding step
  private TimeDuration slidingStep;

  // if it is left close and right open interval
  private boolean leftCRightO;

  public GroupByTimeParameter() {}

  public GroupByTimeParameter(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean leftCRightO) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.leftCRightO = leftCRightO;
  }

  public GroupByTimeParameter(GroupByTimeComponent groupByTimeComponent) {
    this.startTime = groupByTimeComponent.getStartTime();
    this.endTime = groupByTimeComponent.getEndTime();
    this.interval = groupByTimeComponent.getInterval();
    this.slidingStep = groupByTimeComponent.getSlidingStep();
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

  public TimeDuration getInterval() {
    return interval;
  }

  public void setInterval(TimeDuration interval) {
    this.interval = interval;
  }

  public TimeDuration getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(TimeDuration slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public boolean hasOverlap() {
    return interval.getTotalDuration(TimestampPrecisionUtils.currPrecision)
        > slidingStep.getTotalDuration(TimestampPrecisionUtils.currPrecision);
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(startTime, buffer);
    ReadWriteIOUtils.write(endTime, buffer);
    interval.serialize(buffer);
    slidingStep.serialize(buffer);
    ReadWriteIOUtils.write(leftCRightO, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(startTime, stream);
    ReadWriteIOUtils.write(endTime, stream);
    interval.serialize(stream);
    slidingStep.serialize(stream);
    ReadWriteIOUtils.write(leftCRightO, stream);
  }

  public static GroupByTimeParameter deserialize(ByteBuffer buffer) {
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter();
    groupByTimeParameter.setStartTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setEndTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeParameter.setInterval(TimeDuration.deserialize(buffer));
    groupByTimeParameter.setSlidingStep(TimeDuration.deserialize(buffer));
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
        && this.interval.equals(other.interval)
        && this.slidingStep.equals(other.slidingStep)
        && this.leftCRightO == other.leftCRightO;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime, interval, slidingStep, leftCRightO);
  }
}
