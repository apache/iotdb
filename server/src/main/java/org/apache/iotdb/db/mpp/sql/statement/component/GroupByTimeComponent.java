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

package org.apache.iotdb.db.mpp.sql.statement.component;

import org.apache.iotdb.db.mpp.sql.statement.StatementNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

/** This class maintains information of {@code GROUP BY} clause. */
public class GroupByTimeComponent extends StatementNode {

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

  public GroupByTimeComponent() {}

  public GroupByTimeComponent(
      long startTime, long endTime, long interval, long slidingStep, boolean leftCRightO) {
    this(startTime, endTime, interval, slidingStep, false, false, leftCRightO);
  }

  public GroupByTimeComponent(
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

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public long getInterval() {
    return interval;
  }

  public void setInterval(long interval) {
    this.interval = interval;
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

  public long getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public void setSlidingStepByMonth(boolean isSlidingStepByMonth) {
    this.isSlidingStepByMonth = isSlidingStepByMonth;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean isIntervalByMonth) {
    this.isIntervalByMonth = isIntervalByMonth;
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

  public static GroupByTimeComponent deserialize(ByteBuffer buffer) {
    GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();
    groupByTimeComponent.setStartTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeComponent.setEndTime(ReadWriteIOUtils.readLong(buffer));
    groupByTimeComponent.setInterval(ReadWriteIOUtils.readLong(buffer));
    groupByTimeComponent.setSlidingStep(ReadWriteIOUtils.readLong(buffer));
    groupByTimeComponent.setIntervalByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimeComponent.setSlidingStepByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimeComponent.setLeftCRightO(ReadWriteIOUtils.readBool(buffer));
    return groupByTimeComponent;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByTimeComponent)) {
      return false;
    }
    GroupByTimeComponent other = (GroupByTimeComponent) obj;
    return this.startTime == other.startTime
        && this.endTime == other.endTime
        && this.interval == other.interval
        && this.slidingStep == other.slidingStep
        && this.isSlidingStepByMonth == other.isSlidingStepByMonth
        && this.isIntervalByMonth == other.isIntervalByMonth
        && this.leftCRightO == other.leftCRightO;
  }

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
