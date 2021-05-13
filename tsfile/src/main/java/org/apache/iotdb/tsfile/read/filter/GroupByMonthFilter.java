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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.Calendar;
import java.util.Objects;

/**
 * GroupByMonthFilter is used to handle natural month slidingStep and interval by generating
 * dynamically. Attention: it's only supported to access in ascending order now.
 */
public class GroupByMonthFilter extends GroupByFilter {

  private final boolean isSlidingStepByMonth;
  private final boolean isIntervalByMonth;
  private int slidingStepsInMo;
  private int intervalInMo;
  private Calendar calendar = Calendar.getInstance();
  private static final long MS_TO_MONTH = 30 * 86400_000L;
  private int intervalCnt = 0;
  /** 10.31 -> 11.30 -> 12.31, not 10.31 -> 11.30 -> 12.30 */
  private final long initialStartTime;

  public GroupByMonthFilter(
      long interval,
      long slidingStep,
      long startTime,
      long endTime,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth) {
    super(interval, slidingStep, startTime, endTime);
    initialStartTime = startTime;
    calendar.setTimeInMillis(startTime);
    this.isIntervalByMonth = isIntervalByMonth;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    if (isIntervalByMonth) {
      // TODO: 1mo1d
      intervalInMo = (int) (interval / MS_TO_MONTH);
    }
    if (isSlidingStepByMonth) {
      slidingStepsInMo = (int) (slidingStep / MS_TO_MONTH);
    }
    getNextIntervalAndSlidingStep();
  }

  public GroupByMonthFilter(GroupByMonthFilter filter) {
    super(filter.interval, filter.slidingStep, filter.startTime, filter.endTime);
    isIntervalByMonth = filter.isIntervalByMonth;
    isSlidingStepByMonth = filter.isSlidingStepByMonth;
    intervalInMo = filter.intervalInMo;
    slidingStepsInMo = filter.slidingStepsInMo;
    intervalCnt = filter.intervalCnt;
    initialStartTime = filter.initialStartTime;
    calendar = Calendar.getInstance();
    calendar.setTimeInMillis(filter.calendar.getTimeInMillis());
  }

  // TODO: time descending order
  @Override
  public boolean satisfy(long time, Object value) {
    if (time < startTime || time >= endTime) {
      return false;
    } else if (time - startTime < interval) {
      return true;
    } else {
      this.startTime = calendar.getTimeInMillis();
      getNextIntervalAndSlidingStep();
      return satisfy(time, value);
    }
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    boolean isSatisfy = satisfyCurrentInterval(startTime, endTime);
    if (isSatisfy) {
      return true;
    } else {
      long beforeStartTime = this.startTime;
      int beforeIntervalCnt = this.intervalCnt;
      // TODO: optimize to jump but not one by one
      while (endTime >= this.startTime && !isSatisfy) {
        this.startTime = calendar.getTimeInMillis();
        getNextIntervalAndSlidingStep();
        isSatisfy = satisfyCurrentInterval(startTime, endTime);
      }
      // recover the initial state
      this.intervalCnt = beforeIntervalCnt - 1;
      this.startTime = beforeStartTime;
      getNextIntervalAndSlidingStep();
      return isSatisfy;
    }
  }

  @Override
  public Filter copy() {
    return new GroupByMonthFilter(this);
  }

  private boolean satisfyCurrentInterval(long startTime, long endTime) {
    if (endTime < this.startTime || startTime >= this.endTime) {
      return false;
    } else {
      return startTime <= this.startTime || startTime - this.startTime < interval;
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    boolean isContained = isContainedByCurrentInterval(startTime, endTime);
    if (isContained) {
      return true;
    } else {
      long beforeStartTime = this.startTime;
      int beforeIntervalCnt = this.intervalCnt;
      while (!isContained && startTime >= this.startTime) {
        this.startTime = calendar.getTimeInMillis();
        getNextIntervalAndSlidingStep();
        isContained = isContainedByCurrentInterval(startTime, endTime);
      }
      // recover the initial state
      this.intervalCnt = beforeIntervalCnt - 1;
      this.startTime = beforeStartTime;
      getNextIntervalAndSlidingStep();
      return isContained;
    }
  }

  private boolean isContainedByCurrentInterval(long startTime, long endTime) {
    if (startTime < this.startTime || endTime > this.endTime) {
      return false;
    } else {
      return startTime - this.startTime < interval && endTime - this.startTime < interval;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByMonthFilter)) {
      return false;
    }
    GroupByMonthFilter other = (GroupByMonthFilter) obj;
    return this.interval == other.interval
        && this.slidingStep == other.slidingStep
        && this.startTime == other.startTime
        && this.endTime == other.endTime
        && this.isSlidingStepByMonth == other.isSlidingStepByMonth
        && this.isIntervalByMonth == other.isIntervalByMonth;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        interval, slidingStep, startTime, endTime, isSlidingStepByMonth, isIntervalByMonth);
  }

  private void getNextIntervalAndSlidingStep() {
    intervalCnt++;
    if (isIntervalByMonth) {
      calendar.setTimeInMillis(initialStartTime);
      calendar.add(Calendar.MONTH, slidingStepsInMo * (intervalCnt - 1) + intervalInMo);
      this.interval = calendar.getTimeInMillis() - startTime;
    }
    if (isSlidingStepByMonth) {
      calendar.setTimeInMillis(initialStartTime);
      calendar.add(Calendar.MONTH, slidingStepsInMo * intervalCnt);
      this.slidingStep = calendar.getTimeInMillis() - startTime;
    }
  }
}
