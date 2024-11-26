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

package org.apache.tsfile.read.filter.operator;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.tsfile.utils.TimeDuration.calcPositiveIntervalByMonth;
import static org.apache.tsfile.utils.TimeDuration.getConsecutiveTimesIntervalByMonth;

/**
 * GroupByMonthFilter is used to handle natural month slidingStep and interval by generating
 * dynamically. Attention: it's only supported to access in ascending order now.
 */
public class GroupByMonthFilter extends GroupByFilter {
  private final Calendar calendar = Calendar.getInstance();

  private final TimeDuration originalSlidingStep;
  private final TimeDuration originalInterval;

  // These fields will be serialized to remote nodes, as other fields may be updated during process
  private TimeZone timeZone;
  private final long originalStartTime;
  private final long originalEndTime;

  private final TimeUnit currPrecision;

  private long[] startTimes;

  public GroupByMonthFilter(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      TimeZone timeZone,
      TimeUnit currPrecision) {
    super(startTime, endTime);
    this.originalStartTime = startTime;
    this.originalEndTime = endTime;
    this.originalInterval = interval;
    this.originalSlidingStep = slidingStep;
    if (!interval.containsMonth()) {
      this.interval = interval.nonMonthDuration;
    }
    if (!slidingStep.containsMonth()) {
      this.slidingStep = slidingStep.nonMonthDuration;
    }
    this.currPrecision = currPrecision;
    initMonthGroupByParameters(timeZone);
  }

  public GroupByMonthFilter(ByteBuffer buffer) {
    this(
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        TimeDuration.deserialize(buffer),
        TimeDuration.deserialize(buffer),
        TimeZone.getTimeZone(ReadWriteIOUtils.readString(buffer)),
        TimeUnit.values()[ReadWriteIOUtils.readInt(buffer)]);
  }

  @Override
  public boolean timeSatisfy(long time) {
    if (time < originalStartTime || time >= endTime) {
      return false;
    } else if (time >= startTime && time < startTime + slidingStep) {
      return time - startTime < interval;
    } else {
      int count = getTimePointPosition(time);
      getNthTimeInterval(count);
      return time - startTime < interval;
    }
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (satisfyCurrentInterval(startTime, endTime)) {
      return true;
    } else {
      // get the interval which contains the start time
      int count = getTimePointPosition(startTime);
      getNthTimeInterval(count);
      // judge two adjacent intervals
      if (satisfyCurrentInterval(startTime, endTime)) {
        return true;
      } else {
        getNthTimeInterval(count + 1);
        return satisfyCurrentInterval(startTime, endTime);
      }
    }
  }

  private boolean satisfyCurrentInterval(long startTime, long endTime) {
    if (endTime < this.startTime || startTime >= this.endTime) {
      return false;
    } else {
      return startTime - this.startTime < interval;
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (isContainedByCurrentInterval(startTime, endTime)) {
      return true;
    } else {
      // get the interval which contains the start time
      int count = getTimePointPosition(startTime);
      getNthTimeInterval(count);
      // judge single interval that contains start time
      return isContainedByCurrentInterval(startTime, endTime);
    }
  }

  private boolean isContainedByCurrentInterval(long startTime, long endTime) {
    if (startTime < this.startTime || endTime > this.endTime) {
      return false;
    } else {
      return startTime - this.startTime < interval && endTime - this.startTime < interval;
    }
  }

  private void initMonthGroupByParameters(TimeZone timeZone) {
    calendar.setTimeZone(timeZone);
    this.timeZone = timeZone;
    calendar.setTimeInMillis(startTime);
    if (originalSlidingStep.containsMonth()) {
      startTimes =
          getConsecutiveTimesIntervalByMonth(
              startTime,
              originalSlidingStep,
              (int)
                  Math.ceil(
                      ((originalEndTime - originalStartTime)
                          / (double) originalSlidingStep.getMinTotalDuration(currPrecision))),
              timeZone,
              currPrecision);
    }
    getNthTimeInterval(0);
  }

  /** Get the interval that @param time belongs to. */
  private int getTimePointPosition(long time) {
    if (originalSlidingStep.containsMonth()) {
      int searchResult = Arrays.binarySearch(startTimes, time);
      return searchResult >= 0 ? searchResult : Math.max(0, Math.abs(searchResult) - 2);
    } else {
      return (int) ((time - originalStartTime) / slidingStep);
    }
  }

  /** get the Nth time interval. */
  private void getNthTimeInterval(int n) {
    // get interval and sliding step
    if (originalSlidingStep.containsMonth()) {
      if (n < 0 || n > startTimes.length - 1) {
        this.interval = -1;
        return;
      }
      this.startTime = startTimes[n];
      this.slidingStep =
          calcPositiveIntervalByMonth(
                  originalStartTime, originalSlidingStep.multiple(n + 1), timeZone, currPrecision)
              - startTime;
    } else {
      this.startTime = originalStartTime + n * slidingStep;
    }
    if (originalInterval.containsMonth()) {
      this.interval =
          calcPositiveIntervalByMonth(
                  originalStartTime,
                  originalSlidingStep.multiple(n).merge(originalInterval),
                  timeZone,
                  currPrecision)
              - startTime;
    }
  }

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.GROUP_BY_MONTH;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getOperatorType().ordinal(), outputStream);
    ReadWriteIOUtils.write(originalStartTime, outputStream);
    ReadWriteIOUtils.write(originalEndTime, outputStream);
    originalInterval.serialize(outputStream);
    originalSlidingStep.serialize(outputStream);
    ReadWriteIOUtils.write(timeZone.getID(), outputStream);
    ReadWriteIOUtils.write(currPrecision.ordinal(), outputStream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GroupByMonthFilter that = (GroupByMonthFilter) o;
    return originalStartTime == that.originalStartTime
        && originalEndTime == that.originalEndTime
        && originalSlidingStep.equals(that.originalSlidingStep)
        && originalInterval.equals(that.originalInterval)
        && timeZone.equals(that.timeZone)
        && currPrecision == that.currPrecision
        && Arrays.equals(startTimes, that.startTimes);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            super.hashCode(),
            originalSlidingStep,
            originalInterval,
            timeZone,
            originalStartTime,
            originalEndTime,
            currPrecision);
    result = 31 * result + Arrays.hashCode(startTimes);
    return result;
  }

  @Override
  public Filter copy() {
    // A stateful filter must implement copy() method
    // to make sure that each filter has its own state.
    return new GroupByMonthFilter(
        originalStartTime,
        originalEndTime,
        originalInterval,
        originalSlidingStep,
        timeZone,
        currPrecision);
  }
}
