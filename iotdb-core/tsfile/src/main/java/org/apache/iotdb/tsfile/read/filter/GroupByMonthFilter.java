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
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.tsfile.utils.TimeDuration.calcPositiveIntervalByMonth;
import static org.apache.iotdb.tsfile.utils.TimeDuration.getConsecutiveTimesIntervalByMonth;

/**
 * GroupByMonthFilter is used to handle natural month slidingStep and interval by generating
 * dynamically. Attention: it's only supported to access in ascending order now.
 */
public class GroupByMonthFilter extends GroupByFilter {
  private Calendar calendar = Calendar.getInstance();

  private TimeDuration originalSlidingStep;
  private TimeDuration originalInterval;

  // These fields will be serialized to remote nodes, as other fields may be updated during process
  private TimeZone timeZone;
  private long originalStartTime;
  private long originalEndTime;

  private TimeUnit currPrecision;

  private long[] startTimes;

  public GroupByMonthFilter() {}

  public GroupByMonthFilter(
      TimeDuration interval,
      TimeDuration slidingStep,
      long startTime,
      long endTime,
      TimeZone timeZone,
      TimeUnit currPrecision) {
    super(startTime, endTime);
    this.originalInterval = interval;
    this.originalSlidingStep = slidingStep;
    if (!interval.containsMonth()) {
      this.interval = interval.nonMonthDuration;
    }
    if (!slidingStep.containsMonth()) {
      this.slidingStep = slidingStep.nonMonthDuration;
    }
    this.originalStartTime = startTime;
    this.originalEndTime = endTime;
    this.currPrecision = currPrecision;
    initMonthGroupByParameters(timeZone);
  }

  public GroupByMonthFilter(GroupByMonthFilter filter) {
    super(filter.interval, filter.slidingStep, filter.startTime, filter.endTime);
    originalStartTime = filter.originalStartTime;
    originalEndTime = filter.originalEndTime;
    originalSlidingStep = filter.originalSlidingStep;
    originalInterval = filter.originalInterval;
    calendar = Calendar.getInstance();
    calendar.setTimeZone(filter.calendar.getTimeZone());
    calendar.setTimeInMillis(filter.calendar.getTimeInMillis());
    timeZone = filter.timeZone;
    currPrecision = filter.currPrecision;
    // the value in this array will not be changed, so we can copy reference directly
    startTimes = filter.startTimes;
  }

  // TODO: time descending order
  @Override
  public boolean satisfy(long time, Object value) {
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

  @Override
  public Filter copy() {
    return new GroupByMonthFilter(this);
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

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      originalInterval.serialize(outputStream);
      originalSlidingStep.serialize(outputStream);
      ReadWriteIOUtils.write(originalStartTime, outputStream);
      ReadWriteIOUtils.write(originalEndTime, outputStream);
      ReadWriteIOUtils.write(currPrecision.ordinal(), outputStream);
      ReadWriteIOUtils.write(timeZone.getID(), outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    originalInterval = TimeDuration.deserialize(buffer);
    originalSlidingStep = TimeDuration.deserialize(buffer);
    originalStartTime = ReadWriteIOUtils.readLong(buffer);
    originalEndTime = ReadWriteIOUtils.readLong(buffer);
    currPrecision = TimeUnit.values()[ReadWriteIOUtils.readInt(buffer)];
    timeZone = TimeZone.getTimeZone(ReadWriteIOUtils.readString(buffer));
    startTime = originalStartTime;
    endTime = originalEndTime;
    initMonthGroupByParameters(timeZone);
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
    return this.originalInterval == other.originalInterval
        && this.originalSlidingStep == other.originalSlidingStep
        && this.originalStartTime == other.originalStartTime
        && this.originalEndTime == other.originalEndTime
        && this.currPrecision == other.currPrecision
        && this.timeZone.equals(other.timeZone);
  }

  @Override
  public int hashCode() {
    return Objects.hash(originalInterval, originalSlidingStep, originalStartTime, originalEndTime);
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
                  ((originalEndTime - originalStartTime)
                      / originalSlidingStep.getMinTotalDuration(currPrecision)),
              timeZone,
              currPrecision);
    }
    getNthTimeInterval(0);
  }

  /** Get the interval that @param time belongs to. */
  private int getTimePointPosition(long time) {
    if (originalSlidingStep.containsMonth()) {
      int searchResult = Arrays.binarySearch(startTimes, time);
      return searchResult >= 0 ? searchResult : Math.abs(searchResult) - 2;
    } else {
      return (int) ((time - originalStartTime) / slidingStep);
    }
  }

  /** get the Nth time interval. */
  private void getNthTimeInterval(int n) {
    // get interval and sliding step
    if (originalInterval.containsMonth()) {
      this.interval =
          calcPositiveIntervalByMonth(startTime, originalInterval, 1, timeZone, currPrecision)
              - startTime;
    }
    if (originalSlidingStep.containsMonth()) {
      this.startTime = startTimes[n];
      this.slidingStep =
          calcPositiveIntervalByMonth(startTime, originalSlidingStep, 1, timeZone, currPrecision)
              - startTime;
    } else {
      startTime = originalStartTime + n * slidingStep;
    }
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.GROUP_BY_MONTH;
  }
}
