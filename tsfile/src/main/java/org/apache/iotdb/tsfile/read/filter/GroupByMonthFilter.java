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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

/**
 * GroupByMonthFilter is used to handle natural month slidingStep and interval by generating
 * dynamically. Attention: it's only supported to access in ascending order now.
 */
public class GroupByMonthFilter extends GroupByFilter {

  private int slidingStepsInMo;
  private int intervalInMo;
  private Calendar calendar = Calendar.getInstance();
  private static final long MS_TO_MONTH = 30 * 86400_000L;
  /** 10.31 -> 11.30 -> 12.31, not 10.31 -> 11.30 -> 12.30 */
  private long initialStartTime;

  // These fields will be serialized to remote nodes, as other fields may be updated during process
  private TimeZone timeZone;
  private boolean isSlidingStepByMonth;
  private boolean isIntervalByMonth;
  private long originalSlidingStep;
  private long originalInterval;
  private long originalStartTime;
  private long originalEndTime;

  public GroupByMonthFilter() {}

  public GroupByMonthFilter(
      long interval,
      long slidingStep,
      long startTime,
      long endTime,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth,
      TimeZone timeZone) {
    super(interval, slidingStep, startTime, endTime);
    this.originalInterval = interval;
    this.originalSlidingStep = slidingStep;
    this.originalStartTime = startTime;
    this.originalEndTime = endTime;
    initMonthGroupByParameters(isSlidingStepByMonth, isIntervalByMonth, timeZone);
  }

  public GroupByMonthFilter(GroupByMonthFilter filter) {
    super(filter.interval, filter.slidingStep, filter.startTime, filter.endTime);
    isIntervalByMonth = filter.isIntervalByMonth;
    isSlidingStepByMonth = filter.isSlidingStepByMonth;
    intervalInMo = filter.intervalInMo;
    slidingStepsInMo = filter.slidingStepsInMo;
    initialStartTime = filter.initialStartTime;
    originalStartTime = filter.originalStartTime;
    originalEndTime = filter.originalEndTime;
    originalSlidingStep = filter.originalSlidingStep;
    originalInterval = filter.originalInterval;
    calendar = Calendar.getInstance();
    calendar.setTimeZone(filter.calendar.getTimeZone());
    calendar.setTimeInMillis(filter.calendar.getTimeInMillis());
    timeZone = filter.timeZone;
  }

  // TODO: time descending order
  @Override
  public boolean satisfy(long time, Object value) {
    if (time < initialStartTime || time >= endTime) {
      return false;
    } else if (time >= startTime && time < startTime + slidingStep) {
      return time - startTime < interval;
    } else {
      long count = getTimePointPosition(time);
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
      long count = getTimePointPosition(startTime);
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
      long count = getTimePointPosition(startTime);
      getNthTimeInterval(count);
      // judge single interval that contains start time
      return isContainedByCurrentInterval(startTime, endTime);
    }
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      ReadWriteIOUtils.write(originalInterval, outputStream);
      ReadWriteIOUtils.write(originalSlidingStep, outputStream);
      ReadWriteIOUtils.write(originalStartTime, outputStream);
      ReadWriteIOUtils.write(originalEndTime, outputStream);
      ReadWriteIOUtils.write(isSlidingStepByMonth, outputStream);
      ReadWriteIOUtils.write(isIntervalByMonth, outputStream);
      ReadWriteIOUtils.write(timeZone.getID(), outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    originalInterval = ReadWriteIOUtils.readLong(buffer);
    originalSlidingStep = ReadWriteIOUtils.readLong(buffer);
    originalStartTime = ReadWriteIOUtils.readLong(buffer);
    originalEndTime = ReadWriteIOUtils.readLong(buffer);
    isSlidingStepByMonth = ReadWriteIOUtils.readBool(buffer);
    isIntervalByMonth = ReadWriteIOUtils.readBool(buffer);
    timeZone = TimeZone.getTimeZone(ReadWriteIOUtils.readString(buffer));

    interval = originalInterval;
    slidingStep = originalSlidingStep;
    startTime = originalStartTime;
    endTime = originalEndTime;

    initMonthGroupByParameters(isSlidingStepByMonth, isIntervalByMonth, timeZone);
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
        && this.isSlidingStepByMonth == other.isSlidingStepByMonth
        && this.isIntervalByMonth == other.isIntervalByMonth
        && this.timeZone.equals(other.timeZone)
        && this.initialStartTime == other.initialStartTime
        && this.intervalInMo == other.intervalInMo
        && this.slidingStepsInMo == other.slidingStepsInMo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        interval, slidingStep, startTime, endTime, isSlidingStepByMonth, isIntervalByMonth);
  }

  private void initMonthGroupByParameters(
      boolean isSlidingStepByMonth, boolean isIntervalByMonth, TimeZone timeZone) {
    initialStartTime = startTime;
    calendar.setTimeZone(timeZone);
    calendar.setTimeInMillis(startTime);
    this.timeZone = timeZone;
    this.isIntervalByMonth = isIntervalByMonth;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    if (isIntervalByMonth) {
      // TODO: 1mo1d
      intervalInMo = (int) (interval / MS_TO_MONTH);
    }
    if (isSlidingStepByMonth) {
      slidingStepsInMo = (int) (slidingStep / MS_TO_MONTH);
    }
    getNthTimeInterval(0);
  }

  /** Get the interval that @param time belongs to. */
  private long getTimePointPosition(long time) {
    long count;
    if (isSlidingStepByMonth) {
      count = (time - this.initialStartTime) / (slidingStepsInMo * 31 * 86400_000L);
      calendar.setTimeInMillis(initialStartTime);
      calendar.add(Calendar.MONTH, (int) count * slidingStepsInMo);
      while (calendar.getTimeInMillis() < time) {
        calendar.setTimeInMillis(initialStartTime);
        calendar.add(Calendar.MONTH, (int) (count + 1) * slidingStepsInMo);
        if (calendar.getTimeInMillis() > time) {
          break;
        } else {
          count++;
        }
      }
    } else {
      count = (time - this.initialStartTime) / slidingStep;
    }
    return count;
  }

  /** get the Nth time interval. */
  private void getNthTimeInterval(long n) {
    // get start time of time interval
    if (isSlidingStepByMonth) {
      calendar.setTimeInMillis(initialStartTime);
      calendar.add(Calendar.MONTH, (int) (slidingStepsInMo * n));
    } else {
      calendar.setTimeInMillis(initialStartTime + slidingStep * n);
    }
    this.startTime = calendar.getTimeInMillis();

    // get interval and sliding step
    if (isIntervalByMonth) {
      if (isSlidingStepByMonth) {
        calendar.setTimeInMillis(initialStartTime);
        calendar.add(Calendar.MONTH, (int) (slidingStepsInMo * n) + intervalInMo);
      } else {
        calendar.add(Calendar.MONTH, intervalInMo);
      }
      this.interval = calendar.getTimeInMillis() - startTime;
    }
    if (isSlidingStepByMonth) {
      calendar.setTimeInMillis(initialStartTime);
      calendar.add(Calendar.MONTH, (int) (slidingStepsInMo * (n + 1)));
      this.slidingStep = calendar.getTimeInMillis() - startTime;
    }
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.GROUP_BY_MONTH;
  }
}
