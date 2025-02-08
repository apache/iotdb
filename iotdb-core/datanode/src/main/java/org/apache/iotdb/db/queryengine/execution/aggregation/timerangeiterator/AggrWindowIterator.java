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

package org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator;

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.TimeDuration;

import java.time.ZoneId;

/**
 * This class iteratively generates aggregated time windows.
 *
 * <p>For example, startTime = 0, endTime = 10, interval = 5, slidingStep = 3, leftCloseRightOpen,
 * return [0,5],[3,7],[6,9],[9,9]
 */
public class AggrWindowIterator implements ITimeRangeIterator {

  private final long startTime;
  private final long endTime;
  private final TimeDuration interval;
  private final TimeDuration slidingStep;

  private final boolean isAscending;
  private final boolean leftCRightO;

  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;
  // The number of current timeRange, it's used to calculate the cpu when there contains month
  private int timeRangeCount;

  private final ZoneId zoneId;

  @SuppressWarnings("squid:S107")
  public AggrWindowIterator(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean isAscending,
      boolean leftCRightO,
      ZoneId zoneId) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.leftCRightO = leftCRightO;
    this.timeRangeCount = 0;
    this.zoneId = zoneId;
  }

  @Override
  public TimeRange getFirstTimeRange() {
    if (isAscending) {
      return getLeftmostTimeRange();
    } else {
      return getRightmostTimeRange();
    }
  }

  private TimeRange getLeftmostTimeRange() {
    long retEndTime;
    if (interval.containsMonth()) {
      // calculate interval length by natural month based on startTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime =
          Math.min(DateTimeUtils.calcPositiveIntervalByMonth(startTime, interval, zoneId), endTime);
    } else {
      retEndTime = Math.min(startTime + interval.nonMonthDuration, endTime);
    }
    return new TimeRange(startTime, retEndTime);
  }

  private TimeRange getRightmostTimeRange() {
    long retStartTime;
    long retEndTime;
    long queryRange = endTime - startTime;
    long intervalNum;

    if (slidingStep.containsMonth()) {
      intervalNum =
          (long)
              Math.ceil(
                  (double) queryRange
                      / (slidingStep.getMaxTotalDuration(TimestampPrecisionUtils.currPrecision)));
      long tempRetStartTime =
          DateTimeUtils.calcPositiveIntervalByMonth(
              startTime, slidingStep.multiple(intervalNum - 1), zoneId);
      retStartTime = tempRetStartTime;
      while (tempRetStartTime < endTime) {
        intervalNum++;
        retStartTime = tempRetStartTime;
        tempRetStartTime =
            DateTimeUtils.calcPositiveIntervalByMonth(
                retStartTime, slidingStep.multiple(intervalNum - 1), zoneId);
      }
      intervalNum -= 1;
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep.nonMonthDuration);
      retStartTime = slidingStep.nonMonthDuration * (intervalNum - 1) + startTime;
    }

    if (interval.containsMonth()) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime =
          Math.min(
              DateTimeUtils.calcPositiveIntervalByMonth(
                  startTime, interval.merge(slidingStep.multiple(intervalNum - 1)), zoneId),
              endTime);
    } else {
      retEndTime = Math.min(retStartTime + interval.nonMonthDuration, endTime);
    }
    return new TimeRange(retStartTime, retEndTime);
  }

  @Override
  public boolean hasNextTimeRange() {
    if (hasCachedTimeRange) {
      return true;
    }
    if (curTimeRange == null) {
      curTimeRange = getFirstTimeRange();
      timeRangeCount++;
      hasCachedTimeRange = true;
      return true;
    }

    long retStartTime;
    long retEndTime;
    long curStartTime = curTimeRange.getMin();
    if (isAscending) {
      if (slidingStep.containsMonth()) {
        retStartTime =
            DateTimeUtils.calcPositiveIntervalByMonth(
                startTime, slidingStep.multiple(timeRangeCount), zoneId);
      } else {
        retStartTime = curStartTime + slidingStep.nonMonthDuration;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return false;
      }
    } else {
      if (slidingStep.containsMonth()) {
        // group by month doesn't support ascending.
        throw new UnsupportedOperationException(
            "Ascending is not supported when sliding step contains month.");
      } else {
        retStartTime = curStartTime - slidingStep.nonMonthDuration;
      }
      if (retStartTime < startTime) {
        return false;
      }
    }

    if (interval.containsMonth()) {
      retEndTime =
          DateTimeUtils.calcPositiveIntervalByMonth(
              startTime, slidingStep.multiple(timeRangeCount).merge(interval), zoneId);
    } else {
      retEndTime = retStartTime + interval.nonMonthDuration;
    }
    retEndTime = Math.min(retEndTime, endTime);
    curTimeRange = new TimeRange(retStartTime, retEndTime);
    hasCachedTimeRange = true;
    timeRangeCount++;
    return true;
  }

  @Override
  public TimeRange nextTimeRange() {
    if (hasCachedTimeRange || hasNextTimeRange()) {
      hasCachedTimeRange = false;
      return getFinalTimeRange(curTimeRange, leftCRightO);
    }
    return null;
  }

  @Override
  public boolean isAscending() {
    return isAscending;
  }

  @Override
  public long currentOutputTime() {
    return leftCRightO ? curTimeRange.getMin() : curTimeRange.getMax();
  }

  @Override
  public long getTotalIntervalNum() {
    long queryRange = endTime - startTime;
    long intervalNum;

    if (slidingStep.containsMonth()) {
      intervalNum =
          (long)
              Math.ceil(
                  (double) queryRange
                      / (slidingStep.getMaxTotalDuration(TimestampPrecisionUtils.currPrecision)));
      long retStartTime =
          DateTimeUtils.calcPositiveIntervalByMonth(
              startTime, slidingStep.multiple(intervalNum), zoneId);
      while (retStartTime < endTime) {
        intervalNum++;
        retStartTime =
            DateTimeUtils.calcPositiveIntervalByMonth(
                startTime, slidingStep.multiple(intervalNum), zoneId);
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep.nonMonthDuration);
    }
    return intervalNum;
  }

  public void reset() {
    curTimeRange = null;
    hasCachedTimeRange = false;
    timeRangeCount = 0;
  }
}
