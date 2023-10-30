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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.TimeDuration;

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

  @SuppressWarnings("squid:S107")
  public AggrWindowIterator(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean isAscending,
      boolean leftCRightO) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.leftCRightO = leftCRightO;
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
          Math.min(DateTimeUtils.calcPositiveIntervalByMonth(startTime, interval, 1), endTime);
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
                  queryRange
                      / (double)
                          (slidingStep.getMaxTotalDuration(TimestampPrecisionUtils.currPrecision)));
      retStartTime = DateTimeUtils.calcPositiveIntervalByMonth(startTime, slidingStep, intervalNum);
      while (retStartTime >= endTime) {
        intervalNum -= 1;
        retStartTime =
            DateTimeUtils.calcPositiveIntervalByMonth(startTime, slidingStep, intervalNum);
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep.nonMonthDuration);
      retStartTime = slidingStep.nonMonthDuration * (intervalNum - 1) + startTime;
    }

    if (interval.containsMonth()) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime =
          Math.min(DateTimeUtils.calcPositiveIntervalByMonth(retStartTime, interval, 1), endTime);
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
      hasCachedTimeRange = true;
      return true;
    }

    long retStartTime;
    long retEndTime;
    long curStartTime = curTimeRange.getMin();
    if (isAscending) {
      if (slidingStep.containsMonth()) {
        retStartTime = DateTimeUtils.calcPositiveIntervalByMonth(curStartTime, slidingStep, 1);
      } else {
        retStartTime = curStartTime + slidingStep.nonMonthDuration;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return false;
      }
    } else {
      if (slidingStep.containsMonth()) {
        retStartTime = DateTimeUtils.calcNegativeIntervalByMonth(curStartTime, slidingStep);
      } else {
        retStartTime = curStartTime - slidingStep.nonMonthDuration;
      }
      if (retStartTime < startTime) {
        return false;
      }
    }

    if (interval.containsMonth()) {
      retEndTime = DateTimeUtils.calcPositiveIntervalByMonth(retStartTime, interval, 1);
    } else {
      retEndTime = retStartTime + interval.nonMonthDuration;
    }
    retEndTime = Math.min(retEndTime, endTime);
    curTimeRange = new TimeRange(retStartTime, retEndTime);
    hasCachedTimeRange = true;
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
          DateTimeUtils.calcPositiveIntervalByMonth(startTime, slidingStep, intervalNum);
      while (retStartTime < endTime) {
        intervalNum++;
        retStartTime = DateTimeUtils.calcPositiveIntervalByMonth(retStartTime, slidingStep, 1);
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep.nonMonthDuration);
    }
    return intervalNum;
  }

  public void reset() {
    curTimeRange = null;
    hasCachedTimeRange = false;
  }
}
