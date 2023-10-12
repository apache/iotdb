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
import org.apache.iotdb.tsfile.read.common.TimeRange;

/**
 * This class iteratively generates aggregated time windows.
 *
 * <p>For example, startTime = 0, endTime = 10, interval = 5, slidingStep = 3, leftCloseRightOpen,
 * return [0,5],[3,7],[6,9],[9,9]
 */
public class AggrWindowIterator implements ITimeRangeIterator {

  private final long startTime;
  private final long endTime;
  private final long interval;
  private final long slidingStep;
  private final long fixedIntervalInMonth;
  private final long fixedSlidingStepInMonth;
  private final long fixedIntervalOther;
  private final long fixedSlidingStepOther;

  private final boolean isAscending;
  private final boolean isSlidingStepByMonth;
  private final boolean isIntervalByMonth;
  private final boolean leftCRightO;

  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  @SuppressWarnings("squid:S107")
  public AggrWindowIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth,
      boolean leftCRightO,
      long fixedIntervalInMonth,
      long fixedSlidingStepInMonth) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    this.isIntervalByMonth = isIntervalByMonth;
    this.leftCRightO = leftCRightO;
    this.fixedIntervalInMonth = fixedIntervalInMonth;
    this.fixedSlidingStepInMonth = fixedSlidingStepInMonth;
    this.fixedIntervalOther = interval - fixedIntervalInMonth;
    this.fixedSlidingStepOther = slidingStep - fixedSlidingStepInMonth;
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
    if (isIntervalByMonth) {
      // calculate interval length by natural month based on startTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime =
          Math.min(
              DateTimeUtils.calcIntervalByMonthWithFixedOther(
                  startTime, fixedIntervalInMonth, fixedIntervalOther),
              endTime);
    } else {
      retEndTime = Math.min(startTime + interval, endTime);
    }
    return new TimeRange(startTime, retEndTime);
  }

  private TimeRange getRightmostTimeRange() {
    long retStartTime = startTime;
    long retEndTime;
    long queryRange = endTime - startTime;
    long intervalNum;

    if (isSlidingStepByMonth) {
      intervalNum = (long) Math.ceil((double) queryRange / slidingStep);
      if (fixedSlidingStepOther == 0) {
        retStartTime =
            DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * fixedSlidingStepInMonth);
        while (retStartTime > endTime) {
          intervalNum -= 1;
          retStartTime =
              DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * fixedSlidingStepInMonth);
        }
      } else {
        long iterStartTime = startTime;
        // couldn't  merge intervalNum and slidingStep directly
        // ie. startTIme = 1/22, interval = slidingStep = 1mo10d , endTime = 5/24 -> intervalNum = 4
        // If calculated by '3mo30d' is  5/22 and  the result of three iterations is 1/22 -> 3/4 ->
        // 4/14 -> 5/24
        for (int i = 0; i < intervalNum; i++) {
          iterStartTime =
              DateTimeUtils.calcIntervalByMonthWithFixedOther(
                  iterStartTime, fixedSlidingStepInMonth, fixedSlidingStepOther);
          if (iterStartTime >= endTime) {
            break;
          }
          retStartTime = iterStartTime;
        }
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
      retStartTime = slidingStep * (intervalNum - 1) + startTime;
    }

    if (isIntervalByMonth) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime =
          Math.min(
              DateTimeUtils.calcIntervalByMonthWithFixedOther(
                  retStartTime, fixedIntervalInMonth, fixedIntervalOther),
              endTime);
    } else {
      retEndTime = Math.min(retStartTime + interval, endTime);
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
      if (isSlidingStepByMonth) {
        retStartTime =
            DateTimeUtils.calcIntervalByMonthWithFixedOther(
                curStartTime, fixedSlidingStepInMonth, fixedSlidingStepOther);
      } else {
        retStartTime = curStartTime + slidingStep;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return false;
      }
    } else {
      if (isSlidingStepByMonth) {
        retStartTime =
            DateTimeUtils.calcIntervalByMonthWithFixedOther(
                curStartTime, -fixedSlidingStepInMonth, -fixedSlidingStepOther);
      } else {
        retStartTime = curStartTime - slidingStep;
      }
      if (retStartTime < startTime) {
        return false;
      }
    }

    if (isIntervalByMonth) {
      retEndTime =
          DateTimeUtils.calcIntervalByMonthWithFixedOther(
              retStartTime, fixedIntervalInMonth, fixedIntervalOther);
    } else {
      retEndTime = retStartTime + interval;
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

    if (isSlidingStepByMonth) {
      intervalNum = (long) Math.ceil((double) queryRange / slidingStep);
      if (fixedSlidingStepOther == 0) {
        long retStartTime =
            DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * fixedSlidingStepInMonth);
        while (retStartTime > endTime) {
          intervalNum -= 1;
          retStartTime =
              DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * fixedSlidingStepInMonth);
        }
      } else {
        long iterStartTime = startTime;
        for (int i = 1; i <= intervalNum; i++) {
          iterStartTime =
              DateTimeUtils.calcIntervalByMonthWithFixedOther(
                  iterStartTime, fixedSlidingStepInMonth, fixedSlidingStepOther);
          if (iterStartTime >= endTime) {
            intervalNum = i;
            break;
          }
        }
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
    }
    return intervalNum;
  }

  public void reset() {
    curTimeRange = null;
    hasCachedTimeRange = false;
  }
}
