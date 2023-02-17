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

package org.apache.iotdb.db.mpp.aggregation.timerangeiterator;

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import static org.apache.iotdb.db.utils.DateTimeUtils.MS_TO_MONTH;

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

  private final boolean isAscending;
  private final boolean isSlidingStepByMonth;
  private final boolean isIntervalByMonth;
  private final boolean leftCRightO;

  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  public AggrWindowIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth,
      boolean leftCRightO) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    this.isIntervalByMonth = isIntervalByMonth;
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
    if (isIntervalByMonth) {
      // calculate interval length by natural month based on startTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(DateTimeUtils.calcIntervalByMonth(startTime, interval), endTime);
    } else {
      retEndTime = Math.min(startTime + interval, endTime);
    }
    return new TimeRange(startTime, retEndTime);
  }

  private TimeRange getRightmostTimeRange() {
    long retStartTime;
    long retEndTime;
    long queryRange = endTime - startTime;
    long intervalNum;

    if (isSlidingStepByMonth) {
      intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep * MS_TO_MONTH));
      retStartTime = DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
      while (retStartTime >= endTime) {
        intervalNum -= 1;
        retStartTime = DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
      retStartTime = slidingStep * (intervalNum - 1) + startTime;
    }

    if (isIntervalByMonth) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(DateTimeUtils.calcIntervalByMonth(retStartTime, interval), endTime);
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

    long retStartTime, retEndTime;
    long curStartTime = curTimeRange.getMin();
    if (isAscending) {
      if (isSlidingStepByMonth) {
        retStartTime = DateTimeUtils.calcIntervalByMonth(curStartTime, (int) (slidingStep));
      } else {
        retStartTime = curStartTime + slidingStep;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return false;
      }
    } else {
      if (isSlidingStepByMonth) {
        retStartTime = DateTimeUtils.calcIntervalByMonth(curStartTime, (int) (-slidingStep));
      } else {
        retStartTime = curStartTime - slidingStep;
      }
      if (retStartTime < startTime) {
        return false;
      }
    }

    if (isIntervalByMonth) {
      retEndTime = DateTimeUtils.calcIntervalByMonth(retStartTime, (int) (interval));
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
      intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep * MS_TO_MONTH));
      long retStartTime = DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
      while (retStartTime > endTime) {
        intervalNum -= 1;
        retStartTime = DateTimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
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
