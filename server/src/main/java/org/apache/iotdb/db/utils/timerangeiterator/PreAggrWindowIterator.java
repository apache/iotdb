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

package org.apache.iotdb.db.utils.timerangeiterator;

import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.utils.Pair;

public class PreAggrWindowIterator implements ITimeRangeIterator {

  // total query [startTime, endTime)
  private final long startTime;
  private final long endTime;

  private final long interval;
  private final long slidingStep;

  private final boolean isAscending;
  private final boolean isSlidingStepByMonth;
  private final boolean isIntervalByMonth;

  private long curInterval;
  private long curSlidingStep;
  private boolean isIntervalCyclicChange = false;
  private int intervalCnt = 0;

  private static final long MS_TO_MONTH = 30 * 86400_000L;

  public PreAggrWindowIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.isSlidingStepByMonth = isSlidingStepByMonth;
    this.isIntervalByMonth = isIntervalByMonth;
    initIntervalAndStep();
  }

  @Override
  public Pair<Long, Long> getFirstTimeRange() {
    if (isAscending) {
      return getLeftmostTimeRange();
    } else {
      return getRightmostTimeRange();
    }
  }

  private Pair<Long, Long> getLeftmostTimeRange() {
    long retEndTime;
    if (isIntervalByMonth) {
      // calculate interval length by natural month based on startTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(DatetimeUtils.calcIntervalByMonth(startTime, curInterval), endTime);
    } else {
      retEndTime = Math.min(startTime + curInterval, endTime);
    }
    updateIntervalAndStep();
    return new Pair<>(startTime, retEndTime);
  }

  private Pair<Long, Long> getRightmostTimeRange() {
    long retStartTime;
    long retEndTime;
    long queryRange = endTime - startTime;
    long intervalNum;

    if (isSlidingStepByMonth) {
      intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep * MS_TO_MONTH));
      retStartTime = DatetimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
      while (retStartTime >= endTime) {
        intervalNum -= 1;
        retStartTime = DatetimeUtils.calcIntervalByMonth(startTime, intervalNum * slidingStep);
      }
      if (isIntervalCyclicChange
          && endTime > DatetimeUtils.calcIntervalByMonth(retStartTime, interval % slidingStep)) {
        retStartTime = DatetimeUtils.calcIntervalByMonth(retStartTime, interval % slidingStep);
        updateIntervalAndStep();
      }
    } else {
      intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
      retStartTime = slidingStep * (intervalNum - 1) + startTime;
      if (isIntervalCyclicChange && endTime - retStartTime > interval % slidingStep) {
        retStartTime += interval % slidingStep;
        updateIntervalAndStep();
      }
    }

    if (isIntervalByMonth) {
      // calculate interval length by natural month based on curStartTime
      // ie. startTIme = 1/31, interval = 1mo, curEndTime will be set to 2/29
      retEndTime = Math.min(DatetimeUtils.calcIntervalByMonth(retStartTime, curInterval), endTime);
    } else {
      retEndTime = Math.min(retStartTime + curInterval, endTime);
    }
    updateIntervalAndStep();
    return new Pair<>(retStartTime, retEndTime);
  }

  @Override
  public Pair<Long, Long> getNextTimeRange(long curStartTime) {
    long retStartTime, retEndTime;
    if (isAscending) {
      if (isSlidingStepByMonth) {
        retStartTime = DatetimeUtils.calcIntervalByMonth(curStartTime, (int) (curSlidingStep));
      } else {
        retStartTime = curStartTime + curSlidingStep;
      }
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return null;
      }
    } else {
      if (isSlidingStepByMonth) {
        retStartTime = DatetimeUtils.calcIntervalByMonth(curStartTime, (int) (-curSlidingStep));
      } else {
        retStartTime = curStartTime - curSlidingStep;
      }
      if (retStartTime < startTime) {
        return null;
      }
    }

    if (isIntervalByMonth) {
      retEndTime = DatetimeUtils.calcIntervalByMonth(retStartTime, (int) (curInterval));
    } else {
      retEndTime = retStartTime + curInterval;
    }
    retEndTime = Math.min(retEndTime, endTime);
    updateIntervalAndStep();
    return new Pair<>(retStartTime, retEndTime);
  }

  @Override
  public boolean isAscending() {
    return isAscending;
  }

  private void initIntervalAndStep() {
    if (slidingStep >= interval) {
      curInterval = interval;
      curSlidingStep = slidingStep;
    } else if (interval % slidingStep == 0) {
      curInterval = slidingStep;
      curSlidingStep = slidingStep;
    } else {
      isIntervalCyclicChange = true;
      curInterval = interval % slidingStep;
      curSlidingStep = curInterval;
    }
  }

  private void updateIntervalAndStep() {
    if (!isIntervalCyclicChange) {
      return;
    }

    if (isAscending) {
      curSlidingStep = curInterval;
    }
    intervalCnt++;
    if ((intervalCnt & 1) == 1) {
      curInterval = slidingStep - interval % slidingStep;
    } else {
      curInterval = interval % slidingStep;
    }
    if (!isAscending) {
      curSlidingStep = curInterval;
    }
  }
}
