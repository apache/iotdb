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

import org.apache.iotdb.tsfile.read.common.TimeRange;

/**
 * This class iteratively generates pre-aggregated time windows.
 *
 * <p>For example, startTime = 0, endTime = 11, interval = 5, slidingStep = 3, return
 * [0,2),[2,3),[3,5),[5,6),[6,8),[8,9),[9,10)
 */
public class PreAggrWindowIterator implements ITimeRangeIterator {

  private final long startTime;
  private final long endTime;
  private final long interval;
  private final long slidingStep;

  private final boolean isAscending;
  private final boolean leftCRightO;

  private long curInterval;
  private long curSlidingStep;
  private boolean isIntervalCyclicChange = false;
  private int intervalCnt = 0;

  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  public PreAggrWindowIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean leftCRightO) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.isAscending = isAscending;
    this.leftCRightO = leftCRightO;
    initIntervalAndStep();
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
    long retEndTime = Math.min(startTime + curInterval, endTime);
    updateIntervalAndStep();
    return new TimeRange(startTime, retEndTime);
  }

  private TimeRange getRightmostTimeRange() {
    long retStartTime;
    long retEndTime;
    long intervalNum = (long) Math.ceil((endTime - startTime) / (double) slidingStep);
    retStartTime = slidingStep * (intervalNum - 1) + startTime;
    if (isIntervalCyclicChange && endTime - retStartTime > interval % slidingStep) {
      retStartTime += interval % slidingStep;
      updateIntervalAndStep();
    }
    retEndTime = Math.min(retStartTime + curInterval, endTime);
    updateIntervalAndStep();
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
      retStartTime = curStartTime + curSlidingStep;
      // This is an open interval , [0-100)
      if (retStartTime >= endTime) {
        return false;
      }
    } else {
      retStartTime = curStartTime - curSlidingStep;
      if (retStartTime < startTime) {
        return false;
      }
    }
    retEndTime = Math.min(retStartTime + curInterval, endTime);
    updateIntervalAndStep();
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
    if (slidingStep >= interval || interval % slidingStep == 0) {
      return (long) Math.ceil(queryRange / (double) slidingStep);
    }

    long interval1 = interval % slidingStep, interval2 = slidingStep - interval % slidingStep;
    long intervalNum = Math.floorDiv(queryRange, interval1 + interval2);
    long tmpStartTime = startTime + intervalNum * (interval1 + interval2);
    if (tmpStartTime + interval1 > endTime) {
      return intervalNum * 2 + 1;
    } else {
      return intervalNum * 2 + 2;
    }
  }
}
