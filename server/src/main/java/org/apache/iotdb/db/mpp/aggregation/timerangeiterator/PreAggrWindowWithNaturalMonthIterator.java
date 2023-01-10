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

import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.read.common.TimeRange;

public class PreAggrWindowWithNaturalMonthIterator implements ITimeRangeIterator {

  private static final int HEAP_MAX_SIZE = 100;

  private final boolean isAscending;
  private final boolean leftCRightO;
  private final TimeSelector timeBoundaryHeap;

  private final AggrWindowIterator aggrWindowIterator;
  private long curStartTimeForIterator;

  private long lastEndTime;
  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  public PreAggrWindowWithNaturalMonthIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isSlidingStepByMonth,
      boolean isIntervalByMonth,
      boolean leftCRightO) {
    this.isAscending = isAscending;
    this.timeBoundaryHeap = new TimeSelector(HEAP_MAX_SIZE, isAscending);
    this.aggrWindowIterator =
        new AggrWindowIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            isAscending,
            isSlidingStepByMonth,
            isIntervalByMonth,
            leftCRightO);
    this.leftCRightO = leftCRightO;
    initHeap();
  }

  @Override
  public TimeRange getFirstTimeRange() {
    long retStartTime = timeBoundaryHeap.pollFirst();
    lastEndTime = timeBoundaryHeap.first();
    return new TimeRange(retStartTime, lastEndTime);
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

    if (lastEndTime >= curStartTimeForIterator) {
      tryToExpandHeap();
    }
    if (timeBoundaryHeap.isEmpty()) {
      return false;
    }
    long retStartTime = timeBoundaryHeap.pollFirst();
    if (retStartTime >= curStartTimeForIterator) {
      tryToExpandHeap();
    }
    if (timeBoundaryHeap.isEmpty()) {
      return false;
    }
    lastEndTime = timeBoundaryHeap.first();
    curTimeRange = new TimeRange(retStartTime, lastEndTime);
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

  private void initHeap() {
    TimeRange firstTimeRange = aggrWindowIterator.nextTimeRange();
    if (leftCRightO) {
      timeBoundaryHeap.add(firstTimeRange.getMin());
      timeBoundaryHeap.add(firstTimeRange.getMax() + 1);
      curStartTimeForIterator = firstTimeRange.getMin();
    } else {
      timeBoundaryHeap.add(firstTimeRange.getMin() - 1);
      timeBoundaryHeap.add(firstTimeRange.getMax());
      curStartTimeForIterator = firstTimeRange.getMin() - 1;
    }
    tryToExpandHeap();
  }

  private void tryToExpandHeap() {
    TimeRange timeRangeToExpand = null;
    while (aggrWindowIterator.hasNextTimeRange() && timeBoundaryHeap.size() < HEAP_MAX_SIZE) {
      timeRangeToExpand = aggrWindowIterator.nextTimeRange();
      if (leftCRightO) {
        timeBoundaryHeap.add(timeRangeToExpand.getMin());
        timeBoundaryHeap.add(timeRangeToExpand.getMax() + 1);
        curStartTimeForIterator = timeRangeToExpand.getMin();
      } else {
        timeBoundaryHeap.add(timeRangeToExpand.getMin() - 1);
        timeBoundaryHeap.add(timeRangeToExpand.getMax());
        curStartTimeForIterator = timeRangeToExpand.getMin() - 1;
      }
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
    long tmpInterval = 0;
    while (hasNextTimeRange()) {
      tmpInterval++;
      nextTimeRange();
    }

    curTimeRange = null;
    timeBoundaryHeap.clear();
    aggrWindowIterator.reset();
    initHeap();

    return tmpInterval;
  }
}
