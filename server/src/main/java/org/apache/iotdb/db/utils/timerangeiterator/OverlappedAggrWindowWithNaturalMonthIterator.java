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

import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.utils.Pair;

public class OverlappedAggrWindowWithNaturalMonthIterator implements ITimeRangeIterator {

  // total query [startTime, endTime)
  private final long startTime;
  private final long endTime;

  private final long interval;
  private final long slidingStep;

  private final boolean isAscending;
  private final boolean isSlidingStepByMonth;
  private final boolean isIntervalByMonth;

  private static final int HEAP_MAX_SIZE = 100;

  private TimeSelector timeBoundaryHeap;
  private long lastTime;

  public OverlappedAggrWindowWithNaturalMonthIterator(
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
    initHeap();
  }

  @Override
  public Pair<Long, Long> getFirstTimeRange() {
    long retStartTime = timeBoundaryHeap.pollFirst();
    lastTime = timeBoundaryHeap.pollFirst();
    return new Pair<>(retStartTime, lastTime);
  }

  @Override
  public Pair<Long, Long> getNextTimeRange(long curStartTime) {
    if (timeBoundaryHeap.isEmpty()) {
      return null;
    }
    long retStartTime = lastTime;
    lastTime = timeBoundaryHeap.pollFirst();
    return new Pair<>(retStartTime, lastTime);
  }

  private void initHeap() {
    timeBoundaryHeap = new TimeSelector(HEAP_MAX_SIZE, isAscending);
    AggrWindowIterator iterator =
        new AggrWindowIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            isAscending,
            isSlidingStepByMonth,
            isIntervalByMonth);
    Pair<Long, Long> firstTimeRange = iterator.getFirstTimeRange();
    timeBoundaryHeap.add(firstTimeRange.left);
    timeBoundaryHeap.add(firstTimeRange.right);

    long curStartTime = firstTimeRange.left;
    Pair<Long, Long> curTimeRange = iterator.getNextTimeRange(curStartTime);
    while (curTimeRange != null) {
      curStartTime = curTimeRange.left;
      timeBoundaryHeap.add(curTimeRange.left);
      timeBoundaryHeap.add(curTimeRange.right);
      curTimeRange = iterator.getNextTimeRange(curStartTime);
    }
  }

  @Override
  public boolean isAscending() {
    return isAscending;
  }
}
