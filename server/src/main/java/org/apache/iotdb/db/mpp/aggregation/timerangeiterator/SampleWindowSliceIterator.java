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

import java.util.List;

public class SampleWindowSliceIterator implements ITimeRangeIterator {

  private static final int HEAP_MAX_SIZE = 100;
  private final TimeSelector timeBoundaryHeap;

  private final SampleWindowIterator sampleWindowIterator;

  private long curStartTimeForIterator;
  private long lastEndTime;
  private TimeRange curTimeRange;
  private boolean hasCachedTimeRange;

  public SampleWindowSliceIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      List<Integer> samplingIndexes) {
    this.timeBoundaryHeap = new TimeSelector(HEAP_MAX_SIZE, true);
    this.sampleWindowIterator =
        new SampleWindowIterator(startTime, endTime, interval, slidingStep, samplingIndexes);
    initHeap();
  }

  private void initHeap() {
    TimeRange firstTimeRange = sampleWindowIterator.nextTimeRange();
    timeBoundaryHeap.add(firstTimeRange.getMin());
    timeBoundaryHeap.add(firstTimeRange.getMax() + 1);
    curStartTimeForIterator = firstTimeRange.getMin();
    tryToExpandHeap();
  }

  private void tryToExpandHeap() {
    TimeRange timeRangeToExpand;
    while (sampleWindowIterator.hasNextTimeRange() && timeBoundaryHeap.size() < HEAP_MAX_SIZE) {
      timeRangeToExpand = sampleWindowIterator.nextTimeRange();
      timeBoundaryHeap.add(timeRangeToExpand.getMin());
      timeBoundaryHeap.add(timeRangeToExpand.getMax() + 1);
      curStartTimeForIterator = timeRangeToExpand.getMin();
    }
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
      return getFinalTimeRange(curTimeRange, true);
    }
    return null;
  }

  @Override
  public boolean isAscending() {
    return true;
  }

  @Override
  public long currentOutputTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTotalIntervalNum() {
    throw new UnsupportedOperationException();
  }
}
