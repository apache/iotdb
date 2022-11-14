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

import java.util.List;

public class SampleWindowIterator implements ITimeRangeIterator {

  private final ITimeRangeIterator allTimeRangeIterator;
  private final List<Integer> samplingIndexes;

  private int sampleIndex = 0;
  private int timeRangeIndex = 0;

  private TimeRange curTimeRange;

  public SampleWindowIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      List<Integer> samplingIndexes) {
    this.samplingIndexes = samplingIndexes;
    this.allTimeRangeIterator =
        TimeRangeIteratorFactory.getTimeRangeIterator(
            startTime, endTime, interval, slidingStep, true, false, false, true, false);
  }

  @Override
  public TimeRange getFirstTimeRange() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNextTimeRange() {
    return sampleIndex < samplingIndexes.size();
  }

  @Override
  public TimeRange nextTimeRange() {
    while (allTimeRangeIterator.hasNextTimeRange()) {
      TimeRange timeRange = allTimeRangeIterator.nextTimeRange();
      if (timeRangeIndex == samplingIndexes.get(sampleIndex)) {
        curTimeRange = timeRange;
        timeRangeIndex++;
        sampleIndex++;
        break;
      }
      timeRangeIndex++;
    }
    return curTimeRange;
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
    return samplingIndexes.size();
  }
}
