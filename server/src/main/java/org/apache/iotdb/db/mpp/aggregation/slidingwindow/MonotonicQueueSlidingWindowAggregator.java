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

package org.apache.iotdb.db.mpp.aggregation.slidingwindow;

import org.apache.iotdb.db.mpp.aggregation.Accumulator;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.util.Comparator;
import java.util.List;

/**
 * When computing MAX_VALUE, MIN_VALUE, EXTREME, we only add partial aggregation results that
 * maintain monotonicity to queue. The aggregation result always appears at the head of the queue.
 */
public class MonotonicQueueSlidingWindowAggregator extends SlidingWindowAggregator {

  private final Comparator<Column> comparator;

  public MonotonicQueueSlidingWindowAggregator(
      Accumulator accumulator,
      List<InputLocation[]> inputLocationList,
      AggregationStep step,
      Comparator<Column> comparator) {
    super(accumulator, inputLocationList, step);
    this.comparator = comparator;
  }

  @Override
  protected void evictingExpiredValue() {
    while (!deque.isEmpty() && !curTimeRange.contains(deque.getFirst().getTime())) {
      deque.removeFirst();
    }
    this.accumulator.reset();
    if (!deque.isEmpty()) {
      this.accumulator.addIntermediate(deque.getFirst().getPartialResult());
    }
  }

  @Override
  public void processPartialResult(PartialAggregationResult partialResult) {
    if (partialResult.isNull()) {
      return;
    }

    while (!deque.isEmpty()
        && comparator.compare(
                partialResult.getPartialResult()[0], deque.getLast().getPartialResult()[0])
            > 0) {
      deque.removeLast();
    }
    deque.addLast(partialResult);
    this.accumulator.addIntermediate(deque.getFirst().getPartialResult());
  }
}
