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
    while (!deque.isEmpty() && !curTimeRange.contains(deque.getFirst()[0].getLong(0))) {
      deque.removeFirst();
    }
    if (!deque.isEmpty()) {
      this.accumulator.setFinal(deque.getFirst()[1]);
    } else {
      this.accumulator.reset();
    }
  }

  @Override
  public void processPartialResult(Column[] partialResult) {
    if (partialResult[1].isNull(0)) {
      return;
    }

    while (!deque.isEmpty() && comparator.compare(partialResult[1], deque.getLast()[1]) > 0) {
      deque.removeLast();
    }
    deque.addLast(partialResult);
    if (!deque.isEmpty()) {
      this.accumulator.setFinal(partialResult[1]);
    } else {
      this.accumulator.reset();
    }
  }
}
