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

import java.util.List;

/**
 * When calculating MAX_TIME and LAST_VALUE (the MIN_TIME and FIRST_VALUE in descending order), the
 * aggregation result always appears in the most recent pre-aggregation result. So, we do not need
 * to cache the previous pre-aggregated results in the queue.
 */
public class EmptyQueueSlidingWindowAggregator extends SlidingWindowAggregator {

  private long lastTime;

  public EmptyQueueSlidingWindowAggregator(
      Accumulator accumulator, List<InputLocation[]> inputLocationList, AggregationStep step) {
    super(accumulator, inputLocationList, step);
  }

  @Override
  protected void evictingExpiredValue() {
    if (lastTime != -1 && !curTimeRange.contains(lastTime)) {
      this.accumulator.reset();
      lastTime = -1;
    }
  }

  @Override
  public void processPartialResult(PartialAggregationResult partialResult) {
    if (!partialResult.isNull()) {
      this.accumulator.addIntermediate(partialResult.getPartialResult());
      lastTime = partialResult.getTime();
    }
  }
}
