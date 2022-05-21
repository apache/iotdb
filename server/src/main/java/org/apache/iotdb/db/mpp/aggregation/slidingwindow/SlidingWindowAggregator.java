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
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public abstract class SlidingWindowAggregator extends Aggregator {

  // cached partial aggregation result of pre-aggregate windows
  protected Deque<Column[]> deque;

  public SlidingWindowAggregator(
      Accumulator accumulator, List<InputLocation[]> inputLocationList, AggregationStep step) {
    super(accumulator, step, inputLocationList);
    this.deque = new LinkedList<>();
  }

  @Override
  public void processTsBlock(TsBlock tsBlock) {}

  @Override
  public void updateTimeRange(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    evictingExpiredValue();
  }

  /** evicting expired element in queue and reset expired aggregateResult */
  protected abstract void evictingExpiredValue();

  /** update queue and aggregateResult */
  public abstract void processPartialResult(Column[] partialResult);
}
