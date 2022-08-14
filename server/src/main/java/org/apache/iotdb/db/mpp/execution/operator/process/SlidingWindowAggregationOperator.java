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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;

public class SlidingWindowAggregationOperator extends SingleInputAggregationOperator {

  private final ITimeRangeIterator subTimeRangeIterator;
  // current interval of pre-aggregation window [curStartTime, curEndTime)
  private TimeRange curSubTimeRange;

  public SlidingWindowAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize) {
    super(operatorContext, aggregators, child, ascending, timeRangeIterator, maxReturnSize);
    checkArgument(
        groupByTimeParameter != null,
        "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator");
    this.subTimeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
  }

  @Override
  protected boolean calculateNextAggregationResult() {
    while (!isCalculationDone()) {
      if (inputTsBlock == null) {
        // NOTE: child.next() can only be invoked once
        if (child.hasNext() && canCallNext) {
          inputTsBlock = child.next();
          canCallNext = false;
        } else if (child.hasNext()) {
          // if child still has next but can't be invoked now
          return false;
        } else {
          break;
        }
      }

      calculateFromCachedData();
    }

    // update result using aggregators
    updateResultTsBlock();

    return true;
  }

  /** @return if already get the result */
  private boolean isCalculationDone() {
    if (curSubTimeRange == null && !subTimeRangeIterator.hasNextTimeRange()) {
      return true;
    }

    if (curSubTimeRange == null && subTimeRangeIterator.hasNextTimeRange()) {
      curSubTimeRange = subTimeRangeIterator.nextTimeRange();
    }
    return ascending
        ? curSubTimeRange.getMin() > curTimeRange.getMax()
        : curSubTimeRange.getMax() < curTimeRange.getMin();
  }

  private void calculateFromCachedData() {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return;
    }

    for (Aggregator aggregator : aggregators) {
      aggregator.processTsBlock(inputTsBlock);
    }

    inputTsBlock = inputTsBlock.skipFirst();
    if (inputTsBlock.isEmpty()) {
      inputTsBlock = null;
    }
    curSubTimeRange = null;
  }
}
