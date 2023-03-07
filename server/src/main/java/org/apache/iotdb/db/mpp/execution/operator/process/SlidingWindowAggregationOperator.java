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
import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;

public class SlidingWindowAggregationOperator extends SingleInputAggregationOperator {

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

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
    super(operatorContext, aggregators, child, ascending, maxReturnSize);
    checkArgument(
        groupByTimeParameter != null,
        "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator");

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.timeRangeIterator = timeRangeIterator;
    this.subTimeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
  }

  @Override
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  protected boolean calculateNextAggregationResult() {
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      // move to next time window
      curTimeRange = timeRangeIterator.nextTimeRange();

      // clear previous aggregation result
      for (Aggregator aggregator : aggregators) {
        ((SlidingWindowAggregator) aggregator).updateTimeRange(curTimeRange);
      }
    }

    while (!isCalculationDone()) {
      if (inputTsBlock == null) {
        // NOTE: child.next() can only be invoked once
        if (child.hasNextWithTimer() && canCallNext) {
          inputTsBlock = child.nextWithTimer();
          canCallNext = false;
        } else if (child.hasNextWithTimer()) {
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
      ((SlidingWindowAggregator) aggregator).processTsBlock(inputTsBlock);
    }

    inputTsBlock = inputTsBlock.skipFirst();
    if (inputTsBlock.isEmpty()) {
      inputTsBlock = null;
    }
    curSubTimeRange = null;
  }

  @Override
  protected void updateResultTsBlock() {
    curTimeRange = null;
    appendAggregationResult(
        resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
  }
}
