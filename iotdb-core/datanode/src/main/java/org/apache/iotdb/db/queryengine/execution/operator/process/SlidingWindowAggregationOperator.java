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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.slidingwindow.SlidingWindowAggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.initTimeRangeIterator;

public class SlidingWindowAggregationOperator extends SingleInputAggregationOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SlidingWindowAggregationOperator.class);
  private final ITimeRangeIterator timeRangeIterator;
  // Current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final ITimeRangeIterator subTimeRangeIterator;
  // Current interval of pre-aggregation window [curStartTime, curEndTime)
  private TimeRange curSubTimeRange;
  private final boolean outputEndTime;

  public SlidingWindowAggregationOperator(
      OperatorContext operatorContext,
      List<TreeAggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      boolean outputEndTime,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize) {
    super(operatorContext, aggregators, child, ascending, maxReturnSize);
    checkArgument(
        groupByTimeParameter != null,
        "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator");

    List<TSDataType> dataTypes = new ArrayList<>();
    if (outputEndTime) {
      dataTypes.add(TSDataType.INT64);
    }
    for (TreeAggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.timeRangeIterator = timeRangeIterator;
    this.outputEndTime = outputEndTime;
    this.subTimeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
  }

  @Override
  public boolean hasNext() throws Exception {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @SuppressWarnings("squid:S112")
  @Override
  protected boolean calculateNextAggregationResult() throws Exception {
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      // Move to next time window
      curTimeRange = timeRangeIterator.nextTimeRange();

      // Clear previous aggregation result
      for (TreeAggregator aggregator : aggregators) {
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
          // If child still has next but can't be invoked now
          return false;
        } else {
          break;
        }
      }

      calculateFromCachedData();
    }

    // Update result using aggregators
    updateResultTsBlock();

    return true;
  }

  /** return if already get the result. */
  private boolean isCalculationDone() {
    if (curSubTimeRange == null) {
      if (!subTimeRangeIterator.hasNextTimeRange()) {
        return true;
      } else {
        curSubTimeRange = subTimeRangeIterator.nextTimeRange();
      }
    }

    return ascending
        ? curSubTimeRange.getMin() > curTimeRange.getMax()
        : curSubTimeRange.getMax() < curTimeRange.getMin();
  }

  private void calculateFromCachedData() {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return;
    }

    for (TreeAggregator aggregator : aggregators) {
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
    if (!outputEndTime) {
      appendAggregationResult(
          resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
    } else {
      appendAggregationResult(
          resultTsBlockBuilder,
          aggregators,
          timeRangeIterator.currentOutputTime(),
          curTimeRange.getMax());
    }
    curTimeRange = null;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultTsBlockBuilder.getRetainedSizeInBytes();
  }
}
