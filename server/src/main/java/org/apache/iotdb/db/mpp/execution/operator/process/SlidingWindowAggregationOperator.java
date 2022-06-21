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
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.mpp.execution.operator.process.AggregationOperator.updateResultTsBlockFromAggregators;
import static org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator.satisfied;
import static org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator.skipOutOfTimeRangePoints;
import static org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator.initTimeRangeIterator;

public class SlidingWindowAggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator child;

  private TsBlock cachedTsBlock;

  private final List<SlidingWindowAggregator> aggregators;

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final boolean ascending;

  private final TsBlockBuilder tsBlockBuilder;

  public SlidingWindowAggregationOperator(
      OperatorContext operatorContext,
      List<SlidingWindowAggregator> aggregators,
      Operator child,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    checkArgument(
        groupByTimeParameter != null,
        "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator");

    this.operatorContext = operatorContext;
    this.aggregators = aggregators;
    this.child = child;
    List<TSDataType> outputDataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      outputDataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, false);
    this.ascending = ascending;
  }

  @Override
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    // Move to next timeRange
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      curTimeRange = timeRangeIterator.nextTimeRange();
      for (Aggregator aggregator : aggregators) {
        aggregator.updateTimeRange(curTimeRange);
      }
    }

    // 1. Calculate aggregation result based on current time window
    boolean canCallNext = true;
    while (!calcFromTsBlock(cachedTsBlock, curTimeRange)) {
      cachedTsBlock = null;
      // child.next can only be invoked once
      if (child.hasNext() && canCallNext) {
        cachedTsBlock = child.next();
        canCallNext = false;
        // if child still has next but can't be invoked now
      } else if (child.hasNext()) {
        return null;
      } else {
        break;
      }
    }

    // 2. Update result using aggregators
    curTimeRange = null;
    return updateResultTsBlockFromAggregators(tsBlockBuilder, aggregators, timeRangeIterator);
  }

  private boolean calcFromTsBlock(TsBlock intputTsBlock, TimeRange timeRange) {
    // check if the batchData does not contain points in current interval
    if (intputTsBlock != null && satisfied(intputTsBlock, timeRange, ascending)) {
      // skip points that cannot be calculated
      if ((ascending && intputTsBlock.getStartTime() < timeRange.getMin())
          || (!ascending && intputTsBlock.getStartTime() > timeRange.getMax())) {
        intputTsBlock = skipOutOfTimeRangePoints(intputTsBlock, timeRange, ascending);
      }

      for (SlidingWindowAggregator aggregator : aggregators) {
        aggregator.processTsBlock(intputTsBlock);
      }
    }
    // The result is calculated from the cache
    return intputTsBlock != null
        && (ascending
            ? intputTsBlock.getEndTime() > timeRange.getMax()
            : intputTsBlock.getEndTime() < timeRange.getMin());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }
}
