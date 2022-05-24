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
    return timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    // 1. Clear previous aggregation result
    TimeRange curTimeRange = timeRangeIterator.nextTimeRange();
    for (SlidingWindowAggregator aggregator : aggregators) {
      aggregator.updateTimeRange(curTimeRange);
    }

    // 2. Calculate aggregation result based on current time window
    TsBlock inputTsBlock = cachedTsBlock;
    while (!calcFromTsBlock(inputTsBlock, curTimeRange)) {
      if (child.hasNext()) {
        inputTsBlock = child.next();
      } else {
        break;
      }
    }
    if (inputTsBlock != null && !satisfied(inputTsBlock, curTimeRange, ascending)) {
      cachedTsBlock = inputTsBlock;
    } else {
      cachedTsBlock = null;
    }

    // 3. Update result using aggregators
    return updateResultTsBlockFromAggregators(tsBlockBuilder, aggregators, timeRangeIterator);
  }

  private boolean calcFromTsBlock(TsBlock inputTsBlock, TimeRange timeRange) {
    // check if the batchData does not contain points in current interval
    if (inputTsBlock != null && satisfied(inputTsBlock, timeRange, ascending)) {
      // skip points that cannot be calculated
      inputTsBlock = skipOutOfTimeRangePoints(inputTsBlock, timeRange, ascending);
      for (SlidingWindowAggregator aggregator : aggregators) {
        aggregator.processTsBlock(inputTsBlock);
      }
    }
    // The result is calculated from the cache
    return inputTsBlock != null
        && (ascending
            ? inputTsBlock.getEndTime() > timeRange.getMax()
            : inputTsBlock.getEndTime() < timeRange.getMin());
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
