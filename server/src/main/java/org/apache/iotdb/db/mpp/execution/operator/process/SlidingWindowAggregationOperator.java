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
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.TimeRangeIteratorFactory;
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

public class SlidingWindowAggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator child;

  private TsBlock cachedTsBlock;

  private final List<SlidingWindowAggregator> aggregators;

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window
  private TimeRange curTimeRange;

  private final boolean ascending;

  private final TsBlockBuilder tsBlockBuilder;

  public SlidingWindowAggregationOperator(
      OperatorContext operatorContext,
      List<SlidingWindowAggregator> aggregators,
      Operator child,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.operatorContext = operatorContext;
    this.aggregators = aggregators;
    this.child = child;
    List<TSDataType> outputDataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      outputDataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending);
    this.ascending = ascending;
  }

  @Override
  public boolean hasNext() {
    return timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    curTimeRange = timeRangeIterator.nextTimeRange();
    for (SlidingWindowAggregator aggregator : aggregators) {
      aggregator.updateTimeRange(curTimeRange);
    }

    if (cachedTsBlock != null) {
      calcFromTsBlock(cachedTsBlock);
    }

    if (child.hasNext()) {
      calcFromTsBlock(child.next());
    }

    // output result from aggregator
    return AggregationOperator.updateResultTsBlockFromAggregators(
        tsBlockBuilder, aggregators, timeRangeIterator);
  }

  private void calcFromTsBlock(TsBlock inputTsBlock) {
    TsBlock.TsBlockSingleColumnIterator inputTsBlockIterator =
        inputTsBlock.getTsBlockSingleColumnIterator();
    while (inputTsBlockIterator.hasNext() && !isEndCal(inputTsBlockIterator)) {
      for (SlidingWindowAggregator aggregator : aggregators) {
        aggregator.processTsBlock(inputTsBlock.subTsBlock(inputTsBlockIterator.getRowIndex()));
      }
      inputTsBlockIterator.next();
    }
    if (inputTsBlockIterator.hasNext()) {
      cachedTsBlock = inputTsBlock;
    } else {
      cachedTsBlock = null;
    }
  }

  private boolean isEndCal(TsBlock.TsBlockSingleColumnIterator iterator) {
    return ascending
        ? iterator.getEndTime() > curTimeRange.getMax()
        : iterator.getStartTime() < curTimeRange.getMin();
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

  public static ITimeRangeIterator initTimeRangeIterator(
      GroupByTimeParameter groupByTimeParameter, boolean ascending) {
    checkArgument(
        groupByTimeParameter != null,
        "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator");
    return TimeRangeIteratorFactory.getTimeRangeIterator(
        groupByTimeParameter.getStartTime(),
        groupByTimeParameter.getEndTime(),
        groupByTimeParameter.getInterval(),
        groupByTimeParameter.getSlidingStep(),
        ascending,
        groupByTimeParameter.isIntervalByMonth(),
        groupByTimeParameter.isSlidingStepByMonth(),
        groupByTimeParameter.isLeftCRightO(),
        false);
  }
}
