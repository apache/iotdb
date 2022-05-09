/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.utils.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregateScanOperator.initTimeRangeIterator;

/**
 * RawDataAggregateOperator is used to process raw data tsBlock input calculating using value
 * filter. It's possible that there is more than one tsBlock input in one time interval. And it's
 * also possible that one tsBlock can cover multiple time intervals too.
 *
 * <p>Since raw data query with value filter is processed by FilterOperator above TimeJoinOperator,
 * there we can see RawDataAggregateOperator as a one-to-one(one input, ont output) operator.
 *
 * <p>Return aggregation result in one time interval once.
 */
public class RawDataAggregateOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Aggregator> aggregators;
  private final Operator child;
  private final boolean ascending;
  private ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private TsBlock preCachedData;

  // Using for building result tsBlock
  private final TsBlockBuilder tsBlockBuilder;

  public RawDataAggregateOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      Operator child,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.operatorContext = operatorContext;
    this.aggregators = aggregators;
    this.child = child;
    this.ascending = ascending;

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending);
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
  public TsBlock next() {
    // 1. Clear previous aggregation result
    for (Aggregator aggregator : aggregators) {
      aggregator.reset();
      aggregator.setTimeRange(curTimeRange);
    }

    // 2. Calculate aggregation result based on current time window
    while (!calcFromCacheData(curTimeRange)) {
      if (child.hasNext()) {
        preCachedData = child.next();
      } else {
        break;
      }
    }

    // 3. Update result using aggregators
    return AggregateOperator.updateResultTsBlockFromAggregators(
        tsBlockBuilder, aggregators, curTimeRange);
  }

  @Override
  public boolean hasNext() {
    if (!timeRangeIterator.hasNextTimeRange()) {
      return false;
    }
    curTimeRange = timeRangeIterator.nextTimeRange();
    return true;
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  /** @return if already get the result */
  private boolean calcFromCacheData(TimeRange curTimeRange) {
    // check if the batchData does not contain points in current interval
    if (preCachedData != null && satisfied(preCachedData, curTimeRange, ascending)) {
      // skip points that cannot be calculated
      preCachedData = skipOutOfTimeRangePoints(preCachedData, curTimeRange, ascending);

      for (Aggregator aggregator : aggregators) {
        // current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        aggregator.processTsBlock(preCachedData);
      }
    }
    // The result is calculated from the cache
    return (preCachedData != null
            && (ascending
                ? preCachedData.getEndTime() >= curTimeRange.getMax()
                : preCachedData.getStartTime() < curTimeRange.getMin()))
        || isEndCalc(aggregators);
  }

  // skip points that cannot be calculated
  public static TsBlock skipOutOfTimeRangePoints(
      TsBlock tsBlock, TimeRange curTimeRange, boolean ascending) {
    TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
    if (ascending) {
      while (tsBlockIterator.hasNext() && tsBlockIterator.currentTime() < curTimeRange.getMin()) {
        tsBlockIterator.next();
      }
    } else {
      while (tsBlockIterator.hasNext() && tsBlockIterator.currentTime() >= curTimeRange.getMax()) {
        tsBlockIterator.next();
      }
    }
    return tsBlock.subTsBlock(tsBlockIterator.getRowIndex());
  }

  private boolean satisfied(TsBlock tsBlock, TimeRange timeRange, boolean ascending) {
    TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
    if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
      return false;
    }

    return ascending
        ? (tsBlockIterator.getEndTime() >= timeRange.getMin()
            && tsBlockIterator.currentTime() < timeRange.getMax())
        : (tsBlockIterator.getStartTime() < timeRange.getMax()
            && tsBlockIterator.currentTime() >= timeRange.getMin());
  }

  public static boolean isEndCalc(List<Aggregator> aggregators) {
    for (Aggregator aggregator : aggregators) {
      if (!aggregator.hasFinalResult()) {
        return false;
      }
    }
    return true;
  }

  private boolean isTsBlockEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.getPositionCount() == 0;
  }
}
