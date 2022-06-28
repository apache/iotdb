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
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator.initTimeRangeIterator;

/**
 * RawDataAggregationOperator is used to process raw data tsBlock input calculating using value
 * filter. It's possible that there is more than one tsBlock input in one time interval. And it's
 * also possible that one tsBlock can cover multiple time intervals too.
 *
 * <p>Since raw data query with value filter is processed by FilterOperator above TimeJoinOperator,
 * there we can see RawDataAggregateOperator as a one-to-one(one input, ont output) operator.
 *
 * <p>Return aggregation result in one time interval once.
 */
public class RawDataAggregationOperator implements ProcessOperator {

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

  public RawDataAggregationOperator(
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
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    // Move to next timeRange
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      curTimeRange = timeRangeIterator.nextTimeRange();
      for (Aggregator aggregator : aggregators) {
        aggregator.reset();
        aggregator.updateTimeRange(curTimeRange);
      }
    }

    // 1. Calculate aggregation result based on current time window
    boolean canCallNext = true;
    while (!calcFromCacheData(curTimeRange)) {
      preCachedData = null;
      // child.next can only be invoked once
      if (child.hasNext() && canCallNext) {
        preCachedData = child.next();
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
    return AggregationOperator.updateResultTsBlockFromAggregators(
        tsBlockBuilder, aggregators, timeRangeIterator);
  }

  @Override
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
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
    if (preCachedData == null || preCachedData.isEmpty()) {
      return false;
    }
    // check if the batchData does not contain points in current interval
    if (satisfied(preCachedData, curTimeRange, ascending)) {
      // skip points that cannot be calculated
      if ((ascending && preCachedData.getStartTime() < curTimeRange.getMin())
          || (!ascending && preCachedData.getStartTime() > curTimeRange.getMax())) {
        preCachedData = skipOutOfTimeRangePoints(preCachedData, curTimeRange, ascending);
      }

      int lastReadRowIndex = 0;
      for (Aggregator aggregator : aggregators) {
        // current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        lastReadRowIndex = Math.max(lastReadRowIndex, aggregator.processTsBlock(preCachedData));
      }
      if (lastReadRowIndex >= preCachedData.getPositionCount()) {
        preCachedData = null;
      } else {
        preCachedData = preCachedData.subTsBlock(lastReadRowIndex);
      }
    }
    // The result is calculated from the cache
    return (preCachedData != null
            && (ascending
                ? preCachedData.getEndTime() > curTimeRange.getMax()
                : preCachedData.getEndTime() < curTimeRange.getMin()))
        || isEndCalc(aggregators);
  }

  // skip points that cannot be calculated
  public static TsBlock skipOutOfTimeRangePoints(
      TsBlock tsBlock, TimeRange curTimeRange, boolean ascending) {
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? curTimeRange.getMin() : curTimeRange.getMax();
    int left = 0, right = timeColumn.getPositionCount() - 1, mid;
    // if ascending, find the first greater than or equal to targetTime
    // else, find the first less than or equal to targetTime
    while (left < right) {
      mid = (left + right) >> 1;
      if (timeColumn.getLongWithoutCheck(mid) < targetTime) {
        if (ascending) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) > targetTime) {
        if (ascending) {
          right = mid;
        } else {
          left = mid + 1;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) == targetTime) {
        return tsBlock.subTsBlock(mid);
      }
    }
    return tsBlock.subTsBlock(left);
  }

  public static boolean satisfied(TsBlock tsBlock, TimeRange timeRange, boolean ascending) {
    TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
    if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
      return false;
    }

    return ascending
        ? (tsBlockIterator.getEndTime() >= timeRange.getMin()
            && tsBlockIterator.currentTime() <= timeRange.getMax())
        : (tsBlockIterator.getEndTime() <= timeRange.getMax()
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
}
