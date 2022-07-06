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
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.skipOutOfTimeRangePoints;

public class SlidingWindowAggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator child;

  private TsBlock cachedTsBlock;

  private final List<SlidingWindowAggregator> aggregators;

  private final ITimeRangeIterator timeRangeIterator;
  private final ITimeRangeIterator subTimeRangeIterator;

  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;
  // current interval of pre-aggregation window [curStartTime, curEndTime)
  private TimeRange curSubTimeRange;

  private final boolean ascending;

  private final TsBlockBuilder resultTsBlockBuilder;

  private boolean canCallNext = true;

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
    this.resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, false);
    this.subTimeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
    this.ascending = ascending;
  }

  @Override
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    resultTsBlockBuilder.reset();
    while ((curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (!calculateNextResult()) {
        break;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      return resultTsBlockBuilder.build();
    } else {
      return null;
    }
  }

  private boolean calculateNextResult() {
    // Move to next timeRange
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      curTimeRange = timeRangeIterator.nextTimeRange();
      for (Aggregator aggregator : aggregators) {
        aggregator.updateTimeRange(curTimeRange);
      }
    }

    // Calculate aggregation result based on current time window
    while (!isEndCalc()) {
      if (cachedTsBlock == null) {
        // child.next can only be invoked once
        if (child.hasNext()) {
          if (canCallNext) {
            cachedTsBlock = child.next();
            canCallNext = false;
          } else {
            // if child still has next but can't be invoked now
            return false;
          }
        } else {
          break;
        }
      }
      calcFromTsBlock();
    }

    // Update result using aggregators
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
    curTimeRange = null;

    return true;
  }

  protected boolean isEndCalc() {
    if (curSubTimeRange == null && !subTimeRangeIterator.hasNextTimeRange()) {
      return true;
    }

    if (curSubTimeRange == null && subTimeRangeIterator.hasNextTimeRange()) {
      curSubTimeRange = subTimeRangeIterator.nextTimeRange();
    }
    return ascending
        ? curSubTimeRange.getMin() >= curTimeRange.getMax()
        : curSubTimeRange.getMax() <= curTimeRange.getMin();
  }

  private void calcFromTsBlock() {
    if (cachedTsBlock == null || cachedTsBlock.isEmpty()) {
      return;
    }

    // skip points that cannot be calculated
    if ((ascending && cachedTsBlock.getStartTime() < curSubTimeRange.getMin())
        || (!ascending && cachedTsBlock.getStartTime() > curSubTimeRange.getMax())) {
      cachedTsBlock = skipOutOfTimeRangePoints(cachedTsBlock, curSubTimeRange, ascending);
    }

    int lastReadRowIndex = 0;
    for (SlidingWindowAggregator aggregator : aggregators) {
      lastReadRowIndex = Math.max(lastReadRowIndex, aggregator.processTsBlock(cachedTsBlock));
    }
    if (lastReadRowIndex >= cachedTsBlock.getPositionCount()) {
      cachedTsBlock = null;
    } else {
      cachedTsBlock = cachedTsBlock.subTsBlock(lastReadRowIndex);
    }
    curSubTimeRange = null;
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
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }
}
