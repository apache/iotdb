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
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;

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
  private final boolean ascending;

  private final Operator child;
  private TsBlock inputTsBlock;
  private boolean canCallNext;

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final List<Aggregator> aggregators;

  // using for building result tsBlock
  private final TsBlockBuilder resultTsBlockBuilder;

  public RawDataAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      Operator child,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.operatorContext = operatorContext;
    this.ascending = ascending;
    this.child = child;
    this.aggregators = aggregators;

    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);
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
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    // reset operator state
    resultTsBlockBuilder.reset();
    canCallNext = true;

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
        // move to next time window
        curTimeRange = timeRangeIterator.nextTimeRange();

        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.updateTimeRange(curTimeRange);
        }
      }

      // calculate aggregation result on current time window
      if (!calculateNextAggregationResult()) {
        break;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      return resultTsBlockBuilder.build();
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  private boolean calculateNextAggregationResult() {
    while (!calculateAggregationFromRawData(inputTsBlock, aggregators, curTimeRange, ascending)) {
      inputTsBlock = null;

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

    // update result using aggregators
    updateResultTsBlock();

    return true;
  }

  private void updateResultTsBlock() {
    curTimeRange = null;
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
  }
}
