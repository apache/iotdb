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
import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.db.mpp.execution.operator.window.IWindowManager;
import org.apache.iotdb.db.mpp.execution.operator.window.TimeWindowManager;

import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

/**
 * RawDataAggregationOperator is used to process raw data tsBlock input calculating using value
 * filter. It's possible that there is more than one tsBlock input in one time interval. And it's
 * also possible that one tsBlock can cover multiple time intervals too.
 *
 * <p>Since raw data query with value filter is processed by FilterOperator above TimeJoinOperator,
 * there we can see RawDataAggregateOperator as a one-to-one(one input, ont output) operator.
 *
 * <p>Return aggregation result in many time intervals once.
 */
public class RawDataAggregationOperator extends SingleInputAggregationOperator {

  private final IWindowManager windowManager;

  public RawDataAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      long maxReturnSize) {
    super(operatorContext, aggregators, child, ascending, maxReturnSize);
    this.windowManager = new TimeWindowManager(timeRangeIterator);
  }

  private boolean hasMoreData() {
    return inputTsBlock != null || child.hasNext();
  }

  @Override
  public boolean hasNext() {
    return windowManager.hasNext(hasMoreData());
  }

  @Override
  protected boolean calculateNextAggregationResult() {
    long startTime = System.nanoTime();

    while (!calculateFromRawData()) {
      long endTime = System.nanoTime();
      costTime += (endTime - startTime);
      inputTsBlock = null;

      // NOTE: child.next() can only be invoked once
      if (child.hasNext() && canCallNext) {
        inputTsBlock = child.next();
        canCallNext = false;
      } else if (child.hasNext()) {
        // if child still has next but can't be invoked now
        return false;
      } else {
        startTime = System.nanoTime();
        // If there are no points belong to last window, the last window will not
        // initialize window and aggregators
        if (!windowManager.isCurWindowInit()) {
          initWindowAndAggregators();
        }
        endTime = System.nanoTime();
        costTime += (endTime - startTime);
        startTime = endTime;
        break;
      }
      startTime = System.nanoTime();
    }

    updateResultTsBlock();
    // Step into next window
    windowManager.next();
    costTime += (System.nanoTime() - startTime);
    return true;
  }

  private boolean calculateFromRawData() {

    // if window is not initialized, we should init window status and reset aggregators
    if (!windowManager.isCurWindowInit() && !skipPreviousWindowAndInitCurWindow()) {
      return false;
    }

    // If current window has been initialized, we should judge whether inputTsBlock is empty
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return false;
    }

    if (windowManager.satisfiedCurWindow(inputTsBlock)) {

      int lastReadRowIndex = 0;
      for (Aggregator aggregator : aggregators) {
        // Current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        lastReadRowIndex = Math.max(lastReadRowIndex, aggregator.processTsBlock(inputTsBlock));
      }
      if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
        inputTsBlock = null;
        // For the last index of TsBlock, if we can know the aggregation calculation is over
        // we can directly updateResultTsBlock and return true
        return isAllAggregatorsHasFinalResult(aggregators);
      } else {
        inputTsBlock = inputTsBlock.subTsBlock(lastReadRowIndex);
        return true;
      }
    }

    boolean isTsBlockOutOfBound = windowManager.isTsBlockOutOfBound(inputTsBlock);
    return isAllAggregatorsHasFinalResult(aggregators) || isTsBlockOutOfBound;
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, aggregators, windowManager.currentOutputTime());
  }

  private boolean skipPreviousWindowAndInitCurWindow() {
    // Before we initialize windowManager and aggregators, we should ensure that we have consumed
    // all points belong to previous window
    inputTsBlock = windowManager.skipPointsOutOfCurWindow(inputTsBlock);
    // After skipping, if tsBlock is empty, we cannot ensure that we have consumed all points belong
    // to previous window, we should go back to calculateNextAggregationResult() to get a new
    // tsBlock
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return false;
    }
    // If we have consumed all points belong to previous window, we can initialize current window
    // and aggregators
    initWindowAndAggregators();
    return true;
  }

  private void initWindowAndAggregators() {
    windowManager.initCurWindow(inputTsBlock);
    IWindow curWindow = windowManager.getCurWindow();
    for (Aggregator aggregator : aggregators) {
      aggregator.updateWindow(curWindow);
    }
  }
}
