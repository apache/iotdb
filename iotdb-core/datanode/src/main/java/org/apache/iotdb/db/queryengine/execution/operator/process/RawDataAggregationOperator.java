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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.IWindowManager;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowParameter;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;
import static org.apache.iotdb.db.queryengine.execution.operator.window.WindowManagerFactory.genWindowManager;

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

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RawDataAggregationOperator.class);

  private final IWindowManager windowManager;

  // needSkip is the signal to determine whether to skip the points out of current window to get
  // endTime when the resultSet needs to output endTime.
  // If the resultSet needs endTime, needSkip will be set to true when the operator is skipping the
  // points out of current window.
  private boolean needSkip = false;

  // child.hasNext() may return true even there is no more data, the operator may exit without
  // updating the cached data in aggregator.
  // We need hasCachedDataInAggregator to prevent operator exit when there is cached data in
  // aggregators.
  private boolean hasCachedDataInAggregator = false;

  public RawDataAggregationOperator(
      OperatorContext operatorContext,
      List<TreeAggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      long maxReturnSize,
      WindowParameter windowParameter) {
    super(operatorContext, aggregators, child, ascending, maxReturnSize);
    this.windowManager = genWindowManager(windowParameter, timeRangeIterator, ascending);
    this.resultTsBlockBuilder = windowManager.createResultTsBlockBuilder(aggregators);
  }

  private boolean hasMoreData() throws Exception {
    return !(inputTsBlock == null || inputTsBlock.isEmpty())
        || child.hasNextWithTimer()
        || hasCachedDataInAggregator;
  }

  @Override
  public boolean hasNext() throws Exception {
    return windowManager.hasNext(hasMoreData());
  }

  @SuppressWarnings({"squid:S3776", "squid:S135"})
  @Override
  protected boolean calculateNextAggregationResult() throws Exception {

    // if needSkip is true, just get the tsBlock directly.
    while (needSkip || !calculateFromRawData()) {
      inputTsBlock = null;

      // NOTE: child.next() can only be invoked once
      if (child.hasNextWithTimer() && canCallNext) {
        inputTsBlock = child.nextWithTimer();
        canCallNext = false;
        if (needSkip) {
          break;
        }
      } else if (child.hasNextWithTimer()) {
        // if child still has next but can't be invoked now
        return false;
      } else {
        // If there are no points belong to last time window, the last time window will not
        // initialize window and aggregators. Specially for time window.
        if (windowManager.notInitializedLastTimeWindow()) {
          initWindowAndAggregators();
        }
        // If the window is not initialized, it just returns to avoid invoking
        // updateResultTsBlock()
        // but if it's skipping the last window, just break and keep skipping.
        if (needSkip || windowManager.isCurWindowInit()) {
          break;
        }
        return false;
      }
    }

    // Step into next window
    // if needSkip is true, don't need to enter next window again
    if (!needSkip) {
      windowManager.next();
    }
    // When some windows without cached endTime trying to output endTime,
    // they need to skip the points in lastWindow in advance to get endTime
    if (windowManager.needSkipInAdvance()) {
      needSkip = true;
      inputTsBlock = windowManager.skipPointsOutOfCurWindow(inputTsBlock);
      if ((inputTsBlock == null || inputTsBlock.isEmpty()) && child.hasNextWithTimer()) {
        return canCallNext;
      }
      needSkip = false;
    }

    updateResultTsBlock();
    // After updating, the data in aggregators is consumed.
    hasCachedDataInAggregator = false;

    return true;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135"})
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

      // Get the indexes in tsBlock which needs to be processed by aggregator, and the last row
      // needed to be processed.
      int tsBlockSize = inputTsBlock.getPositionCount();
      IWindow curWindow = windowManager.getCurWindow();

      Column[] controlAndTimeColumn = new Column[2];
      controlAndTimeColumn[0] = curWindow.getControlColumn(inputTsBlock);
      controlAndTimeColumn[1] = inputTsBlock.getTimeColumn();

      BitMap needProcess = new BitMap(tsBlockSize);
      int lastIndexToProcess = -1;
      boolean hasSkip = false;

      for (int i = 0; i < tsBlockSize; i++) {
        if (windowManager.isIgnoringNull() && controlAndTimeColumn[0].isNull(i)) {
          lastIndexToProcess = i;
          hasSkip = true;
          continue;
        }
        if (!curWindow.satisfy(controlAndTimeColumn[0], i)) {
          break;
        }
        needProcess.mark(i);
        curWindow.mergeOnePoint(controlAndTimeColumn, i);
        lastIndexToProcess = i;
      }

      // if no row needs to skip, just send a null parameter.
      if (!hasSkip) {
        needProcess = null;
      }

      TsBlock inputRegion = inputTsBlock.getRegion(0, lastIndexToProcess + 1);
      for (TreeAggregator aggregator : aggregators) {
        // Current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        aggregator.processTsBlock(inputRegion, needProcess);
      }
      int lastReadRowIndex = lastIndexToProcess + 1;
      // If lastReadRowIndex is not zero, some of tsBlock is consumed and result is cached in
      // aggregators.
      if (lastReadRowIndex != 0) {
        hasCachedDataInAggregator = true;
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
    windowManager.appendAggregationResult(resultTsBlockBuilder, aggregators);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes());
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
    windowManager.initCurWindow();
    for (TreeAggregator aggregator : aggregators) {
      aggregator.reset();
    }
  }
}
