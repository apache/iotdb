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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.AggregationUtil;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockUtil;

public class TimeWindowManager implements IWindowManager {

  private TimeWindow curWindow;
  private boolean initialized;

  private boolean ascending;

  private ITimeRangeIterator timeRangeIterator;

  private boolean needSkip;

  public TimeWindowManager(ITimeRangeIterator timeRangeIterator) {
    this.timeRangeIterator = timeRangeIterator;
    this.initialized = false;
    this.curWindow = new TimeWindow(this.timeRangeIterator.nextTimeRange());
    this.ascending = timeRangeIterator.isAscending();
    // At beginning, we do not need to skip inputTsBlock
    this.needSkip = false;
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow(TsBlock tsBlock) {
    this.initialized = true;
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return this.curWindow.getCurTimeRange() != null || this.timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public void next() {
    // When we go into next window, we should pay attention to previous window whether all points
    // belong to previous window have been consumed. If not, we need skip these points.
    this.needSkip = true;
    this.initialized = false;
    this.curWindow.update(this.timeRangeIterator.nextTimeRange());
  }

  @Override
  public long currentOutputTime() {
    return timeRangeIterator.currentOutputTime();
  }

  @Override
  public IWindow getCurWindow() {
    return curWindow;
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    // If we do not need to skip, we return tsBlock directly
    if (!this.needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    int positionCount = inputTsBlock.getPositionCount();
    // Used to mark the index we could skip to.
    int skipIndex = 0;
    // If current window overlaps with inputTsBlock, we can use bisection method to find the index
    if (satisfiedCurWindow(inputTsBlock)) {
      // If ascending, find the index of first greater than or equal to targetTime
      // If !ascending, find the index of first less than or equal to targetTime
      if ((ascending && inputTsBlock.getStartTime() < curWindow.getCurMinTime())
          || (!ascending && inputTsBlock.getStartTime() > curWindow.getCurMaxTime())) {
        skipIndex =
            TsBlockUtil.getFirstConditionIndex(
                inputTsBlock, curWindow.getCurTimeRange(), ascending);
      }
    } else {
      // Here, current window does not overlap with inputTsBlock. We could skip the whole
      // inputTsBlock if the time range of inputTsBlock has been overdue compare to the time range
      // of current window.
      if ((ascending && inputTsBlock.getEndTime() < curWindow.getCurMinTime())
          || (!ascending && inputTsBlock.getEndTime() > curWindow.getCurMaxTime())) {
        skipIndex = positionCount;
      }
    }
    // This means that we have skipped all points before the time range of current window.
    if (skipIndex < positionCount) {
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(skipIndex);
  }

  @Override
  public boolean satisfiedCurWindow(TsBlock inputTsBlock) {
    return AggregationUtil.satisfiedTimeRange(inputTsBlock, curWindow.getCurTimeRange(), ascending);
  }

  @Override
  public boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return inputTsBlock != null
        && (this.ascending
            ? inputTsBlock.getEndTime() > this.curWindow.getCurMaxTime()
            : inputTsBlock.getEndTime() < this.curWindow.getCurMinTime());
  }
}
