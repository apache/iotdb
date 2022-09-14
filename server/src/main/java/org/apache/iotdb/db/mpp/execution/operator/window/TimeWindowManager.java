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

  public TimeWindowManager(ITimeRangeIterator timeRangeIterator) {
    this.timeRangeIterator = timeRangeIterator;
    this.initialized = false;
    this.curWindow = new TimeWindow();
    this.ascending = timeRangeIterator.isAscending();
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow(TsBlock tsBlock) {
    this.curWindow.update(this.timeRangeIterator.nextTimeRange());
    this.initialized = true;
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return this.curWindow.getCurTimeRange() != null || this.timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public void next() {
    this.initialized = false;
    this.curWindow.update(null);
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
    if ((ascending && inputTsBlock.getStartTime() < curWindow.getCurMinTime())
        || (!ascending && inputTsBlock.getStartTime() > curWindow.getCurMaxTime())) {
      return TsBlockUtil.skipPointsOutOfTimeRange(
          inputTsBlock, curWindow.getCurTimeRange(), ascending);
    }
    return inputTsBlock;
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
