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

package org.apache.iotdb.db.queryengine.execution.operator.window;

import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public class CountWindowManager implements IWindowManager {

  private final CountWindow countWindow;

  private boolean needSkip;
  private boolean initialized;

  public CountWindowManager(CountWindowParameter countWindowParameter) {
    this.countWindow = new CountWindow(countWindowParameter);
    this.needSkip = false;
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow() {
    this.initialized = true;
    countWindow.resetCurCount();
    countWindow.setStartTime(Long.MAX_VALUE);
    countWindow.setEndTime(Long.MIN_VALUE);
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return hasMoreData;
  }

  @Override
  public void next() {
    this.needSkip = true;
    this.initialized = false;
  }

  @Override
  public IWindow getCurWindow() {
    return countWindow;
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    if (!needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    Column timeColumn = inputTsBlock.getTimeColumn();
    Column controlColumn = countWindow.getControlColumn(inputTsBlock);
    long leftCount = countWindow.getLeftCount();
    int i = 0;
    int size = inputTsBlock.getPositionCount();

    for (; i < size && leftCount != 0; i++) {

      if (isIgnoringNull() && controlColumn.isNull(i)) {
        continue;
      } else {
        // A Count Window has exactly the row number of countNumber
        // if leftCount is zero, the window is finished.
        leftCount--;
      }

      long currentTime = timeColumn.getLong(i);
      // judge whether we need update endTime
      if (countWindow.getStartTime() > currentTime) {
        countWindow.setStartTime(currentTime);
      }
      // judge whether we need update endTime
      if (countWindow.getEndTime() < currentTime) {
        countWindow.setEndTime(currentTime);
      }
    }

    countWindow.setLeftCount(leftCount);

    // we can create a new window beginning at index i of inputTsBlock
    if (i < size) {
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(i);
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output endTime column.
    if (countWindow.isNeedOutputEndTime()) {
      dataTypes.add(0, TSDataType.INT64);
    }
    return new TsBlockBuilder(dataTypes);
  }

  @Override
  public void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {
    if (countWindow.getLeftCount() != 0) {
      return;
    }
    long endTime = countWindow.isNeedOutputEndTime() ? countWindow.getEndTime() : -1;
    outputAggregators(aggregators, resultTsBlockBuilder, countWindow.getStartTime(), endTime);
  }

  @Override
  public boolean needSkipInAdvance() {
    return true;
  }

  @Override
  public boolean isIgnoringNull() {
    return countWindow.isIgnoreNull();
  }
}
