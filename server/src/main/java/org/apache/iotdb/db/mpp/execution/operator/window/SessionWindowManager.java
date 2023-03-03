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

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.util.List;

public class SessionWindowManager implements IWindowManager {

  private final boolean isNeedOutputEndTime;

  private boolean initialized;

  private boolean needSkip;

  private final SessionWindow sessionWindow;

  public SessionWindowManager(boolean isNeedOutputEndTime, long timeInterval, boolean ascending) {
    this.isNeedOutputEndTime = isNeedOutputEndTime;
    this.initialized = false;
    // At beginning, we do not need to skip inputTsBlock
    this.needSkip = false;
    this.sessionWindow = new SessionWindow(timeInterval, ascending);
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow() {
    this.initialized = true;
    this.sessionWindow.setInitializedTimeValue(false);
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return hasMoreData;
  }

  @Override
  public void next() {
    // When we go into next window, we should pay attention to previous window whether all points
    // belong to previous window have been consumed. If not, we need skip these points.
    this.needSkip = true;
    this.initialized = false;
    this.sessionWindow.setLastTsBlockTime(0);
  }

  @Override
  public IWindow getCurWindow() {
    return sessionWindow;
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    if (!needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    TimeColumn timeColumn = inputTsBlock.getTimeColumn();
    int i = 0, size = inputTsBlock.getPositionCount();
    long previousTimeValue = sessionWindow.getTimeValue();

    for (; i < size; i++) {
      long currentTime = timeColumn.getLong(i);
      if (Math.abs(currentTime - previousTimeValue) > sessionWindow.getTimeInterval()) {
        sessionWindow.setTimeValue(previousTimeValue);
        break;
      }
      // judge whether we need update endTime
      if (sessionWindow.getStartTime() > currentTime) {
        sessionWindow.setStartTime(currentTime);
      }
      // judge whether we need update endTime
      if (sessionWindow.getEndTime() < currentTime) {
        sessionWindow.setEndTime(currentTime);
      }
      previousTimeValue = currentTime;
    }

    // we can create a new window beginning at index i of inputTsBlock
    if (i < size) {
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(i);
  }

  @Override
  public boolean satisfiedCurWindow(TsBlock inputTsBlock) {
    return true;
  }

  @Override
  public boolean isTsBlockOutOfBound(TsBlock inputTsBlock) {
    return false;
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output endTime column.
    if (isNeedOutputEndTime) {
      dataTypes.add(0, TSDataType.INT64);
    }
    return new TsBlockBuilder(dataTypes);
  }

  @Override
  public void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {
    long endTime = isNeedOutputEndTime ? sessionWindow.getEndTime() : -1;
    outputAggregators(aggregators, resultTsBlockBuilder, sessionWindow.getStartTime(), endTime);
  }

  @Override
  public boolean needSkipInAdvance() {
    return isNeedOutputEndTime;
  }

  @Override
  public boolean isIgnoringNull() {
    return false;
  }
}
