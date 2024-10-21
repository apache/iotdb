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

package org.apache.iotdb.db.queryengine.execution.operator.window;

import org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory.KeepEvaluator;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public class ConditionWindowManager implements IWindowManager {

  private final ConditionWindow conditionWindow;
  private boolean initialized;
  private boolean needSkip;
  private boolean isFirstSkip;
  private final KeepEvaluator keepEvaluator;

  public ConditionWindowManager(ConditionWindowParameter conditionWindowParameter) {
    this.conditionWindow = new ConditionWindow(conditionWindowParameter);
    // In group by condition, the first data point cannot be guaranteed to be true in controlColumn,
    // so there is going to be a skipPointsOutOfBounds() in the beginning.
    this.needSkip = true;
    this.keepEvaluator =
        AccumulatorFactory.initKeepEvaluator(conditionWindowParameter.getKeepExpression());
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow() {
    this.initialized = true;
    this.conditionWindow.setTimeInitialized(false);
    this.conditionWindow.setKeep(0);
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return hasMoreData;
  }

  @Override
  public void next() {
    this.needSkip = true;
    this.initialized = false;
    isFirstSkip = true;
  }

  @Override
  public IWindow getCurWindow() {
    return conditionWindow;
  }

  /** skip the row remains in the current window(controlColumn is true). */
  private boolean skipFirstPhrase(Column controlColumn, int index) {
    if (!isFirstSkip) {
      return false;
    }

    return controlColumn.isNull(index) || !controlColumn.getBoolean(index);
  }

  /** skip the row which don't belong to any window(controlColumn is false or null). */
  private boolean skipSecondPhrase(Column controlColumn, int index) {
    if (isFirstSkip) {
      return false;
    }

    return !controlColumn.isNull(index) && controlColumn.getBoolean(index);
  }

  private boolean needBreak(Column controlColumn, int index) {
    if (isIgnoringNull() && controlColumn.isNull(index)) {
      return false;
    }

    return skipFirstPhrase(controlColumn, index) || skipSecondPhrase(controlColumn, index);
  }

  private void updateTime(long currentTime) {
    if (conditionWindow.getStartTime() > currentTime) {
      conditionWindow.setStartTime(currentTime);
    }
    if (conditionWindow.getEndTime() < currentTime) {
      conditionWindow.setEndTime(currentTime);
    }
  }

  /**
   * skipPointsOutOfBound has two phrases in ConditionWindowManager. First phrase is to skip the row
   * with the controlColumn of true in current window, which usually happens when FIRST_VALUE() or
   * MAX_TIME() leaves early in aggregator. Second phrase is to skip the row with the controlColumn
   * of false/null which don't belong to current window. isFirstSkip is used to identify the phrase.
   */
  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {

    if (!needSkip || inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    Column controlColumn = conditionWindow.getControlColumn(inputTsBlock);
    Column timeColumn = inputTsBlock.getTimeColumn();
    int i = 0;
    int k = 0;
    int size = inputTsBlock.getPositionCount();
    for (; i < size && !needBreak(controlColumn, i); i++) {

      // if ignoreNull is true, ignore the controlColumn of null.
      if (isIgnoringNull() && controlColumn.isNull(i)) {
        continue;
      }

      // update endTime and record the row processed, only the first phrase of skip in current
      // window need to record them.
      if (isFirstSkip) {
        k++;
        updateTime(timeColumn.getLong(i));
      }
    }

    // record the row processed in the first phrase of skip. If the tsBlock is null, the skip may
    // not finish.
    if (isFirstSkip) {
      if (i != size) {
        isFirstSkip = false;
      }
      conditionWindow.setKeep(conditionWindow.getKeep() + k);
      return inputTsBlock.subTsBlock(i);
    }

    if (i < size) {
      // we can create a new window beginning at index i of inputTsBlock.
      needSkip = false;
    }
    return inputTsBlock.subTsBlock(i);
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<TreeAggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output endTime column.
    if (conditionWindow.isOutputEndTime()) {
      dataTypes.add(0, TSDataType.INT64);
    }
    return new TsBlockBuilder(dataTypes);
  }

  @Override
  public void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<TreeAggregator> aggregators) {
    if (!keepEvaluator.apply(conditionWindow.getKeep())) {
      return;
    }
    long endTime = conditionWindow.isOutputEndTime() ? conditionWindow.getEndTime() : -1;
    outputAggregators(aggregators, resultTsBlockBuilder, conditionWindow.getStartTime(), endTime);
  }

  @Override
  public boolean needSkipInAdvance() {
    return true;
  }

  @Override
  public boolean isIgnoringNull() {
    return conditionWindow.ignoringNull();
  }
}
