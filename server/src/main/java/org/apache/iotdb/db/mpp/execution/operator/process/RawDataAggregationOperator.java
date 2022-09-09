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

  public RawDataAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      long maxReturnSize) {
    super(operatorContext, aggregators, child, ascending, timeRangeIterator, maxReturnSize);
  }

  @Override
  public boolean hasNext() {
    if (finish) {
      return false;
    }
    return inputTsBlock != null || child.hasNext() || windowManager.hasNext();
  }

  @Override
  protected boolean calculateNextAggregationResult() {
    while (!calculateAndUpdateFromRawData()) {
      inputTsBlock = null;

      // NOTE: child.next() can only be invoked once
      if (child.hasNext() && canCallNext) {
        inputTsBlock = child.next();
        canCallNext = false;
      } else if (child.hasNext()) {
        // if child still has next but can't be invoked now
        return false;
      } else {
        if (!windowManager.isCurWindowInit()) {
          initWindowManagerAndAggregators();
        }
        updateResultTsBlock();
        break;
      }
    }

    return true;
  }

  private boolean calculateAndUpdateFromRawData() {
    // 待使用的inputTsBlock为空就直接return false
    // 在calculateNextAggregationResult方法中，就会向子节点拿下一个TsBlock
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return false;
    }

    // 如果当前窗口还没有初始化，那么需要先初始化
    // 主要是窗口的一些状态值比如时间窗口的时间范围TimeRange、状态窗口的第一个值用于后面做比较等
    // 然后使用aggregator.updateWindow方法，清空之前aggregator的聚合结果。
    if (!windowManager.isCurWindowInit()) {
      // init 才是真正的给当前窗口赋值
      initWindowManagerAndAggregators();
    }

    // 如果inputTsBlock满足当前窗口的时间范围
    // 对于时间窗口来说，就是之前的逻辑
    // 对于其他三种窗口来说，其实并不需要对时间范围进行判断，因为每一点都会处于窗口内
    // 但是，因为我们拿到的TsBlock的时间戳范围是会超过用户给定的时间范围的，所以这里也需要进行判断
    // 比如 用户给定[100,200]时间范围，数据共有[0,1000]，这时候TsBlock中的时间戳是可能会超过200的
    // 所以不能把TsBlock中所有的数据点全部用于窗口，也需要时间范围的判断
    if (windowManager.satisfiedTimeRange(inputTsBlock)) {
      inputTsBlock = windowManager.skipPointsOutOfTimeRange(inputTsBlock);

      int lastReadRowIndex = 0;
      for (Aggregator aggregator : aggregators) {
        // current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        lastReadRowIndex = Math.max(lastReadRowIndex, aggregator.processTsBlock(inputTsBlock));
      }
      if (lastReadRowIndex >= inputTsBlock.getPositionCount()) {
        inputTsBlock = null;
        // 用于判断如果TsBlock刚好用完，聚合窗口是否也刚好结束。
        // NOTE: 但是
        // 如果旧的TsBlock还剩8个点，但是窗口共10个点。也就是新的TsBlock向后再找两个点窗口才结束。
        // 按目前的FirstValue的addIntInput逻辑，第一个数据就会把candidate记为true，所以下面这个条件会为true
        // candidate的逻辑是不是要在merge中写？
        if (isAllAggregatorsHasFinalResult(aggregators)) {
          updateResultTsBlock();
          return true;
        }
        return false;
      } else {
        inputTsBlock = inputTsBlock.subTsBlock(lastReadRowIndex);
        updateResultTsBlock();
        return true;
      }
    }
    boolean isTsBlockOutOfBound = windowManager.isTsBlockOutOfBound(inputTsBlock);
    boolean canMakeWindow = isAllAggregatorsHasFinalResult(aggregators) || isTsBlockOutOfBound;
    if (canMakeWindow) {
      updateResultTsBlock();
    }
    return canMakeWindow;
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, aggregators, windowManager.currentOutputTime());
    // 对于其他三种窗口来说，可能只能返回true？但是什么时候false呢，怎么知道窗口已经超出时间范围？
    if (windowManager.hasNext()) {
      // 这里是清空窗口数据，置为notInit
      windowManager.genNextWindow();
    } else {
      finish = true;
    }
  }

  private void initWindowManagerAndAggregators() {
    windowManager.initCurWindow(inputTsBlock);
    IWindow curWindow = windowManager.getCurWindow();
    for (Aggregator aggregator : aggregators) {
      aggregator.updateWindow(curWindow);
    }
  }
}
