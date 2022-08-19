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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;

/**
 * AggregationOperator can process the situation: aggregation of intermediate aggregate result, it
 * will output one tsBlock contain many results on aggregation time intervals. One intermediate
 * tsBlock input will contain the result of many time intervals.
 */
public class AggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final List<Operator> children;
  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  private final boolean[] canCallNext;

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final List<Aggregator> aggregators;

  // using for building result tsBlock
  private final TsBlockBuilder resultTsBlockBuilder;

  private final long maxRetainedSize;
  private final long childrenRetainedSize;
  private final long maxReturnSize;

  public AggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      List<Operator> children,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;

    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.canCallNext = new boolean[inputOperatorsCount];
    for (int i = 0; i < inputOperatorsCount; i++) {
      canCallNext[i] = false;
    }

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.maxRetainedSize = children.stream().mapToLong(Operator::calculateMaxReturnSize).sum();
    this.childrenRetainedSize =
        children.stream().mapToLong(Operator::calculateRetainedSizeAfterCallingNext).sum();
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + childrenRetainedSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + childrenRetainedSize;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        canCallNext[i] = true;
      } else {
        if (isEmpty(i)) {
          listenableFutures.add(blocked);
          canCallNext[i] = true;
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
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

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      boolean hasCachedData = prepareInput();
      if (!hasCachedData) {
        break;
      }

      if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
        // move to next time window
        curTimeRange = timeRangeIterator.nextTimeRange();

        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.updateTimeRange(curTimeRange);
        }
      }

      // calculate aggregation result on current time window
      calculateNextAggregationResult();
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      return resultTsBlock;
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
    for (Operator child : children) {
      child.close();
    }
  }

  private boolean prepareInput() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (inputTsBlocks[i] != null) {
        continue;
      }
      if (!canCallNext[i]) {
        return false;
      }

      inputTsBlocks[i] = children.get(i).next();
      canCallNext[i] = false;
      if (inputTsBlocks[i] == null) {
        return false;
      }
    }
    return true;
  }

  private void calculateNextAggregationResult() {
    // consume current input tsBlocks
    for (Aggregator aggregator : aggregators) {
      aggregator.processTsBlocks(inputTsBlocks);
    }

    for (int i = 0; i < inputOperatorsCount; i++) {
      inputTsBlocks[i] = inputTsBlocks[i].skipFirst();
      if (inputTsBlocks[i].isEmpty()) {
        inputTsBlocks[i] = null;
      }
    }

    // update result using aggregators
    updateResultTsBlock();
  }

  private void updateResultTsBlock() {
    curTimeRange = null;
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
  }

  private boolean isEmpty(int index) {
    return inputTsBlocks[index] == null || inputTsBlocks[index].isEmpty();
  }
}
