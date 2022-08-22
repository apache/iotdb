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

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;

public abstract class SingleInputAggregationOperator implements ProcessOperator {

  protected final OperatorContext operatorContext;
  protected final boolean ascending;

  protected final Operator child;
  protected TsBlock inputTsBlock;
  protected boolean canCallNext;

  protected final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;

  protected final List<Aggregator> aggregators;

  // using for building result tsBlock
  protected final TsBlockBuilder resultTsBlockBuilder;

  protected final long maxRetainedSize;
  protected final long maxReturnSize;

  public SingleInputAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      Operator child,
      boolean ascending,
      ITimeRangeIterator timeRangeIterator,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.ascending = ascending;
    this.child = child;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.maxRetainedSize = child.calculateMaxReturnSize();
    this.maxReturnSize = maxReturnSize;
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
    child.close();
  }

  protected abstract boolean calculateNextAggregationResult();

  protected void updateResultTsBlock() {
    curTimeRange = null;
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }
}
