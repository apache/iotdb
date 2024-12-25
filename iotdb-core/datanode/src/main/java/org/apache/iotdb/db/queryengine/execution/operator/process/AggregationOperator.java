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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;

/**
 * {@link AggregationOperator} can process the situation: aggregation of intermediate aggregate
 * result, it will output one tsBlock contain many results on aggregation time intervals. One
 * intermediate tsBlock input will contain the result of many time intervals.
 */
public class AggregationOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AggregationOperator.class);

  private final ITimeRangeIterator timeRangeIterator;
  // Current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final List<TreeAggregator> aggregators;

  // Using for building result tsBlock
  private final TsBlockBuilder resultTsBlockBuilder;

  private final long maxRetainedSize;
  private final long childrenRetainedSize;
  private final boolean outputEndTime;

  public AggregationOperator(
      OperatorContext operatorContext,
      List<TreeAggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      List<Operator> children,
      boolean outputEndTime,
      long maxReturnSize) {
    super(operatorContext, children);
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;
    List<TSDataType> dataTypes = new ArrayList<>();
    if (outputEndTime) {
      dataTypes.add(TSDataType.INT64);
    }
    for (TreeAggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.childrenRetainedSize =
        children.stream().mapToLong(Operator::calculateRetainedSizeAfterCallingNext).sum();
    this.maxRetainedSize =
        childrenRetainedSize == 0
            ? 0
            : children.stream().mapToLong(Operator::calculateMaxReturnSize).sum();
    this.outputEndTime = outputEndTime;
    this.maxReturnSize = maxReturnSize;
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
  public boolean hasNext() throws Exception {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() throws Exception {
    // Start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (!prepareInput()) {
        break;
      }

      if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
        // Move to next time window
        curTimeRange = timeRangeIterator.nextTimeRange();

        // Clear previous aggregation result
        for (TreeAggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      // Calculate aggregation result on current time window
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
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  private void calculateNextAggregationResult() {
    // Consume current input tsBlocks
    for (TreeAggregator aggregator : aggregators) {
      aggregator.processTsBlocks(inputTsBlocks);
    }

    for (int i = 0; i < inputOperatorsCount; i++) {
      inputTsBlocks[i] = inputTsBlocks[i].skipFirst();
      if (inputTsBlocks[i].isEmpty()) {
        inputTsBlocks[i] = null;
      }
    }

    // Update result using aggregators
    updateResultTsBlock();
  }

  private void updateResultTsBlock() {
    if (!outputEndTime) {
      appendAggregationResult(
          resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
    } else {
      appendAggregationResult(
          resultTsBlockBuilder,
          aggregators,
          timeRangeIterator.currentOutputTime(),
          curTimeRange.getMax());
    }
    curTimeRange = null;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(canCallNext)
        + resultTsBlockBuilder.getRetainedSizeInBytes();
  }
}
