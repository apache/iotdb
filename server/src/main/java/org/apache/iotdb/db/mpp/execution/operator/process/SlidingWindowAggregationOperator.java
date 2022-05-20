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

import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SlidingWindowAggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Operator> children;
  private final int inputOperatorsCount;

  private final TsBlock[] inputTsBlocks;

  private final List<SlidingWindowAggregator> aggregators;
  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final TsBlockBuilder tsBlockBuilder;

  public SlidingWindowAggregationOperator(
      OperatorContext operatorContext,
      List<SlidingWindowAggregator> aggregators,
      List<Operator> children,
      List<TSDataType> outputDataTypes,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.operatorContext = operatorContext;
    this.aggregators = aggregators;
    this.children = children;

    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending);
  }

  @Override
  public boolean hasNext() {
    return timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    // update input tsBlock
    curTimeRange = timeRangeIterator.nextTimeRange();
    for (int i = 0; i < inputOperatorsCount; i++) {
      inputTsBlocks[i] = children.get(i).next();
    }
    // consume current input tsBlocks
    for (SlidingWindowAggregator aggregator : aggregators) {
      aggregator.setTimeRange(curTimeRange);
      aggregator.processTsBlocks(inputTsBlocks);
    }
    // output result from aggregator
    return null;
  }

  public static TsBlock updateResultTsBlockFromAggregators(
      TsBlockBuilder tsBlockBuilder,
      List<SlidingWindowAggregator> aggregators,
      ITimeRangeIterator timeRangeIterator) {
    tsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    timeColumnBuilder.writeLong(timeRangeIterator.currentOutputTime());
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    for (SlidingWindowAggregator aggregator : aggregators) {
      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.getOutputType().length];
      columnBuilder[0] = columnBuilders[columnIndex++];
      if (columnBuilder.length > 1) {
        columnBuilder[1] = columnBuilders[columnIndex++];
      }
      aggregator.outputResult(columnBuilder);
    }
    tsBlockBuilder.declarePosition();
    return tsBlockBuilder.build();
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      ListenableFuture<Void> blocked = children.get(i).isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
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

  public static ITimeRangeIterator initTimeRangeIterator(
      GroupByTimeParameter groupByTimeParameter, boolean ascending) {
    checkArgument(groupByTimeParameter != null, "");
    return TimeRangeIteratorFactory.getTimeRangeIterator(
        groupByTimeParameter.getStartTime(),
        groupByTimeParameter.getEndTime(),
        groupByTimeParameter.getInterval(),
        groupByTimeParameter.getSlidingStep(),
        ascending,
        groupByTimeParameter.isIntervalByMonth(),
        groupByTimeParameter.isSlidingStepByMonth(),
        groupByTimeParameter.isLeftCRightO(),
        false);
  }
}
