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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator.initTimeRangeIterator;

/**
 * AggregationOperator can process the situation: aggregation of intermediate aggregate result, it
 * will output one result based on time interval. One intermediate tsBlock input will only contain
 * the result of one time interval exactly.
 */
public class AggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Aggregator> aggregators;
  private final List<Operator> children;

  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  private final TsBlockBuilder tsBlockBuilder;

  private final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  public AggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      List<Operator> children,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputPartialTimeWindow) {
    this.operatorContext = operatorContext;
    this.aggregators = aggregators;
    this.children = children;

    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, outputPartialTimeWindow);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    // update input tsBlock
    if (curTimeRange == null && timeRangeIterator.hasNextTimeRange()) {
      curTimeRange = timeRangeIterator.nextTimeRange();
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (inputTsBlocks[i] != null) {
        continue;
      }
      inputTsBlocks[i] = children.get(i).next();
      if (inputTsBlocks[i] == null) {
        return null;
      }
    }
    // consume current input tsBlocks
    for (Aggregator aggregator : aggregators) {
      aggregator.reset();
      aggregator.processTsBlocks(inputTsBlocks);
    }
    // output result from aggregator
    curTimeRange = null;
    for (int i = 0; i < inputOperatorsCount; i++) {
      inputTsBlocks[i] = null;
    }
    return updateResultTsBlockFromAggregators(tsBlockBuilder, aggregators, timeRangeIterator);
  }

  @Override
  public boolean hasNext() {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  public static TsBlock updateResultTsBlockFromAggregators(
      TsBlockBuilder tsBlockBuilder,
      List<? extends Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator) {
    tsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    timeColumnBuilder.writeLong(timeRangeIterator.currentOutputTime());
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    for (Aggregator aggregator : aggregators) {
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
}
