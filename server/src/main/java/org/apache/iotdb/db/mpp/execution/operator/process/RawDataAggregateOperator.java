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
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.utils.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregateScanOperator.initTimeRangeIterator;

/**
 * RawDataAggregateOperator is used to process raw data tsBlock input calculating using value
 * filter. It's possible that there is more than one tsBlock input in one time interval. And it's
 * also possible that one tsBlock can cover multiple time intervals too.
 */
public class RawDataAggregateOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Aggregator> aggregators;
  private final List<Operator> children;

  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  private final TsBlockBuilder tsBlockBuilder;

  private ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  public RawDataAggregateOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      List<Operator> children,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
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
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return ProcessOperator.super.isBlocked();
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void close() throws Exception {
    ProcessOperator.super.close();
  }

  @Override
  public boolean isFinished() {
    return false;
  }
}
