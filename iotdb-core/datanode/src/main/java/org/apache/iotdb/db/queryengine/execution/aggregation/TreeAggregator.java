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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.AGGREGATION_FROM_RAW_DATA;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.AGGREGATION_FROM_STATISTICS;

public class TreeAggregator {

  protected final Accumulator accumulator;
  // In some intermediate result input, inputLocation[] should include two columns
  protected List<InputLocation[]> inputLocationList;
  protected final AggregationStep step;
  public static final QueryExecutionMetricSet QUERY_EXECUTION_METRICS =
      QueryExecutionMetricSet.getInstance();

  // Used for SeriesAggregateScanOperator
  public TreeAggregator(Accumulator accumulator, AggregationStep step) {
    this.accumulator = accumulator;
    this.step = step;
    this.inputLocationList =
        Collections.singletonList(new InputLocation[] {new InputLocation(0, 0)});
  }

  // Used for AggregateOperator, AlignedSeriesAggregateScanOperator
  public TreeAggregator(
      Accumulator accumulator, AggregationStep step, List<InputLocation[]> inputLocationList) {
    this.accumulator = accumulator;
    this.step = step;
    this.inputLocationList = inputLocationList;
  }

  // Used for SeriesAggregateScanOperator and RawDataAggregateOperator
  public void processTsBlock(TsBlock tsBlock, BitMap bitMap) {
    long startTime = System.nanoTime();
    try {
      checkArgument(
          step.isInputRaw(),
          "Step in SeriesAggregateScanOperator and RawDataAggregateOperator can only process raw input");
      for (InputLocation[] inputLocations : inputLocationList) {
        Column[] timeAndValueColumn = new Column[1 + inputLocations.length];
        timeAndValueColumn[0] = tsBlock.getTimeColumn();
        for (int i = 0; i < inputLocations.length; i++) {
          checkArgument(
              inputLocations[i].getTsBlockIndex() == 0,
              "RawDataAggregateOperator can only process one tsBlock input.");
          int index = inputLocations[i].getValueColumnIndex();
          // for count_time, time column is also its value column
          // for max_by, the input column can also be time column.
          timeAndValueColumn[1 + i] =
              index == -1 ? timeAndValueColumn[0] : tsBlock.getColumn(index);
        }
        accumulator.addInput(timeAndValueColumn, bitMap);
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          AGGREGATION_FROM_RAW_DATA, System.nanoTime() - startTime);
    }
  }

  // Used for AggregateOperator
  public void processTsBlocks(TsBlock[] tsBlock) {
    long startTime = System.nanoTime();
    try {
      checkArgument(!step.isInputRaw(), "Step in AggregateOperator cannot process raw input");
      if (step.isInputFinal()) {
        checkArgument(inputLocationList.size() == 1, "Final output can only be single column");
        Column finalResult =
            tsBlock[inputLocationList.get(0)[0].getTsBlockIndex()].getColumn(
                inputLocationList.get(0)[0].getValueColumnIndex());
        accumulator.setFinal(finalResult);
      } else {
        for (InputLocation[] inputLocations : inputLocationList) {
          Column[] columns = new Column[inputLocations.length];
          for (int i = 0; i < inputLocations.length; i++) {
            columns[i] =
                tsBlock[inputLocations[i].getTsBlockIndex()].getColumn(
                    inputLocations[i].getValueColumnIndex());
          }
          accumulator.addIntermediate(columns);
        }
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          AGGREGATION_FROM_RAW_DATA, System.nanoTime() - startTime);
    }
  }

  public void outputResult(ColumnBuilder[] columnBuilder) {
    if (step.isOutputPartial()) {
      accumulator.outputIntermediate(columnBuilder);
    } else {
      accumulator.outputFinal(columnBuilder[0]);
    }
  }

  /** Used for SeriesAggregateScanOperator. */
  public void processStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    long startTime = System.nanoTime();
    try {
      for (InputLocation[] inputLocations : inputLocationList) {
        int valueIndex = inputLocations[0].getValueColumnIndex();
        // valueIndex == -1 means it is count_time, we need to use timeStatistics
        accumulator.addStatistics(valueIndex == -1 ? timeStatistics : valueStatistics[valueIndex]);
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          AGGREGATION_FROM_STATISTICS, System.nanoTime() - startTime);
    }
  }

  public TSDataType[] getOutputType() {
    if (step.isOutputPartial()) {
      return accumulator.getIntermediateType();
    } else {
      return new TSDataType[] {accumulator.getFinalType()};
    }
  }

  public void reset() {
    accumulator.reset();
  }

  public boolean hasFinalResult() {
    return accumulator.hasFinalResult();
  }
}
