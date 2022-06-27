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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Aggregator {

  protected final Accumulator accumulator;
  // In some intermediate result input, inputLocation[] should include two columns
  protected List<InputLocation[]> inputLocationList;
  protected final AggregationStep step;

  protected TimeRange curTimeRange = new TimeRange(0, Long.MAX_VALUE);

  // Used for SeriesAggregateScanOperator
  public Aggregator(Accumulator accumulator, AggregationStep step) {
    this.accumulator = accumulator;
    this.step = step;
  }

  // Used for aggregateOperator
  public Aggregator(
      Accumulator accumulator, AggregationStep step, List<InputLocation[]> inputLocationList) {
    this.accumulator = accumulator;
    this.step = step;
    this.inputLocationList = inputLocationList;
  }

  // Used for SeriesAggregateScanOperator and RawDataAggregateOperator
  public int processTsBlock(TsBlock tsBlock) {
    checkArgument(
        step.isInputRaw(),
        "Step in SeriesAggregateScanOperator and RawDataAggregateOperator can only process raw input");
    if (inputLocationList == null) {
      return accumulator.addInput(tsBlock.getTimeAndValueColumn(0), curTimeRange);
    } else {
      int lastReadReadIndex = 0;
      for (InputLocation[] inputLocations : inputLocationList) {
        checkArgument(
            inputLocations[0].getTsBlockIndex() == 0,
            "RawDataAggregateOperator can only process one tsBlock input.");
        Column[] timeValueColumn = new Column[2];
        timeValueColumn[0] = tsBlock.getTimeColumn();
        timeValueColumn[1] = tsBlock.getColumn(inputLocations[0].getValueColumnIndex());
        lastReadReadIndex =
            Math.max(lastReadReadIndex, accumulator.addInput(timeValueColumn, curTimeRange));
      }
      return lastReadReadIndex;
    }
  }

  // Used for AggregateOperator
  public void processTsBlocks(TsBlock[] tsBlock) {
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
  }

  public void outputResult(ColumnBuilder[] columnBuilder) {
    if (step.isOutputPartial()) {
      accumulator.outputIntermediate(columnBuilder);
    } else {
      accumulator.outputFinal(columnBuilder[0]);
    }
  }

  public void processStatistics(Statistics statistics) {
    accumulator.addStatistics(statistics);
  }

  /** Used for AlignedSeriesAggregateScanOperator. */
  public void processStatistics(Statistics[] statistics) {
    for (InputLocation[] inputLocations : inputLocationList) {
      checkArgument(
          inputLocations[0].getTsBlockIndex() == 0,
          "AlignedSeriesAggregateScanOperator can only process one tsBlock input.");
      int valueIndex = inputLocations[0].getValueColumnIndex();
      accumulator.addStatistics(statistics[valueIndex]);
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
    curTimeRange = new TimeRange(0, Long.MAX_VALUE);
    accumulator.reset();
  }

  public boolean hasFinalResult() {
    return accumulator.hasFinalResult();
  }

  public void updateTimeRange(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
  }

  public TimeRange getCurTimeRange() {
    return curTimeRange;
  }
}
