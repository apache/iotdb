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

import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class TreeAggregator {

  protected final Accumulator accumulator;
  // In some intermediate result input, inputLocation[] should include two columns
  protected List<InputLocation[]> inputLocationList;
  protected final AggregationStep step;

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
    checkArgument(
        step.isInputRaw(),
        DataNodeQueryMessages
            .EXCEPTION_STEP_IN_SERIESAGGREGATESCANOPERATOR_AND_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCES_5575BD95);
    for (InputLocation[] inputLocations : inputLocationList) {
      Column[] timeAndValueColumn = new Column[1 + inputLocations.length];
      timeAndValueColumn[0] = tsBlock.getTimeColumn();
      for (int i = 0; i < inputLocations.length; i++) {
        checkArgument(
            inputLocations[i].getTsBlockIndex() == 0,
            DataNodeQueryMessages
                .EXCEPTION_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_5ABCB8C0);
        int index = inputLocations[i].getValueColumnIndex();
        // for count_time, time column is also its value column
        // for max_by, the input column can also be time column.
        timeAndValueColumn[1 + i] = index == -1 ? timeAndValueColumn[0] : tsBlock.getColumn(index);
      }
      accumulator.addInput(timeAndValueColumn, bitMap);
    }
  }

  // Used for AggregateOperator
  public void processTsBlocks(TsBlock[] tsBlock) {
    checkArgument(
        !step.isInputRaw(),
        DataNodeQueryMessages
            .EXCEPTION_STEP_IN_AGGREGATEOPERATOR_CANNOT_PROCESS_RAW_INPUT_22620F61);
    if (step.isInputFinal()) {
      checkArgument(
          inputLocationList.size() == 1,
          DataNodeQueryMessages.EXCEPTION_FINAL_OUTPUT_CAN_ONLY_BE_SINGLE_COLUMN_6D82F9E0);
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

  /** Used for SeriesAggregateScanOperator. */
  public void processStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    for (InputLocation[] inputLocations : inputLocationList) {
      int valueIndex = inputLocations[0].getValueColumnIndex();
      // valueIndex == -1 means it is count_time, we need to use timeStatistics
      accumulator.addStatistics(valueIndex == -1 ? timeStatistics : valueStatistics[valueIndex]);
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
