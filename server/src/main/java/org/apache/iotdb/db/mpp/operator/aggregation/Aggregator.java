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

package org.apache.iotdb.db.mpp.operator.aggregation;

import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import java.util.List;

public class Aggregator {

  private final Accumulator accumulator;
  private final List<InputLocation> inputLocationList;
  private final AggregationStep step;
  private final TSDataType intermediateType;
  private final TSDataType finalType;

  private TimeRange timeRange;

  public Aggregator(
      Accumulator accumulator,
      AggregationStep step,
      List<InputLocation> inputLocationList,
      TSDataType intermediateType,
      TSDataType finalType) {
    this.accumulator = accumulator;
    this.step = step;
    this.inputLocationList = inputLocationList;
    this.intermediateType = intermediateType;
    this.finalType = finalType;
  }

  public void processTsBlock(TsBlock tsBlock) {
    if (step.isInputRaw()) {
      accumulator.addInput(tsBlock.getTimeAndValueColumn(0), timeRange);
    } else {
      accumulator.addIntermediate(tsBlock.getColumns(new int[] {0}));
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

  public TSDataType getOutputType() {
    if (step.isOutputPartial()) {
      return intermediateType;
    } else {
      return finalType;
    }
  }

  public void reset() {
    accumulator.reset();
  }

  public boolean hasFinalResult() {
    return accumulator.hasFinalResult();
  }

  public void setTimeRange(TimeRange timeRange) {
    this.timeRange = timeRange;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }
}
