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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.primitives.Ints;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableAggregator {
  private final TableAccumulator accumulator;
  private final AggregationNode.Step step;
  private final TSDataType outputType;
  private final int[] inputChannels;
  private final OptionalInt maskChannel;

  public TableAggregator(
      TableAccumulator accumulator,
      AggregationNode.Step step,
      TSDataType outputType,
      List<Integer> inputChannels,
      OptionalInt maskChannel) {
    this.accumulator = requireNonNull(accumulator, "accumulator is null");
    this.step = requireNonNull(step, "step is null");
    this.outputType = requireNonNull(outputType, "intermediateType is null");
    this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
    this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
    checkArgument(
        step.isInputRaw() || inputChannels.size() == 1,
        "expected 1 input channel for intermediate aggregation");
  }

  public TSDataType getType() {
    return outputType;
  }

  public void processBlock(TsBlock block) {
    Column[] arguments = block.getColumns(inputChannels);

    // process count(*)
    if (arguments.length == 0) {
      arguments =
          new Column[] {new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, block.getPositionCount())};
    }

    if (step.isInputRaw()) {
      accumulator.addInput(arguments);
    } else {
      accumulator.addIntermediate(arguments[0]);
    }
  }

  public void evaluate(ColumnBuilder columnBuilder) {
    if (step.isOutputPartial()) {
      accumulator.evaluateIntermediate(columnBuilder);
    } else {
      accumulator.evaluateFinal(columnBuilder);
    }
  }

  public void processStatistics(Statistics[] statistics) {
    accumulator.addStatistics(statistics);
  }

  public boolean hasFinalResult() {
    return accumulator.hasFinalResult();
  }

  public void reset() {
    accumulator.reset();
  }

  public long getEstimatedSize() {
    return accumulator.getEstimatedSize();
  }

  public int getChannelCount() {
    return this.inputChannels.length;
  }
}
