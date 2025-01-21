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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;

import com.google.common.primitives.Ints;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class WindowAggregator {
  private final TableAccumulator accumulator;
  private final TSDataType outputType;
  private final int[] inputChannels;

  public WindowAggregator(
      TableAccumulator accumulator, TSDataType outputType, List<Integer> inputChannels) {
    this.accumulator = requireNonNull(accumulator, "accumulator is null");
    this.outputType = requireNonNull(outputType, "intermediateType is null");
    this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
  }

  public TSDataType getType() {
    return outputType;
  }

  public void addInput(Partition partition) {
    List<Column[]> allColumns = partition.getAllColumns();
    for (Column[] columns : allColumns) {
      addInput(columns);
    }
  }

  public void addInput(Column[] columns) {
    Column[] arguments = new Column[inputChannels.length];
    for (int i = 0; i < inputChannels.length; i++) {
      arguments[i] = columns[inputChannels[i]];
    }

    // Process count(*)
    int count = columns[0].getPositionCount();
    if (arguments.length == 0) {
      arguments = new Column[] {new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, count)};
    }

    AggregationMask mask = AggregationMask.createSelectAll(count);
    accumulator.addInput(arguments, mask);
  }

  public void removeInput(Partition partition) {
    List<Column[]> allColumns = partition.getAllColumns();
    for (Column[] columns : allColumns) {
      removeInput(columns);
    }
  }

  private void removeInput(Column[] columns) {
    Column[] arguments = new Column[inputChannels.length];
    for (int i = 0; i < inputChannels.length; i++) {
      arguments[i] = columns[inputChannels[i]];
    }

    // Process count(*)
    int count = columns[0].getPositionCount();
    if (arguments.length == 0) {
      arguments = new Column[] {new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, count)};
    }

    accumulator.removeInput(arguments);
  }

  public void evaluate(ColumnBuilder columnBuilder) {
    accumulator.evaluateFinal(columnBuilder);
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

  public boolean removable() {
    return accumulator.removable();
  }

  public long getEstimatedSize() {
    return accumulator.getEstimatedSize();
  }

  public int getChannelCount() {
    return this.inputChannels.length;
  }
}
