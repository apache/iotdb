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

package org.apache.iotdb.db.queryengine.execution.aggregation.slidingwindow;

import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class SlidingWindowAggregator extends Aggregator {

  // cached partial aggregation result of pre-aggregate windows
  protected Deque<PartialAggregationResult> deque;

  protected TimeRange curTimeRange;

  protected SlidingWindowAggregator(
      Accumulator accumulator, List<InputLocation[]> inputLocationList, AggregationStep step) {
    super(accumulator, step, inputLocationList);
    this.deque = new LinkedList<>();
  }

  public void processTsBlock(TsBlock tsBlock) {
    checkArgument(
        step.isInputPartial(),
        "Step in SlidingWindowAggregationOperator can only process partial result");
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    Column[] valueColumn = new Column[inputLocationList.get(0).length];
    for (int i = 0; i < inputLocationList.get(0).length; i++) {
      InputLocation inputLocation = inputLocationList.get(0)[i];
      checkArgument(
          inputLocation.getTsBlockIndex() == 0,
          "SlidingWindowAggregationOperator can only process one tsBlock input.");
      valueColumn[i] = tsBlock.getColumn(inputLocation.getValueColumnIndex());
    }
    processPartialResult(new PartialAggregationResult(timeColumn, valueColumn));
  }

  public void updateTimeRange(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    evictingExpiredValue();
  }

  /** evicting expired element in queue and reset expired aggregateResult. */
  protected abstract void evictingExpiredValue();

  /** update queue and aggregateResult. */
  public abstract void processPartialResult(PartialAggregationResult partialResult);

  protected static class PartialAggregationResult {

    private final TimeColumn timeColumn;
    private final Column[] partialResultColumns;

    public PartialAggregationResult(TimeColumn timeColumn, Column[] partialResultColumns) {
      this.timeColumn = timeColumn;
      this.partialResultColumns = partialResultColumns;
    }

    public boolean isNull() {
      return partialResultColumns[0].isNull(0);
    }

    public long getTime() {
      return timeColumn.getLong(0);
    }

    public Column[] getPartialResult() {
      return partialResultColumns;
    }

    public List<TSDataType> getDataTypes() {
      return Arrays.stream(partialResultColumns)
          .sequential()
          .map(Column::getDataType)
          .collect(Collectors.toList());
    }

    public Column[] opposite() {
      List<TSDataType> dataTypes = getDataTypes();
      TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypes);
      ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
      Column[] results = new Column[partialResultColumns.length];
      for (int i = 0; i < partialResultColumns.length; i++) {
        switch (dataTypes.get(i)) {
          case INT32:
            columnBuilders[i].writeInt(partialResultColumns[i].getInt(0) * -1);
            break;
          case INT64:
            columnBuilders[i].writeLong(partialResultColumns[i].getLong(0) * -1);
            break;
          case FLOAT:
            columnBuilders[i].writeFloat(partialResultColumns[i].getFloat(0) * -1);
            break;
          case DOUBLE:
            columnBuilders[i].writeDouble(partialResultColumns[i].getDouble(0) * -1);
            break;
          case TEXT:
          case BOOLEAN:
            throw new UnSupportedDataTypeException(
                String.format("Unsupported data type in opposite : %s", dataTypes.get(i)));
          default:
            throw new IllegalArgumentException("Unknown data type: " + dataTypes.get(i));
        }
        results[i] = columnBuilders[i].build();
      }
      return results;
    }
  }
}
