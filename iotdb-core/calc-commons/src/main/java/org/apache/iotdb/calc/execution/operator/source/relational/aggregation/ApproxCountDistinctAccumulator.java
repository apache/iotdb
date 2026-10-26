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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLog;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLogStateFactory;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.HyperLogLog.DEFAULT_STANDARD_ERROR;

public class ApproxCountDistinctAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ApproxCountDistinctAccumulator.class);
  private final TSDataType seriesDataType;
  private final TypeServices.HyperLogLogColumnAdder valueAdder;
  private final HyperLogLogStateFactory.SingleHyperLogLogState state =
      HyperLogLogStateFactory.createSingleState();

  private static final int DEFAULT_HYPERLOGLOG_BUCKET_SIZE = 2048;

  public ApproxCountDistinctAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.valueAdder =
        TypeServices.HYPER_LOG_LOG_COLUMN_ADDER_SERVICE.call(Type.fromTsDataType(seriesDataType));
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + RamUsageEstimator.shallowSizeOfInstance(HyperLogLog.class)
        + Integer.BYTES * DEFAULT_HYPERLOGLOG_BUCKET_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new ApproxCountDistinctAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    double maxStandardError =
        arguments.length == 1 ? DEFAULT_STANDARD_ERROR : arguments[1].getDouble(0);
    HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);

    Column valueColumn = arguments[0];
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          valueAdder.add(hll, valueColumn, i);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          valueAdder.add(hll, valueColumn, position);
        }
      }
    }
  }

  @Override
  public void addIntermediate(Column argument) {

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        HyperLogLog current = new HyperLogLog(argument.getBinary(i).getValues());
        state.merge(current);
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_APPROX_COUNT_DISTINCT_SHOULD_BE_BINARYCOLUMN_F444D3BE);
    columnBuilder.writeBinary(new Binary(state.getHyperLogLog().serialize()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(state.getHyperLogLog().cardinality());
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(
        CalcMessages.APPROX_COUNT_DISTINCT_ACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS);
  }

  @Override
  public void reset() {
    state.getHyperLogLog().reset();
  }

  public void addBooleanInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getBoolean(position));
        }
      }
    }
  }

  public void addIntInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getInt(position));
        }
      }
    }
  }

  public void addLongInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getLong(position));
        }
      }
    }
  }

  public void addFloatInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getFloat(position));
        }
      }
    }
  }

  public void addDoubleInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getDouble(position));
        }
      }
    }
  }

  public void addBinaryInput(Column valueColumn, AggregationMask mask, HyperLogLog hll) {
    int positionCount = mask.getPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          hll.add(valueColumn.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          hll.add(valueColumn.getBinary(position));
        }
      }
    }
  }

  public static HyperLogLog getOrCreateHyperLogLog(
      HyperLogLogStateFactory.SingleHyperLogLogState state, double maxStandardError) {
    HyperLogLog hll = state.getHyperLogLog();
    if (hll == null) {
      hll = new HyperLogLog(maxStandardError);
      state.setHyperLogLog(hll);
    }
    return hll;
  }
}
