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

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.function.Supplier;

public class ExtremeAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExtremeAccumulator.class);
  private final TSDataType seriesDataType;
  private final Type type;
  private final TsPrimitiveType extremeResult;
  private final TypeServices.ColumnValueUpdater valueUpdater;
  private final TypeServices.StatisticsValueUpdater statisticsValueUpdater;
  private boolean initResult;

  public ExtremeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.type = Type.fromTsDataType(seriesDataType);
    this.extremeResult = type.getTsPrimitiveType();
    this.valueUpdater = TypeServices.EXTREME_COLUMN_VALUE_UPDATER_SERVICE.call(type);
    this.statisticsValueUpdater = TypeServices.EXTREME_STATISTICS_VALUE_UPDATER_SERVICE.call(type);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new ExtremeAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    Column column = arguments[0];
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          initResult |= valueUpdater.update(extremeResult, column, i, initResult);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult |= valueUpdater.update(extremeResult, column, position, initResult);
        }
      }
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      initResult |= valueUpdater.update(extremeResult, argument, i, initResult);
    }
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    if (statistics == null || statistics[0] == null) {
      return;
    }

    Supplier<RuntimeException> exceptionSupplier =
        () ->
            new UnSupportedDataTypeException(
                String.format(
                    CalcMessages.UNSUPPORTED_DATA_TYPE_IN_EXTREME_AGGREGATION, seriesDataType));
    if (statisticsValueUpdater.update(
        extremeResult, statistics[0].getMaxValue(), initResult, exceptionSupplier)) {
      initResult = true;
    }
    if (statisticsValueUpdater.update(
        extremeResult, statistics[0].getMinValue(), initResult, exceptionSupplier)) {
      initResult = true;
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    type.write(columnBuilder, extremeResult);
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    type.write(columnBuilder, extremeResult);
  }

  @Override
  public void reset() {
    initResult = false;
    extremeResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  private void addIntInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < column.getPositionCount(); i++) {
        if (!column.isNull(i)) {
          updateIntResult(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          updateIntResult(column.getInt(position));
        }
      }
    }
  }

  private void updateIntResult(int val) {
    int candidateResult = extremeResult.getInt();

    if (!initResult || compareExtreme(val, candidateResult) > 0) {
      initResult = true;
      extremeResult.setInt(val);
    }
  }

  private void addLongInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < column.getPositionCount(); i++) {
        if (!column.isNull(i)) {
          updateLongResult(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          updateLongResult(column.getLong(position));
        }
      }
    }
  }

  private void updateLongResult(long val) {
    long candidateResult = extremeResult.getLong();

    if (!initResult || compareExtreme(val, candidateResult) > 0) {
      initResult = true;
      extremeResult.setLong(val);
    }
  }

  private void addFloatInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < column.getPositionCount(); i++) {
        if (!column.isNull(i)) {
          updateFloatResult(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          updateFloatResult(column.getFloat(position));
        }
      }
    }
  }

  private void updateFloatResult(float val) {
    float absExtVal = Math.abs(val);
    float candidateResult = extremeResult.getFloat();
    float absCandidateResult = Math.abs(extremeResult.getFloat());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setFloat(val);
    }
  }

  private void addDoubleInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < column.getPositionCount(); i++) {
        if (!column.isNull(i)) {
          updateDoubleResult(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          updateDoubleResult(column.getDouble(position));
        }
      }
    }
  }

  private void updateDoubleResult(double val) {
    double absExtVal = Math.abs(val);
    double candidateResult = extremeResult.getDouble();
    double absCandidateResult = Math.abs(extremeResult.getDouble());

    if (!initResult
        || (absExtVal > absCandidateResult)
        || (absExtVal == absCandidateResult) && val > candidateResult) {
      initResult = true;
      extremeResult.setDouble(val);
    }
  }

  private int compareExtreme(int left, int right) {
    int absComparison = Long.compare(Math.abs((long) left), Math.abs((long) right));
    return absComparison == 0 ? Integer.compare(left, right) : absComparison;
  }

  private int compareExtreme(long left, long right) {
    int absComparison = compareAbs(left, right);
    return absComparison == 0 ? Long.compare(left, right) : absComparison;
  }

  private int compareAbs(long left, long right) {
    if (left == Long.MIN_VALUE) {
      return right == Long.MIN_VALUE ? 0 : 1;
    }
    if (right == Long.MIN_VALUE) {
      return -1;
    }
    return Long.compare(Math.abs(left), Math.abs(right));
  }
}
