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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;

public class MinAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MinAccumulator.class);
  private final TSDataType seriesDataType;
  private final Type type;
  private final TypeServices.ColumnValueUpdater valueUpdater;
  private final TypeServices.StatisticsValueUpdater statisticsValueUpdater;
  private final TsPrimitiveType minResult;
  private boolean initResult;

  public MinAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.type = Type.fromTsDataType(seriesDataType);
    this.minResult = type.getTsPrimitiveType();
    this.valueUpdater = TypeServices.MIN_COLUMN_VALUE_UPDATER_SERVICE.call(type);
    this.statisticsValueUpdater = TypeServices.MIN_STATISTICS_VALUE_UPDATER_SERVICE.call(type);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new MinAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(
        arguments.length == 1,
        CalcMessages.EXCEPTION_ARGUMENT_OF_MIN_SHOULD_BE_ONE_COLUMN_F8AA1E88);

    addInput(arguments[0], mask);
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      if (valueUpdater.update(minResult, argument, i, initResult)) {
        initResult = true;
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    type.write(columnBuilder, minResult);
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    type.write(columnBuilder, minResult);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
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
                    CalcMessages.UNSUPPORTED_DATA_TYPE_IN_MIN_AGGREGATION, seriesDataType));
    if (statisticsValueUpdater.update(
        minResult, statistics[0].getMinValue(), initResult, exceptionSupplier)) {
      initResult = true;
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.minResult.reset();
  }

  private void addInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i) && valueUpdater.update(minResult, valueColumn, i, initResult)) {
          initResult = true;
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        if (!valueColumn.isNull(position)
            && valueUpdater.update(minResult, valueColumn, position, initResult)) {
          initResult = true;
        }
      }
    }
  }

  private void addIntInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateIntMinValue(valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateIntMinValue(valueColumn.getInt(position));
        }
      }
    }
  }

  protected void updateIntMinValue(int value) {
    if (!initResult || value < minResult.getInt()) {
      initResult = true;
      minResult.setInt(value);
    }
  }

  private void addLongInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateLongMinValue(valueColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateLongMinValue(valueColumn.getLong(position));
        }
      }
    }
  }

  protected void updateLongMinValue(long value) {
    if (!initResult || value < minResult.getLong()) {
      initResult = true;
      minResult.setLong(value);
    }
  }

  private void addFloatInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateFloatMinValue(valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateFloatMinValue(valueColumn.getFloat(position));
        }
      }
    }
  }

  protected void updateFloatMinValue(float value) {
    if (!initResult || value < minResult.getFloat()) {
      initResult = true;
      minResult.setFloat(value);
    }
  }

  private void addDoubleInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateDoubleMinValue(valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateDoubleMinValue(valueColumn.getDouble(position));
        }
      }
    }
  }

  protected void updateDoubleMinValue(double value) {
    if (!initResult || value < minResult.getDouble()) {
      initResult = true;
      minResult.setDouble(value);
    }
  }

  private void addBinaryInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateBinaryMinValue(valueColumn.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBinaryMinValue(valueColumn.getBinary(position));
        }
      }
    }
  }

  protected void updateBinaryMinValue(Binary value) {
    if (!initResult || value.compareTo(minResult.getBinary()) < 0) {
      initResult = true;
      minResult.setBinary(value);
    }
  }

  private void addBooleanInput(Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          updateBooleanMinValue(valueColumn.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBooleanMinValue(valueColumn.getBoolean(position));
        }
      }
    }
  }

  protected void updateBooleanMinValue(boolean value) {
    if (!initResult || !value) {
      initResult = true;
      minResult.setBoolean(value);
    }
  }
}
