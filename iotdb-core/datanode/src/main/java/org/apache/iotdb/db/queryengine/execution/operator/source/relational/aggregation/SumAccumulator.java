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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class SumAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SumAccumulator.class);
  private final TSDataType argumentDataType;
  private double sumValue = 0;
  private boolean initResult = false;

  public SumAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new SumAccumulator(this.argumentDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 1, "argument of SUM should be one column");
    switch (argumentDataType) {
      case INT32:
        addIntInput(arguments[0], mask);
        return;
      case INT64:
        addLongInput(arguments[0], mask);
        return;
      case FLOAT:
        addFloatInput(arguments[0], mask);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], mask);
        return;
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in SUM Aggregation: %s", argumentDataType));
    }
  }

  @Override
  public void removeInput(Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of SUM should be one column");
    switch (argumentDataType) {
      case INT32:
        removeIntInput(arguments[0]);
        return;
      case INT64:
        removeLongInput(arguments[0]);
        return;
      case FLOAT:
        removeFloatInput(arguments[0]);
        return;
      case DOUBLE:
        removeDoubleInput(arguments[0]);
        return;
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in SUM Aggregation: %s", argumentDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      initResult = true;
      sumValue += argument.getDouble(i);
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValue);
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValue);
    }
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

    initResult = true;
    if (statistics[0] instanceof IntegerStatistics) {
      sumValue += statistics[0].getSumLongValue();
    } else {
      sumValue += statistics[0].getSumDoubleValue();
    }
  }

  @Override
  public void reset() {
    this.initResult = false;
    this.sumValue = 0.0;
  }

  @Override
  public boolean removable() {
    return true;
  }

  private void addIntInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      int count = column.getPositionCount();
      for (int i = 0; i < count; i++) {
        if (!column.isNull(i)) {
          initResult = true;
          sumValue += column.getInt(i);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult = true;
          sumValue += column.getInt(position);
        }
      }
    }
  }

  private void addLongInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      int count = column.getPositionCount();
      for (int i = 0; i < count; i++) {
        if (!column.isNull(i)) {
          initResult = true;
          sumValue += column.getLong(i);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult = true;
          sumValue += column.getLong(position);
        }
      }
    }
  }

  private void addFloatInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      int count = column.getPositionCount();
      for (int i = 0; i < count; i++) {
        if (!column.isNull(i)) {
          initResult = true;
          sumValue += column.getFloat(i);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult = true;
          sumValue += column.getFloat(position);
        }
      }
    }
  }

  private void addDoubleInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      int count = column.getPositionCount();
      for (int i = 0; i < count; i++) {
        if (!column.isNull(i)) {
          initResult = true;
          sumValue += column.getDouble(i);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult = true;
          sumValue += column.getDouble(position);
        }
      }
    }
  }

  private void removeIntInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        sumValue -= column.getInt(i);
      }
    }
  }

  private void removeLongInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        sumValue -= column.getLong(i);
      }
    }
  }

  private void removeFloatInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        sumValue -= column.getFloat(i);
      }
    }
  }

  private void removeDoubleInput(Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        sumValue -= column.getDouble(i);
      }
    }
  }
}
