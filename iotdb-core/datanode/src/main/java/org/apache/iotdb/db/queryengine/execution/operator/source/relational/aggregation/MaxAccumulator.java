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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class MaxAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MaxAccumulator.class);
  private final TSDataType seriesDataType;
  private final TsPrimitiveType maxResult;
  private boolean initResult;

  public MaxAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.maxResult = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new MaxAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of Max should be one column");

    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[0]);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0]);
        return;
      case FLOAT:
        addFloatInput(arguments[0]);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0]);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments[0]);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[0]);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      switch (seriesDataType) {
        case INT32:
        case DATE:
          updateIntMaxValue(argument.getInt(i));
          break;
        case INT64:
        case TIMESTAMP:
          updateLongMaxValue(argument.getLong(i));
          break;
        case FLOAT:
          updateFloatMaxValue(argument.getFloat(i));
          break;
        case DOUBLE:
          updateDoubleMaxValue(argument.getDouble(i));
          break;
        case STRING:
        case TEXT:
        case BLOB:
          updateBinaryMaxValue(argument.getBinary(i));
          break;
        case BOOLEAN:
          updateBooleanMaxValue(argument.getBoolean(i));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in Max Aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }

    switch (seriesDataType) {
      case INT32:
      case DATE:
        columnBuilder.writeInt(maxResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(maxResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(maxResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(maxResult.getDouble());
        break;
      case STRING:
      case TEXT:
      case BLOB:
        columnBuilder.writeBinary(maxResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(maxResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Max Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        columnBuilder.writeInt(maxResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(maxResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(maxResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(maxResult.getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        columnBuilder.writeBinary(maxResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(maxResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MaxAggregation: %s", seriesDataType));
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
    switch (seriesDataType) {
      case INT32:
      case DATE:
        updateIntMaxValue((int) statistics[0].getMaxValue());
        break;
      case INT64:
        updateLongMaxValue((long) statistics[0].getMaxValue());
        break;
      case TIMESTAMP:
        updateLongMaxValue(statistics[0].getEndTime());
        break;
      case FLOAT:
        updateFloatMaxValue((float) statistics[0].getMaxValue());
        break;
      case DOUBLE:
        updateDoubleMaxValue((double) statistics[0].getMaxValue());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        updateBinaryMaxValue((Binary) statistics[0].getMaxValue());
        break;
      case BOOLEAN:
        updateBooleanMaxValue((boolean) statistics[0].getMaxValue());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Last Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.maxResult.reset();
  }

  private void addIntInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateIntMaxValue(valueColumn.getInt(i));
      }
    }
  }

  protected void updateIntMaxValue(int value) {
    if (!initResult || value > maxResult.getInt()) {
      initResult = true;
      maxResult.setInt(value);
    }
  }

  private void addLongInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateLongMaxValue(valueColumn.getLong(i));
      }
    }
  }

  protected void updateLongMaxValue(long value) {
    if (!initResult || value > maxResult.getLong()) {
      initResult = true;
      maxResult.setLong(value);
    }
  }

  private void addFloatInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatMaxValue(valueColumn.getFloat(i));
      }
    }
  }

  protected void updateFloatMaxValue(float value) {
    if (!initResult || value > maxResult.getFloat()) {
      initResult = true;
      maxResult.setFloat(value);
    }
  }

  private void addDoubleInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleMaxValue(valueColumn.getDouble(i));
      }
    }
  }

  protected void updateDoubleMaxValue(double value) {
    if (!initResult || value > maxResult.getDouble()) {
      initResult = true;
      maxResult.setDouble(value);
    }
  }

  private void addBinaryInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryMaxValue(valueColumn.getBinary(i));
      }
    }
  }

  protected void updateBinaryMaxValue(Binary value) {
    if (!initResult || value.compareTo(maxResult.getBinary()) > 0) {
      initResult = true;
      maxResult.setBinary(value);
    }
  }

  private void addBooleanInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanMaxValue(valueColumn.getBoolean(i));
      }
    }
  }

  protected void updateBooleanMaxValue(boolean value) {
    if (!initResult || value) {
      initResult = true;
      maxResult.setBoolean(value);
    }
  }
}
