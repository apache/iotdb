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

public class MinAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MinAccumulator.class);
  private final TSDataType seriesDataType;
  private final TsPrimitiveType minResult;
  private boolean initResult;

  public MinAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.minResult = TsPrimitiveType.getByType(seriesDataType);
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
  public void addInput(Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of Min should be one column");

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
          updateIntMinValue(argument.getInt(i));
          break;
        case INT64:
        case TIMESTAMP:
          updateLongMinValue(argument.getLong(i));
          break;
        case FLOAT:
          updateFloatMinValue(argument.getFloat(i));
          break;
        case DOUBLE:
          updateDoubleMinValue(argument.getDouble(i));
          break;
        case STRING:
        case TEXT:
        case BLOB:
          updateBinaryMinValue(argument.getBinary(i));
          break;
        case BOOLEAN:
          updateBooleanMinValue(argument.getBoolean(i));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in Min Aggregation: %s", seriesDataType));
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
        columnBuilder.writeInt(minResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(minResult.getDouble());
        break;
      case STRING:
      case TEXT:
      case BLOB:
        columnBuilder.writeBinary(minResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(minResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Min Aggregation: %s", seriesDataType));
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
        columnBuilder.writeInt(minResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(minResult.getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        columnBuilder.writeBinary(minResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(minResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Min Aggregation: %s", seriesDataType));
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
        updateIntMinValue((int) statistics[0].getMinValue());
        break;
      case INT64:
        updateLongMinValue((long) statistics[0].getMinValue());
        break;
      case TIMESTAMP:
        updateLongMinValue(statistics[0].getStartTime());
        break;
      case FLOAT:
        updateFloatMinValue((float) statistics[0].getMinValue());
        break;
      case DOUBLE:
        updateDoubleMinValue((double) statistics[0].getMinValue());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        updateBinaryMinValue((Binary) statistics[0].getMinValue());
        break;
      case BOOLEAN:
        updateBooleanMinValue((boolean) statistics[0].getMinValue());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Last Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.minResult.reset();
  }

  private void addIntInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateIntMinValue(valueColumn.getInt(i));
      }
    }
  }

  protected void updateIntMinValue(int value) {
    if (!initResult || value < minResult.getInt()) {
      initResult = true;
      minResult.setInt(value);
    }
  }

  private void addLongInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateLongMinValue(valueColumn.getLong(i));
      }
    }
  }

  protected void updateLongMinValue(long value) {
    if (!initResult || value < minResult.getLong()) {
      initResult = true;
      minResult.setLong(value);
    }
  }

  private void addFloatInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatMinValue(valueColumn.getFloat(i));
      }
    }
  }

  protected void updateFloatMinValue(float value) {
    if (!initResult || value < minResult.getFloat()) {
      initResult = true;
      minResult.setFloat(value);
    }
  }

  private void addDoubleInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleMinValue(valueColumn.getDouble(i));
      }
    }
  }

  protected void updateDoubleMinValue(double value) {
    if (!initResult || value < minResult.getDouble()) {
      initResult = true;
      minResult.setDouble(value);
    }
  }

  private void addBinaryInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryMinValue(valueColumn.getBinary(i));
      }
    }
  }

  protected void updateBinaryMinValue(Binary value) {
    if (!initResult || value.compareTo(minResult.getBinary()) < 0) {
      initResult = true;
      minResult.setBinary(value);
    }
  }

  private void addBooleanInput(Column valueColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanMinValue(valueColumn.getBoolean(i));
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
