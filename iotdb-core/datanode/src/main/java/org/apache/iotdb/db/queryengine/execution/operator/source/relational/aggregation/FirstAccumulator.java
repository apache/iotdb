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
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;

public class FirstAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FirstAccumulator.class);
  protected final TSDataType seriesDataType;
  protected TsPrimitiveType firstValue;
  protected long minTime = Long.MAX_VALUE;
  protected boolean initResult = false;

  public FirstAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    firstValue = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new FirstAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments) {
    // arguments[0] is value column, arguments[1] is time column
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[0], arguments[1]);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], arguments[1]);
        return;
      case FLOAT:
        addFloatInput(arguments[0], arguments[1]);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], arguments[1]);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments[0], arguments[1]);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[0], arguments[1]);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in First: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of First should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = 8;

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          updateIntFirstValue(intVal, time);
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          updateLongFirstValue(longVal, time);
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          updateFloatFirstValue(floatVal, time);
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          updateDoubleFirstValue(doubleVal, time);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          updateBinaryFirstValue(binaryVal, time);
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          updateBooleanFirstValue(boolVal, time);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in First Aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of First should be BinaryColumn");
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(
          new Binary(serializeTimeValue(seriesDataType, minTime, firstValue)));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      switch (seriesDataType) {
        case INT32:
        case DATE:
          columnBuilder.writeInt(firstValue.getInt());
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(firstValue.getLong());
          break;
        case FLOAT:
          columnBuilder.writeFloat(firstValue.getFloat());
          break;
        case DOUBLE:
          columnBuilder.writeDouble(firstValue.getDouble());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          columnBuilder.writeBinary(firstValue.getBinary());
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(firstValue.getBoolean());
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in First aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    if (statistics == null || statistics[0] == null) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        updateIntFirstValue((int) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      case INT64:
        updateLongFirstValue((long) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      case TIMESTAMP:
        updateLongFirstValue(statistics[0].getStartTime(), statistics[0].getStartTime());
        break;
      case FLOAT:
        updateFloatFirstValue((float) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      case DOUBLE:
        updateDoubleFirstValue(
            (double) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        updateBinaryFirstValue(
            (Binary) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      case BOOLEAN:
        updateBooleanFirstValue(
            (boolean) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in First Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.minTime = Long.MAX_VALUE;
    this.firstValue.reset();
  }

  protected void addIntInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateIntFirstValue(valueColumn.getInt(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateIntFirstValue(int value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setInt(value);
    }
  }

  protected void addLongInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateLongFirstValue(valueColumn.getLong(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateLongFirstValue(long value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setLong(value);
    }
  }

  protected void addFloatInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatFirstValue(valueColumn.getFloat(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateFloatFirstValue(float value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleFirstValue(valueColumn.getDouble(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateDoubleFirstValue(double value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setDouble(value);
    }
  }

  protected void addBinaryInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryFirstValue(valueColumn.getBinary(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateBinaryFirstValue(Binary value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBinary(value);
    }
  }

  protected void addBooleanInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanFirstValue(valueColumn.getBoolean(i), timeColumn.getLong(i));
        return;
      }
    }
  }

  protected void updateBooleanFirstValue(boolean value, long curTime) {
    initResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBoolean(value);
    }
  }
}
