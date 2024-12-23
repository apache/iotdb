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

public class LastAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastAccumulator.class);
  protected final TSDataType seriesDataType;
  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;

  public LastAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    lastValue = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new LastAccumulator(seriesDataType);
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
            String.format("Unsupported data type in Last: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of Last should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = Long.BYTES;

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          updateIntLastValue(intVal, time);
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          updateLongLastValue(longVal, time);
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          updateFloatLastValue(floatVal, time);
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          updateDoubleLastValue(doubleVal, time);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          updateBinaryLastValue(binaryVal, time);
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          updateBooleanLastValue(boolVal, time);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in Last Aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of Last should be BinaryColumn");

    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeTimeValue(seriesDataType, maxTime, lastValue)));
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
          columnBuilder.writeInt(lastValue.getInt());
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(lastValue.getLong());
          break;
        case FLOAT:
          columnBuilder.writeFloat(lastValue.getFloat());
          break;
        case DOUBLE:
          columnBuilder.writeDouble(lastValue.getDouble());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          columnBuilder.writeBinary(lastValue.getBinary());
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(lastValue.getBoolean());
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in Last aggregation: %s", seriesDataType));
      }
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
        updateIntLastValue((int) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      case INT64:
        updateLongLastValue((long) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      case TIMESTAMP:
        updateLongLastValue(statistics[0].getEndTime(), statistics[0].getEndTime());
        break;
      case FLOAT:
        updateFloatLastValue((float) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      case DOUBLE:
        updateDoubleLastValue((double) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        updateBinaryLastValue((Binary) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      case BOOLEAN:
        updateBooleanLastValue((boolean) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Last Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.maxTime = Long.MIN_VALUE;
    this.lastValue.reset();
  }

  protected void addIntInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateIntLastValue(valueColumn.getInt(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateIntLastValue(int value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setInt(value);
    }
  }

  protected void addLongInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateLongLastValue(valueColumn.getLong(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateLongLastValue(long value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setLong(value);
    }
  }

  protected void addFloatInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatLastValue(valueColumn.getFloat(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateFloatLastValue(float value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleLastValue(valueColumn.getDouble(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateDoubleLastValue(double value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setDouble(value);
    }
  }

  protected void addBinaryInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryLastValue(valueColumn.getBinary(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateBinaryLastValue(Binary value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBinary(value);
    }
  }

  protected void addBooleanInput(Column valueColumn, Column timeColumn) {
    for (int i = 0; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanLastValue(valueColumn.getBoolean(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateBooleanLastValue(boolean value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBoolean(value);
    }
  }
}
