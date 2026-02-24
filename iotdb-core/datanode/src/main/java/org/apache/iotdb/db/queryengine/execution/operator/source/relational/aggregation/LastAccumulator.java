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
import org.apache.tsfile.file.metadata.statistics.DateStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValueWithNull;

public class LastAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastAccumulator.class);
  protected final TSDataType seriesDataType;
  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;
  protected boolean initNullTimeValue = false;

  public LastAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.lastValue = TsPrimitiveType.getByType(seriesDataType);
  }

  public boolean hasInitResult() {
    return this.initResult;
  }

  public long getMaxTime() {
    return this.maxTime;
  }

  public TsPrimitiveType getLastValue() {
    return this.lastValue;
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
  public void addInput(Column[] arguments, AggregationMask mask) {
    // arguments[0] is value column, arguments[1] is time column
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[0], arguments[1], mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], arguments[1], mask);
        return;
      case FLOAT:
        addFloatInput(arguments[0], arguments[1], mask);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], arguments[1], mask);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        addBinaryInput(arguments[0], arguments[1], mask);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[0], arguments[1], mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of LAST should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      boolean isOrderTimeNull = BytesUtils.bytesToBool(bytes, Long.BYTES);
      int offset = Long.BYTES + 1;

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          if (!isOrderTimeNull) {
            updateIntLastValue(intVal, time);
          } else {
            updateIntNullTimeValue(intVal);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          if (!isOrderTimeNull) {
            updateLongLastValue(longVal, time);
          } else {
            updateLongNullTimeValue(longVal);
          }
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          if (!isOrderTimeNull) {
            updateFloatLastValue(floatVal, time);
          } else {
            updateFloatNullTimeValue(floatVal);
          }
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          if (!isOrderTimeNull) {
            updateDoubleLastValue(doubleVal, time);
          } else {
            updateDoubleNullTimeValue(doubleVal);
          }
          break;
        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          if (!isOrderTimeNull) {
            updateBinaryLastValue(binaryVal, time);
          } else {
            updateBinaryNullTimeValue(binaryVal);
          }
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          if (!isOrderTimeNull) {
            updateBooleanLastValue(boolVal, time);
          } else {
            updateBooleanNullTimeValue(boolVal);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of LAST should be BinaryColumn");

    if (initResult || initNullTimeValue) {
      // if the initResult is activated, the result must carry a not null time
      boolean isOrderTimeNull = !initResult;
      columnBuilder.writeBinary(
          new Binary(
              serializeTimeValueWithNull(seriesDataType, maxTime, isOrderTimeNull, lastValue)));
      return;
    }
    columnBuilder.appendNull();
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {

    // all the values are the null
    if (!initResult && !initNullTimeValue) {
      columnBuilder.appendNull();
      return;
    }

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
      case OBJECT:
      case STRING:
        columnBuilder.writeBinary(lastValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(lastValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST aggregation: %s", seriesDataType));
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
        updateIntLastValue(
            ((Number) statistics[0].getLastValue()).intValue(), statistics[0].getEndTime());
        break;
      case INT64:
      case TIMESTAMP:
        updateLongLastValue(
            ((Number) statistics[0].getLastValue()).longValue(), statistics[0].getEndTime());
        break;
      case FLOAT:
        updateFloatLastValue(
            ((Number) statistics[0].getLastValue()).floatValue(), statistics[0].getEndTime());
        break;
      case DOUBLE:
        updateDoubleLastValue(
            ((Number) statistics[0].getLastValue()).doubleValue(), statistics[0].getEndTime());
        break;
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
        if (statistics[0] instanceof DateStatistics) {
          updateBinaryLastValue(
              new Binary(
                  TSDataType.getDateStringValue((Integer) statistics[0].getLastValue()),
                  StandardCharsets.UTF_8),
              statistics[0].getEndTime());
        } else {
          if (statistics[0].getLastValue() instanceof Binary) {
            updateBinaryLastValue(
                (Binary) statistics[0].getLastValue(), statistics[0].getEndTime());
          } else {
            updateBinaryLastValue(
                new Binary(String.valueOf(statistics[0].getLastValue()), StandardCharsets.UTF_8),
                statistics[0].getEndTime());
          }
        }
        break;
      case BOOLEAN:
        updateBooleanLastValue((boolean) statistics[0].getLastValue(), statistics[0].getEndTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.initNullTimeValue = false;
    this.maxTime = Long.MIN_VALUE;
    this.lastValue.reset();
  }

  private boolean checkAndUpdateLastTime(long curTime) {
    if (!initResult || curTime > maxTime) {
      initResult = true;
      maxTime = curTime;
      return true;
    }
    return false;
  }

  private boolean checkAndUpdateNullTime() {
    if (!initResult && !initNullTimeValue) {
      initNullTimeValue = true;
      return true;
    }
    return false;
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
  protected void addIntInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Time is not null. Attempt to update the value with the minimum time.
        updateIntLastValue(valueColumn.getInt(position), timeColumn.getLong(position));
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateIntNullTimeValue(valueColumn.getInt(position));
      }
    }
  }

  protected void updateIntLastValue(int value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setInt(value);
    }
  }

  protected void updateIntNullTimeValue(int value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setInt(value);
    }
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
  protected void addLongInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Time is not null. Attempt to update the value with the minimum time.
        updateLongLastValue(valueColumn.getLong(position), timeColumn.getLong(position));
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateLongNullTimeValue(valueColumn.getLong(position));
      }
    }
  }

  protected void updateLongLastValue(long value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setLong(value);
    }
  }

  protected void updateLongNullTimeValue(long value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setLong(value);
    }
  }

  protected void addFloatInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateFloatLastValue(valueColumn.getFloat(position), timeColumn.getLong(position));
      } else {
        updateFloatNullTimeValue(valueColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatLastValue(float value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setFloat(value);
    }
  }

  protected void updateFloatNullTimeValue(float value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateDoubleLastValue(valueColumn.getDouble(position), timeColumn.getLong(position));
      } else {
        updateDoubleNullTimeValue(valueColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleLastValue(double value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setDouble(value);
    }
  }

  protected void updateDoubleNullTimeValue(double value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setDouble(value);
    }
  }

  protected void addBinaryInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBinaryLastValue(valueColumn.getBinary(position), timeColumn.getLong(position));
      } else {
        updateBinaryNullTimeValue(valueColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryLastValue(Binary value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setBinary(value);
    }
  }

  protected void updateBinaryNullTimeValue(Binary value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setBinary(value);
    }
  }

  protected void addBooleanInput(Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBooleanLastValue(valueColumn.getBoolean(position), timeColumn.getLong(position));
      } else {
        updateBooleanNullTimeValue(valueColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanLastValue(boolean value, long curTime) {
    if (checkAndUpdateLastTime(curTime)) {
      lastValue.setBoolean(value);
    }
  }

  protected void updateBooleanNullTimeValue(boolean value) {
    if (checkAndUpdateNullTime()) {
      lastValue.setBoolean(value);
    }
  }
}
