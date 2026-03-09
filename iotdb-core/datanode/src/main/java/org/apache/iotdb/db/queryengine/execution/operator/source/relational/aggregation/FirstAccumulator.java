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

public class FirstAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FirstAccumulator.class);
  protected final TSDataType seriesDataType;
  protected TsPrimitiveType firstValue;
  protected long minTime = Long.MAX_VALUE;
  protected boolean initResult = false;
  private final boolean canFinishAfterInit;
  protected boolean initNullTimeValue = false;

  public FirstAccumulator(TSDataType seriesDataType, boolean canFinishAfterInit) {
    this.seriesDataType = seriesDataType;
    this.firstValue = TsPrimitiveType.getByType(seriesDataType);
    this.canFinishAfterInit = canFinishAfterInit;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new FirstAccumulator(seriesDataType, canFinishAfterInit);
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
            String.format("Unsupported data type in FIRST: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of FIRST should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      boolean isTimeNull = BytesUtils.bytesToBool(bytes, 8);
      int offset = 9;

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          if (!isTimeNull) {
            updateIntFirstValue(intVal, time);
          } else {
            updateIntNullTimeValue(intVal);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          if (!isTimeNull) {
            updateLongFirstValue(longVal, time);
          } else {
            updateLongNullTimeValue(longVal);
          }
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          if (!isTimeNull) {
            updateFloatFirstValue(floatVal, time);
          } else {
            updateFloatNullTimeValue(floatVal);
          }
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          if (!isTimeNull) {
            updateDoubleFirstValue(doubleVal, time);
          } else {
            updateDoubleNullTimeValue(doubleVal);
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          if (!isTimeNull) {
            updateBinaryFirstValue(binaryVal, time);
          } else {
            updateBinaryNullTimeValue(binaryVal);
          }
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          if (!isTimeNull) {
            updateBooleanFirstValue(boolVal, time);
          } else {
            updateBooleanNullTimeValue(boolVal);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in FIRST Aggregation: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of FIRST should be BinaryColumn");

    // Case 1: Found a valid result with a valid timestamp (Highest Priority)
    if (initResult || initNullTimeValue) {
      // if the initResult is activated, the result must carry a not null time
      boolean isOrderTimeNull = !initResult;
      columnBuilder.writeBinary(
          new Binary(
              serializeTimeValueWithNull(seriesDataType, minTime, isOrderTimeNull, firstValue)));
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
      case OBJECT:
        columnBuilder.writeBinary(firstValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(firstValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST aggregation: %s", seriesDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return canFinishAfterInit && initResult;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    if (statistics == null || statistics[0] == null) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        updateIntFirstValue(
            ((Number) statistics[0].getFirstValue()).intValue(), statistics[0].getStartTime());
        break;
      case INT64:
      case TIMESTAMP:
        updateLongFirstValue(
            ((Number) statistics[0].getFirstValue()).longValue(), statistics[0].getStartTime());
        break;
      case FLOAT:
        updateFloatFirstValue(
            ((Number) statistics[0].getFirstValue()).floatValue(), statistics[0].getStartTime());
        break;
      case DOUBLE:
        updateDoubleFirstValue(
            ((Number) statistics[0].getFirstValue()).doubleValue(), statistics[0].getStartTime());
        break;
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        if (statistics[0] instanceof DateStatistics) {
          updateBinaryFirstValue(
              new Binary(
                  TSDataType.getDateStringValue((Integer) statistics[0].getFirstValue()),
                  StandardCharsets.UTF_8),
              statistics[0].getStartTime());
        } else {
          if (statistics[0].getFirstValue() instanceof Binary) {
            updateBinaryFirstValue(
                (Binary) statistics[0].getFirstValue(), statistics[0].getStartTime());
          } else {
            updateBinaryFirstValue(
                new Binary(String.valueOf(statistics[0].getFirstValue()), StandardCharsets.UTF_8),
                statistics[0].getStartTime());
          }
        }
        break;
      case BOOLEAN:
        updateBooleanFirstValue(
            (boolean) statistics[0].getFirstValue(), statistics[0].getStartTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.initNullTimeValue = false;
    this.minTime = Long.MAX_VALUE;
    this.firstValue.reset();
  }

  private boolean checkAndUpdateFirstTime(long curTime) {
    if (!initResult || curTime < minTime) {
      initResult = true;
      minTime = curTime;
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
        updateIntFirstValue(valueColumn.getInt(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateIntNullTimeValue(valueColumn.getInt(position));
      }
    }
  }

  protected void updateIntFirstValue(int value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setInt(value);
    }
  }

  protected void updateIntNullTimeValue(int value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setInt(value);
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
        updateLongFirstValue(valueColumn.getLong(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: Time is NULL, the nullTimeValue should only be assigned once
        updateLongNullTimeValue(valueColumn.getLong(position));
      }
    }
  }

  protected void updateLongFirstValue(long value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setLong(value);
    }
  }

  protected void updateLongNullTimeValue(long value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setLong(value);
    }
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
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
        updateFloatFirstValue(valueColumn.getFloat(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateFloatNullTimeValue(valueColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatFirstValue(float value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setFloat(value);
    }
  }

  protected void updateFloatNullTimeValue(float value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setFloat(value);
    }
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
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
        updateDoubleFirstValue(valueColumn.getDouble(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateDoubleNullTimeValue(valueColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleFirstValue(double value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setDouble(value);
    }
  }

  protected void updateDoubleNullTimeValue(double value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setDouble(value);
    }
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
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
        updateBinaryFirstValue(valueColumn.getBinary(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBinaryNullTimeValue(valueColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryFirstValue(Binary value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setBinary(value);
    }
  }

  protected void updateBinaryNullTimeValue(Binary value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setBinary(value);
    }
  }

  /**
   * Updates the accumulator, prioritizing values with valid timestamps over those with null
   * timestamps.
   */
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
        updateBooleanFirstValue(valueColumn.getBoolean(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBooleanNullTimeValue(valueColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanFirstValue(boolean value, long curTime) {
    if (checkAndUpdateFirstTime(curTime)) {
      firstValue.setBoolean(value);
    }
  }

  protected void updateBooleanNullTimeValue(boolean value) {
    if (checkAndUpdateNullTime()) {
      firstValue.setBoolean(value);
    }
  }
}
