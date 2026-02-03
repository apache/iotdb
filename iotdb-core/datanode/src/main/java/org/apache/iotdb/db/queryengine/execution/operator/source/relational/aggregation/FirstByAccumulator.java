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

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValueWithNull;

public class FirstByAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FirstByAccumulator.class);

  protected final TSDataType xDataType;
  protected final TSDataType yDataType;

  protected final boolean xIsTimeColumn;
  protected final boolean yIsTimeColumn;

  private long yFirstTime = Long.MAX_VALUE;

  private final TsPrimitiveType xResult;
  private boolean xIsNull = true;

  private boolean initResult = false;
  protected boolean initNullTimeValue = false;

  private final boolean canFinishAfterInit;

  public FirstByAccumulator(
      TSDataType xDataType,
      TSDataType yDataType,
      boolean xIsTimeColumn,
      boolean yIsTimeColumn,
      boolean canFinishAfterInit) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xIsTimeColumn = xIsTimeColumn;
    this.yIsTimeColumn = yIsTimeColumn;

    this.xResult = TsPrimitiveType.getByType(xDataType);
    this.canFinishAfterInit = canFinishAfterInit;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new FirstByAccumulator(
        xDataType, yDataType, xIsTimeColumn, yIsTimeColumn, canFinishAfterInit);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 3, "Length of input Column[] for FIRST_BY should be 3");

    // arguments[0] is x column, arguments[1] is y column, arguments[2] is time column
    switch (xDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      case FLOAT:
        addFloatInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        addBinaryInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[0], arguments[1], arguments[2], mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", yDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of FIRST_BY should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long curTime = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = Long.BYTES;
      boolean isOrderTimeNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;
      boolean isXValueNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;

      switch (xDataType) {
        case INT32:
        case DATE:
          int intVal = isXValueNull ? 0 : BytesUtils.bytesToInt(bytes, offset);
          if (!isOrderTimeNull) {
            updateIntFirstValue(isXValueNull, intVal, curTime);
          } else {
            updateIntNullTimeValue(isXValueNull, intVal);
          }
          break;

        case INT64:
        case TIMESTAMP:
          long longVal =
              isXValueNull ? 0 : BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          if (!isOrderTimeNull) {
            updateLongFirstValue(isXValueNull, longVal, curTime);
          } else {
            updateLongNullTimeValue(isXValueNull, longVal);
          }
          break;

        case FLOAT:
          float floatVal = isXValueNull ? 0 : BytesUtils.bytesToFloat(bytes, offset);
          if (!isOrderTimeNull) {
            updateFloatFirstValue(isXValueNull, floatVal, curTime);
          } else {
            updateFloatNullTimeValue(isXValueNull, floatVal);
          }
          break;

        case DOUBLE:
          double doubleVal = isXValueNull ? 0 : BytesUtils.bytesToDouble(bytes, offset);
          if (!isOrderTimeNull) {
            updateDoubleFirstValue(isXValueNull, doubleVal, curTime);
          } else {
            updateDoubleNullTimeValue(isXValueNull, doubleVal);
          }
          break;

        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          Binary binaryVal = null;
          if (!isXValueNull) {
            int length = BytesUtils.bytesToInt(bytes, offset);
            offset += Integer.BYTES;
            binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          }
          if (!isOrderTimeNull) {
            updateBinaryFirstValue(isXValueNull, binaryVal, curTime);
          } else {
            updateBinaryNullTimeValue(isXValueNull, binaryVal);
          }
          break;

        case BOOLEAN:
          boolean boolVal = false;
          if (!isXValueNull) {
            boolVal = BytesUtils.bytesToBool(bytes, offset);
          }
          if (!isOrderTimeNull) {
            updateBooleanFirstValue(isXValueNull, boolVal, curTime);
          } else {
            updateBooleanNullTimeValue(isXValueNull, boolVal);
          }
          break;

        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in FIRST_BY Aggregation: %s", yDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of FIRST_BY should be BinaryColumn");

    if (initResult || initNullTimeValue) {
      // if the initResult is activated, the result must carry a not null time
      boolean isOrderTimeNull = !initResult;
      columnBuilder.writeBinary(
          new Binary(
              serializeTimeValueWithNull(
                  xDataType, yFirstTime, xIsNull, isOrderTimeNull, xResult)));
      return;
    }

    columnBuilder.appendNull();
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (xIsNull) {
      columnBuilder.appendNull();
      return;
    }

    switch (xDataType) {
      case INT32:
      case DATE:
        columnBuilder.writeInt(xResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(xResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(xResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(xResult.getDouble());
        break;
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
        columnBuilder.writeBinary(xResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(xResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return canFinishAfterInit && initResult;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {

    // only first_by(x, time) and first_by(time, x) can use statistics optimization

    Statistics xStatistics = statistics[0];
    Statistics yStatistics = statistics[1];

    if (yIsTimeColumn && yStatistics == null || xIsTimeColumn && xStatistics == null) {
      return;
    }

    if (yIsTimeColumn) {
      if (xStatistics == null || xStatistics.getStartTime() > yStatistics.getStartTime()) {
        if (!initResult || yStatistics.getStartTime() < yFirstTime) {
          initResult = true;
          yFirstTime = yStatistics.getStartTime();
          xIsNull = true;
        }
      } else {
        if (!initResult || yStatistics.getStartTime() < yFirstTime) {
          initResult = true;
          yFirstTime = yStatistics.getStartTime();
          xIsNull = false;

          switch (xDataType) {
            case INT32:
            case DATE:
              xResult.setInt(((Number) xStatistics.getFirstValue()).intValue());
              break;
            case INT64:
            case TIMESTAMP:
              xResult.setLong(((Number) xStatistics.getFirstValue()).longValue());
              break;
            case FLOAT:
              xResult.setFloat(((Number) statistics[0].getFirstValue()).floatValue());
              break;
            case DOUBLE:
              xResult.setDouble(((Number) statistics[0].getFirstValue()).doubleValue());
              break;
            case TEXT:
            case BLOB:
            case OBJECT:
            case STRING:
              if (statistics[0].getFirstValue() instanceof Binary) {
                xResult.setBinary((Binary) statistics[0].getFirstValue());
              } else {
                xResult.setBinary(
                    new Binary(
                        String.valueOf(statistics[0].getFirstValue()), StandardCharsets.UTF_8));
              }
              break;
            case BOOLEAN:
              xResult.setBoolean((boolean) statistics[0].getFirstValue());
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Unsupported data type in FIRST_BY Aggregation: %s", yDataType));
          }
        }
      }
    } else {
      // x is time column
      if (yStatistics != null && (!initResult || yStatistics.getStartTime() < yFirstTime)) {
        initResult = true;
        xIsNull = false;
        yFirstTime = yStatistics.getStartTime();
        xResult.setLong(yStatistics.getStartTime());
      }
    }
  }

  @Override
  public void reset() {
    initResult = false;
    initNullTimeValue = false;
    xIsNull = true;
    this.yFirstTime = Long.MAX_VALUE;
    this.xResult.reset();
  }

  private boolean checkAndUpdateFirstTime(boolean isXValueNull, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;

      if (isXValueNull) {
        xIsNull = true;
        return false;
      } else {
        xIsNull = false;
        return true;
      }
    }
    return false;
  }

  private boolean checkAndUpdateNullTime(boolean isXValueNull) {
    if (!initResult && !initNullTimeValue) {
      initNullTimeValue = true;

      if (isXValueNull) {
        xIsNull = true;
        return false;
      } else {
        xIsNull = false;
        return true;
      }
    }
    return false;
  }

  protected void addIntInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: The order time is not null. Attempt to update the xResult.
        updateIntFirstValue(
            xColumn.isNull(position), xColumn.getInt(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: The order time is null. Attempt to update the xNullTimeValue.
        updateIntNullTimeValue(xColumn.isNull(position), xColumn.getInt(position));
      }
    }
  }

  protected void updateIntFirstValue(boolean isXValueNull, int xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setInt(xValue);
    }
  }

  protected void updateIntNullTimeValue(boolean isXValueNull, int xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setInt(xValue);
    }
  }

  protected void addLongInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Valid Time
        updateLongFirstValue(
            xColumn.isNull(position), xColumn.getLong(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: Null Time
        updateLongNullTimeValue(xColumn.isNull(position), xColumn.getLong(position));
      }
    }
  }

  protected void updateLongFirstValue(boolean isXValueNull, long xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setLong(xValue);
    }
  }

  protected void updateLongNullTimeValue(boolean isXValueNull, long xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setLong(xValue);
    }
  }

  protected void addFloatInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        // Case A: Valid Time
        updateFloatFirstValue(
            xColumn.isNull(position), xColumn.getFloat(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        // Case B: Null Time
        updateFloatNullTimeValue(xColumn.isNull(position), xColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatFirstValue(boolean isXValueNull, float xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setFloat(xValue);
    }
  }

  protected void updateFloatNullTimeValue(boolean isXValueNull, float xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setFloat(xValue);
    }
  }

  protected void addDoubleInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateDoubleFirstValue(
            xColumn.isNull(position), xColumn.getDouble(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateDoubleNullTimeValue(xColumn.isNull(position), xColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleFirstValue(boolean isXValueNull, double xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setDouble(xValue);
    }
  }

  protected void updateDoubleNullTimeValue(boolean isXValueNull, double xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setDouble(xValue);
    }
  }

  protected void addBinaryInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateBinaryFirstValue(
            xColumn.isNull(position), xColumn.getBinary(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBinaryNullTimeValue(xColumn.isNull(position), xColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryFirstValue(boolean isXValueNull, Binary xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setBinary(xValue);
    }
  }

  protected void updateBinaryNullTimeValue(boolean isXValueNull, Binary xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setBinary(xValue);
    }
  }

  protected void addBooleanInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      // Check if the time is null
      if (!timeColumn.isNull(position)) {
        updateBooleanFirstValue(
            xColumn.isNull(position), xColumn.getBoolean(position), timeColumn.getLong(position));
        if (canFinishAfterInit) {
          return;
        }
      } else {
        updateBooleanNullTimeValue(xColumn.isNull(position), xColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanFirstValue(boolean isXValueNull, boolean xValue, long curTime) {
    if (checkAndUpdateFirstTime(isXValueNull, curTime)) {
      xResult.setBoolean(xValue);
    }
  }

  protected void updateBooleanNullTimeValue(boolean isXValueNull, boolean xValue) {
    if (checkAndUpdateNullTime(isXValueNull)) {
      xResult.setBoolean(xValue);
    }
  }
}
