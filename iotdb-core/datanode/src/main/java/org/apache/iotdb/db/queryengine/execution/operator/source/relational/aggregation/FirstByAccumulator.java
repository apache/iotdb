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
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;

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
      boolean isXNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;

      if (isXNull) {
        if (!initResult || curTime < yFirstTime) {
          initResult = true;
          yFirstTime = curTime;
          xIsNull = true;
        }
        continue;
      }

      switch (xDataType) {
        case INT32:
        case DATE:
          int xIntVal = BytesUtils.bytesToInt(bytes, offset);
          updateIntFirstValue(xIntVal, curTime);
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          updateLongFirstValue(longVal, curTime);
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          updateFloatFirstValue(floatVal, curTime);
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          updateDoubleFirstValue(doubleVal, curTime);
          break;
        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          updateBinaryFirstValue(binaryVal, curTime);
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          updateBooleanFirstValue(boolVal, curTime);
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

    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(
          new Binary(serializeTimeValue(xDataType, yFirstTime, xIsNull, xResult)));
    }
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult || xIsNull) {
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
    xIsNull = true;
    this.yFirstTime = Long.MAX_VALUE;
    this.xResult.reset();
  }

  protected void addIntInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateIntFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateIntFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateIntFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setInt(xColumn.getInt(xIdx));
      }
    }
  }

  protected void updateIntFirstValue(int val, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setInt(val);
    }
  }

  protected void addLongInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateLongFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateLongFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateLongFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setLong(xColumn.getLong(xIdx));
      }
    }
  }

  protected void updateLongFirstValue(long value, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setLong(value);
    }
  }

  protected void addFloatInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateFloatFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateFloatFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateFloatFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setFloat(xColumn.getFloat(xIdx));
      }
    }
  }

  protected void updateFloatFirstValue(float value, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setFloat(value);
    }
  }

  protected void addDoubleInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateDoubleFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateDoubleFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateDoubleFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setDouble(xColumn.getDouble(xIdx));
      }
    }
  }

  protected void updateDoubleFirstValue(double val, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setDouble(val);
    }
  }

  protected void addBinaryInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateBinaryFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateBinaryFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateBinaryFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setBinary(xColumn.getBinary(xIdx));
      }
    }
  }

  protected void updateBinaryFirstValue(Binary val, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setBinary(val);
    }
  }

  protected void addBooleanInput(
      Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!yColumn.isNull(i)) {
          updateBooleanFirstValue(xColumn, i, timeColumn.getLong(i));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!yColumn.isNull(position)) {
          updateBooleanFirstValue(xColumn, position, timeColumn.getLong(position));
          if (canFinishAfterInit) {
            return;
          }
        }
      }
    }
  }

  protected void updateBooleanFirstValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setBoolean(xColumn.getBoolean(xIdx));
      }
    }
  }

  protected void updateBooleanFirstValue(boolean val, long curTime) {
    if (!initResult || curTime < yFirstTime) {
      initResult = true;
      yFirstTime = curTime;
      xIsNull = false;
      xResult.setBoolean(val);
    }
  }
}
