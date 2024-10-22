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
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_BY_AGGREGATION;

public class LastByAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastByAccumulator.class);

  private final TSDataType xDataType;
  private final TSDataType yDataType;

  private final boolean xIsTimeColumn;
  private final boolean yIsTimeColumn;

  private long yLastTime = Long.MIN_VALUE;

  private final TsPrimitiveType xResult;
  private boolean xIsNull = true;

  protected boolean initResult = false;

  public LastByAccumulator(
      TSDataType xDataType, TSDataType yDataType, boolean xIsTimeColumn, boolean yIsTimeColumn) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xIsTimeColumn = xIsTimeColumn;
    this.yIsTimeColumn = yIsTimeColumn;

    this.xResult = TsPrimitiveType.getByType(xDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new LastByAccumulator(xDataType, yDataType, xIsTimeColumn, yIsTimeColumn);
  }

  @Override
  public void addInput(Column[] arguments) {
    checkArgument(arguments.length == 3, "Length of input Column[] for LastBy should be 3");

    // arguments[0] is x column, arguments[1] is y column, arguments[2] is time column
    switch (xDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments[0], arguments[1], arguments[2]);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], arguments[1], arguments[2]);
        return;
      case FLOAT:
        addFloatInput(arguments[0], arguments[1], arguments[2]);
        return;
      case DOUBLE:
        addDoubleInput(arguments[0], arguments[1], arguments[2]);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments[0], arguments[1], arguments[2]);
        return;
      case BOOLEAN:
        addBooleanInput(arguments[0], arguments[1], arguments[2]);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastBy: %s", yDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn || argument instanceof RunLengthEncodedColumn,
        "intermediate input and output of LastBy should be BinaryColumn");

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
        if (!initResult || curTime > yLastTime) {
          initResult = true;
          yLastTime = curTime;
          xIsNull = true;
        }
        continue;
      }

      switch (xDataType) {
        case INT32:
        case DATE:
          int xIntVal = BytesUtils.bytesToInt(bytes, offset);
          updateIntLastValue(xIntVal, curTime);
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          updateLongLastValue(longVal, curTime);
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          updateFloatLastValue(floatVal, curTime);
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          updateDoubleLastValue(doubleVal, curTime);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          updateBinaryLastValue(binaryVal, curTime);
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          updateBooleanLastValue(boolVal, curTime);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in Last Aggregation: %s", yDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of LastBy should be BinaryColumn");

    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeTimeWithValue()));
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
      case STRING:
        columnBuilder.writeBinary(xResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(xResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastBy: %s", xDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {

    // only last_by(x, time) and last_by(time, x) can use statistics optimization

    Statistics xStatistics = statistics[0];
    Statistics yStatistics = statistics[1];

    if (yIsTimeColumn && yStatistics == null || xIsTimeColumn && xStatistics == null) {
      return;
    }

    if (yIsTimeColumn) {
      if (xStatistics == null || xStatistics.getEndTime() < yStatistics.getEndTime()) {
        if (!initResult || yStatistics.getEndTime() > yLastTime) {
          initResult = true;
          yLastTime = yStatistics.getEndTime();
          xIsNull = true;
        }
      } else {
        if (!initResult || yStatistics.getEndTime() > yLastTime) {
          initResult = true;
          yLastTime = yStatistics.getEndTime();
          xIsNull = false;

          if (xStatistics instanceof TimeStatistics) {
            xResult.setLong(xStatistics.getEndTime());
            return;
          }

          switch (xDataType) {
            case INT32:
            case DATE:
              xResult.setInt((int) xStatistics.getLastValue());
              break;
            case INT64:
            case TIMESTAMP:
              xResult.setLong((long) xStatistics.getLastValue());
              break;
            case FLOAT:
              xResult.setFloat((float) statistics[0].getLastValue());
              break;
            case DOUBLE:
              xResult.setDouble((double) statistics[0].getLastValue());
              break;
            case TEXT:
            case BLOB:
            case STRING:
              xResult.setBinary((Binary) statistics[0].getLastValue());
              break;
            case BOOLEAN:
              xResult.setBoolean((boolean) statistics[0].getLastValue());
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format(
                      "Unsupported data type: %s in Aggregation: %s",
                      yDataType, LAST_BY_AGGREGATION));
          }
        }
      }
    } else {
      // x is time column
      if (yStatistics != null && (!initResult || yStatistics.getEndTime() > yLastTime)) {
        initResult = true;
        xIsNull = false;
        yLastTime = yStatistics.getEndTime();
        xResult.setLong(yStatistics.getEndTime());
      }
    }
  }

  @Override
  public void reset() {
    initResult = false;
    xIsNull = true;
    this.yLastTime = Long.MIN_VALUE;
    this.xResult.reset();
  }

  private byte[] serializeTimeWithValue() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeLong(yLastTime);
      dataOutputStream.writeBoolean(xIsNull);
      if (!xIsNull) {
        switch (xDataType) {
          case INT32:
          case DATE:
            dataOutputStream.writeInt(xResult.getInt());
            break;
          case INT64:
          case TIMESTAMP:
            dataOutputStream.writeLong(xResult.getLong());
            break;
          case FLOAT:
            dataOutputStream.writeFloat(xResult.getFloat());
            break;
          case DOUBLE:
            dataOutputStream.writeDouble(xResult.getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            dataOutputStream.writeInt(xResult.getBinary().getValues().length);
            dataOutputStream.write(xResult.getBinary().getValues());
            break;
          case BOOLEAN:
            dataOutputStream.writeBoolean(xResult.getBoolean());
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Unsupported data type: %s in aggregation %s", xDataType, LAST_BY_AGGREGATION));
        }
      }
    } catch (IOException e) {
      throw new UnsupportedOperationException(
          String.format(
              "Failed to serialize intermediate result for Accumulator %s, errorMsg: %s.",
              LAST_BY_AGGREGATION, e.getMessage()));
    }
    return byteArrayOutputStream.toByteArray();
  }

  // TODO can add last position optimization if last position is null ?
  private void addIntInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateIntLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateIntLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setInt(xColumn.getInt(xIdx));
      }
    }
  }

  protected void updateIntLastValue(int val, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setInt(val);
    }
  }

  private void addLongInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateLongLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateLongLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setLong(xColumn.getLong(xIdx));
      }
    }
  }

  protected void updateLongLastValue(long value, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setLong(value);
    }
  }

  private void addFloatInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateFloatLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateFloatLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setFloat(xColumn.getFloat(xIdx));
      }
    }
  }

  protected void updateFloatLastValue(float value, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setFloat(value);
    }
  }

  private void addDoubleInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateDoubleLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateDoubleLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setDouble(xColumn.getDouble(xIdx));
      }
    }
  }

  protected void updateDoubleLastValue(double val, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setDouble(val);
    }
  }

  private void addBinaryInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateBinaryLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateBinaryLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setBinary(xColumn.getBinary(xIdx));
      }
    }
  }

  protected void updateBinaryLastValue(Binary val, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setBinary(val);
    }
  }

  private void addBooleanInput(Column xColumn, Column yColumn, Column timeColumn) {
    for (int i = 0; i < yColumn.getPositionCount(); i++) {
      if (!yColumn.isNull(i)) {
        updateBooleanLastValue(xColumn, i, timeColumn.getLong(i));
      }
    }
  }

  protected void updateBooleanLastValue(Column xColumn, int xIdx, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      if (xColumn.isNull(xIdx)) {
        xIsNull = true;
      } else {
        xIsNull = false;
        xResult.setBoolean(xColumn.getBoolean(xIdx));
      }
    }
  }

  protected void updateBooleanLastValue(boolean val, long curTime) {
    if (!initResult || curTime > yLastTime) {
      initResult = true;
      yLastTime = curTime;
      xIsNull = false;
      xResult.setBoolean(val);
    }
  }
}
