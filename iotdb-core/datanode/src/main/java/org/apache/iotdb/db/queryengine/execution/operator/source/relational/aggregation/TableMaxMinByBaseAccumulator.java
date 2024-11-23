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
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.calcTypeSize;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeValue;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public abstract class TableMaxMinByBaseAccumulator implements TableAccumulator {

  protected final TSDataType xDataType;

  protected final TSDataType yDataType;

  private final TsPrimitiveType yExtremeValue;

  private final TsPrimitiveType xResult;

  private boolean xNull = true;

  private boolean initResult;

  public static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type in MaxBy/MinBy: %s";

  protected TableMaxMinByBaseAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xResult = TsPrimitiveType.getByType(xDataType);
    this.yExtremeValue = TsPrimitiveType.getByType(yDataType);
  }

  // Column should be like: | x | y |
  @Override
  public void addInput(Column[] arguments) {
    checkArgument(arguments.length == 2, "Length of input Column[] for MaxBy/MinBy should be 2");
    switch (yDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments);
        return;
      case FLOAT:
        addFloatInput(arguments);
        return;
      case DOUBLE:
        addDoubleInput(arguments);
        return;
      case STRING:
      case TEXT:
      case BLOB:
        addBinaryInput(arguments);
        return;
      case BOOLEAN:
        addBooleanInput(arguments);
        return;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of max_by/min_by should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      updateFromBytesIntermediateInput(bytes);
    }
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of Max_By/Min_By should be BinaryColumn");

    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    columnBuilder.writeBinary(new Binary(serialize()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    writeX(columnBuilder);
  }

  @Override
  public void reset() {
    initResult = false;
    xNull = true;
    this.xResult.reset();
    this.yExtremeValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  private void addIntInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateIntResult(column[1].getInt(i), column[0], i);
      }
    }
  }

  private void updateIntResult(int yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getInt())) {
      initResult = true;
      yExtremeValue.setInt(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addLongInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateLongResult(column[1].getLong(i), column[0], i);
      }
    }
  }

  private void updateLongResult(long yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getLong())) {
      initResult = true;
      yExtremeValue.setLong(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addFloatInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateFloatResult(column[1].getFloat(i), column[0], i);
      }
    }
  }

  private void updateFloatResult(float yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getFloat())) {
      initResult = true;
      yExtremeValue.setFloat(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addDoubleInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateDoubleResult(column[1].getDouble(i), column[0], i);
      }
    }
  }

  private void updateDoubleResult(double yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getDouble())) {
      initResult = true;
      yExtremeValue.setDouble(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addBinaryInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateBinaryResult(column[1].getBinary(i), column[0], i);
      }
    }
  }

  private void updateBinaryResult(Binary yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getBinary())) {
      initResult = true;
      yExtremeValue.setBinary(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addBooleanInput(Column[] column) {
    int count = column[1].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column[1].isNull(i)) {
        updateBooleanResult(column[1].getBoolean(i), column[0], i);
      }
    }
  }

  private void updateBooleanResult(boolean yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getBoolean())) {
      initResult = true;
      yExtremeValue.setBoolean(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void writeX(ColumnBuilder columnBuilder) {
    if (xNull) {
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
      case STRING:
      case BLOB:
        columnBuilder.writeBinary(xResult.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(xResult.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, xDataType));
    }
  }

  private void updateX(Column xColumn, int xIndex) {
    if (xColumn.isNull(xIndex)) {
      xNull = true;
    } else {
      xNull = false;
      switch (xDataType) {
        case INT32:
        case DATE:
          xResult.setInt(xColumn.getInt(xIndex));
          break;
        case INT64:
        case TIMESTAMP:
          xResult.setLong(xColumn.getLong(xIndex));
          break;
        case FLOAT:
          xResult.setFloat(xColumn.getFloat(xIndex));
          break;
        case DOUBLE:
          xResult.setDouble(xColumn.getDouble(xIndex));
          break;
        case TEXT:
        case STRING:
        case BLOB:
          xResult.setBinary(xColumn.getBinary(xIndex));
          break;
        case BOOLEAN:
          xResult.setBoolean(xColumn.getBoolean(xIndex));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(UNSUPPORTED_TYPE_MESSAGE, xDataType));
      }
    }
  }

  private byte[] serialize() {
    byte[] valueBytes;
    int yLength = calcTypeSize(yDataType, yExtremeValue);
    if (xNull) {
      valueBytes = new byte[yLength + 1];
      serializeValue(yDataType, yExtremeValue, valueBytes, 0);
      BytesUtils.boolToBytes(true, valueBytes, yLength);
    } else {
      valueBytes = new byte[yLength + 1 + calcTypeSize(xDataType, xResult)];
      int offset = 0;
      serializeValue(yDataType, yExtremeValue, valueBytes, offset);
      offset = yLength;

      BytesUtils.boolToBytes(false, valueBytes, offset);
      offset += 1;

      serializeValue(xDataType, xResult, valueBytes, offset);
    }
    return valueBytes;
  }

  private void updateFromBytesIntermediateInput(byte[] bytes) {
    int offset = 0;
    // Use Column to store x value
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(xDataType));
    ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
    switch (yDataType) {
      case INT32:
      case DATE:
        int intMaxVal = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateIntResult(intMaxVal, columnBuilder.build(), 0);
        break;
      case INT64:
      case TIMESTAMP:
        long longMaxVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateLongResult(longMaxVal, columnBuilder.build(), 0);
        break;
      case FLOAT:
        float floatMaxVal = BytesUtils.bytesToFloat(bytes, offset);
        offset += Float.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateFloatResult(floatMaxVal, columnBuilder.build(), 0);
        break;
      case DOUBLE:
        double doubleMaxVal = BytesUtils.bytesToDouble(bytes, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateDoubleResult(doubleMaxVal, columnBuilder.build(), 0);
        break;
      case STRING:
      case TEXT:
      case BLOB:
        int length = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        Binary binaryMaxVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
        offset += length;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateBinaryResult(binaryMaxVal, columnBuilder.build(), 0);
        break;
      case BOOLEAN:
        boolean booleanMaxVal = BytesUtils.bytesToBool(bytes, offset);
        offset += 1;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateBooleanResult(booleanMaxVal, columnBuilder.build(), 0);
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
  }

  private void readXFromBytesIntermediateInput(
      byte[] bytes, int offset, ColumnBuilder columnBuilder) {
    boolean isXNull = BytesUtils.bytesToBool(bytes, offset);
    offset += 1;
    if (isXNull) {
      columnBuilder.appendNull();
    } else {
      switch (xDataType) {
        case INT32:
        case DATE:
          columnBuilder.writeInt(BytesUtils.bytesToInt(bytes, offset));
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(BytesUtils.bytesToLongFromOffset(bytes, 8, offset));
          break;
        case FLOAT:
          columnBuilder.writeFloat(BytesUtils.bytesToFloat(bytes, offset));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(BytesUtils.bytesToDouble(bytes, offset));
          break;
        case TEXT:
        case STRING:
        case BLOB:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          columnBuilder.writeBinary(new Binary(BytesUtils.subBytes(bytes, offset, length)));
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(BytesUtils.bytesToBool(bytes, offset));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(UNSUPPORTED_TYPE_MESSAGE, xDataType));
      }
    }
  }

  /**
   * @param yValue Input y.
   * @param yExtremeValue Current extreme value of y.
   * @return True if yValue is the new extreme value.
   */
  protected abstract boolean check(int yValue, int yExtremeValue);

  protected abstract boolean check(long yValue, long yExtremeValue);

  protected abstract boolean check(float yValue, float yExtremeValue);

  protected abstract boolean check(double yValue, double yExtremeValue);

  protected abstract boolean check(Binary yValue, Binary yExtremeValue);

  protected abstract boolean check(boolean yValue, boolean yExtremeValue);
}
