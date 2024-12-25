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

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public abstract class MaxMinByBaseAccumulator implements Accumulator {

  private final TSDataType xDataType;

  private final TSDataType yDataType;

  private final TsPrimitiveType yExtremeValue;

  private final TsPrimitiveType xResult;

  private boolean xNull = true;

  private boolean initResult;

  private long yTimeStamp = Long.MAX_VALUE;

  private static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type in MaxBy/MinBy: %s";

  protected MaxMinByBaseAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xResult = TsPrimitiveType.getByType(xDataType);
    this.yExtremeValue = TsPrimitiveType.getByType(yDataType);
  }

  // Column should be like: | Time | x | y |
  @Override
  public void addInput(Column[] column, BitMap bitMap) {
    checkArgument(column.length == 3, "Length of input Column[] for MaxBy/MinBy should be 3");
    switch (yDataType) {
      case INT32:
      case DATE:
        addIntInput(column, bitMap);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(column, bitMap);
        return;
      case FLOAT:
        addFloatInput(column, bitMap);
        return;
      case DOUBLE:
        addDoubleInput(column, bitMap);
        return;
      case STRING:
        addBinaryInput(column, bitMap);
        return;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
  }

  // partialResult should be like: | partialMaxByBinary |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MaxBy/MinBy should be 1");
    // Return if y is null.
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    updateFromBytesIntermediateInput(bytes);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  // finalResult should be single column, like: | finalXValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    updateX(finalResult, 0);
  }

  // columnBuilders should be like | TextIntermediateColumnBuilder |
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MaxValue should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
      return;
    }
    columnBuilders[0].writeBinary(new Binary(serialize()));
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
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
    yTimeStamp = Long.MAX_VALUE;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return xDataType;
  }

  private void addIntInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateIntResult(column[0].getLong(i), column[2].getInt(i), column[1], i);
      }
    }
  }

  private void updateIntResult(long time, int yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getInt())
        || (yValue == yExtremeValue.getInt() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setInt(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addLongInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateLongResult(column[0].getLong(i), column[2].getLong(i), column[1], i);
      }
    }
  }

  private void updateLongResult(long time, long yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getLong())
        || (yValue == yExtremeValue.getLong() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setLong(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addFloatInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateFloatResult(column[0].getLong(i), column[2].getFloat(i), column[1], i);
      }
    }
  }

  private void updateFloatResult(long time, float yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getFloat())
        || (yValue == yExtremeValue.getFloat() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setFloat(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addDoubleInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateDoubleResult(column[0].getLong(i), column[2].getDouble(i), column[1], i);
      }
    }
  }

  private void updateDoubleResult(long time, double yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getDouble())
        || (yValue == yExtremeValue.getDouble() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setDouble(yValue);
      updateX(xColumn, xIndex);
    }
  }

  private void addBinaryInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateBinaryResult(column[0].getLong(i), column[2].getBinary(i), column[1], i);
      }
    }
  }

  private void updateBinaryResult(long time, Binary yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getBinary())
        || (yValue.compareTo(yExtremeValue.getBinary()) == 0 && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setBinary(yValue);
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
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeLong(yTimeStamp);
      writeIntermediateToStream(yDataType, yExtremeValue, dataOutputStream);
      dataOutputStream.writeBoolean(xNull);
      if (!xNull) {
        writeIntermediateToStream(xDataType, xResult, dataOutputStream);
      }
    } catch (IOException e) {
      throw new UnsupportedOperationException(
          "Failed to serialize intermediate result for MaxByAccumulator.", e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  private void writeIntermediateToStream(
      TSDataType dataType, TsPrimitiveType value, DataOutputStream dataOutputStream)
      throws IOException {
    switch (dataType) {
      case INT32:
      case DATE:
        dataOutputStream.writeInt(value.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        dataOutputStream.writeLong(value.getLong());
        break;
      case FLOAT:
        dataOutputStream.writeFloat(value.getFloat());
        break;
      case DOUBLE:
        dataOutputStream.writeDouble(value.getDouble());
        break;
      case TEXT:
      case STRING:
      case BLOB:
        String content = value.getBinary().toString();
        dataOutputStream.writeInt(content.length());
        dataOutputStream.writeBytes(content);
        break;
      case BOOLEAN:
        dataOutputStream.writeBoolean(value.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, dataType));
    }
  }

  private void updateFromBytesIntermediateInput(byte[] bytes) {
    long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
    int offset = Long.BYTES;
    // Use Column to store x value
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(xDataType));
    ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
    switch (yDataType) {
      case INT32:
      case DATE:
        int intMaxVal = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateIntResult(time, intMaxVal, columnBuilder.build(), 0);
        break;
      case INT64:
      case TIMESTAMP:
        long longMaxVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateLongResult(time, longMaxVal, columnBuilder.build(), 0);
        break;
      case FLOAT:
        float floatMaxVal = BytesUtils.bytesToFloat(bytes, offset);
        offset += Float.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateFloatResult(time, floatMaxVal, columnBuilder.build(), 0);
        break;
      case DOUBLE:
        double doubleMaxVal = BytesUtils.bytesToDouble(bytes, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateDoubleResult(time, doubleMaxVal, columnBuilder.build(), 0);
        break;
      case STRING:
        int length = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        Binary binaryMaxVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
        offset += length;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateBinaryResult(time, binaryMaxVal, columnBuilder.build(), 0);
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
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
}
