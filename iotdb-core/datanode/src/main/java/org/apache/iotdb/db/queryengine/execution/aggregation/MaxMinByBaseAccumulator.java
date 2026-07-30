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

import org.apache.iotdb.calc.execution.aggregation.Accumulator;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.utils.TypeServices;
import org.apache.iotdb.db.utils.TypeServices.Aggregation.MaxMinByAccumulatorStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
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
public abstract class MaxMinByBaseAccumulator
    implements Accumulator, TypeServices.Aggregation.MaxMinByAccumulator {

  private final TSDataType xDataType;

  private final TSDataType yDataType;

  private final TsPrimitiveType yExtremeValue;

  private final TsPrimitiveType xResult;

  private final MaxMinByAccumulatorStrategy xStrategy;

  private final MaxMinByAccumulatorStrategy yStrategy;

  private boolean xNull = true;

  private boolean initResult;

  private long yTimeStamp = Long.MAX_VALUE;

  protected MaxMinByBaseAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    final Type xType = Type.fromTsDataType(xDataType);
    final Type yType = Type.fromTsDataType(yDataType);
    this.xResult = xType.getTsPrimitiveType();
    this.yExtremeValue = yType.getTsPrimitiveType();
    this.xStrategy = TypeServices.Aggregation.MAX_MIN_BY_ACCUMULATOR_STRATEGY_SERVICE.call(xType);
    this.yStrategy = TypeServices.Aggregation.MAX_MIN_BY_ACCUMULATOR_STRATEGY_SERVICE.call(yType);
    ensureSupported();
  }

  // Column should be like: | Time | x | y |
  @Override
  public void addInput(Column[] column, BitMap bitMap) {
    checkArgument(column.length == 3, "Length of input Column[] for MaxBy/MinBy should be 3");
    yStrategy.addInput(this, column, bitMap);
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

  @Override
  public void updateIntResult(long time, int yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getInt())
        || (yValue == yExtremeValue.getInt() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setInt(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateLongResult(long time, long yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getLong())
        || (yValue == yExtremeValue.getLong() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setLong(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateFloatResult(long time, float yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getFloat())
        || (yValue == yExtremeValue.getFloat() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setFloat(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateDoubleResult(long time, double yValue, Column xColumn, int xIndex) {
    if (!initResult
        || check(yValue, yExtremeValue.getDouble())
        || (yValue == yExtremeValue.getDouble() && time < yTimeStamp)) {
      initResult = true;
      yTimeStamp = time;
      yExtremeValue.setDouble(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateBinaryResult(long time, Binary yValue, Column xColumn, int xIndex) {
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
    xStrategy.writeXResult(columnBuilder, xResult);
  }

  private void updateX(Column xColumn, int xIndex) {
    if (xColumn.isNull(xIndex)) {
      xNull = true;
    } else {
      xNull = false;
      xStrategy.setXResult(xResult, xColumn, xIndex);
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
    Type.fromTsDataType(dataType).serialize(value, dataOutputStream);
  }

  private void updateFromBytesIntermediateInput(byte[] bytes) {
    final long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
    yStrategy.updateIntermediate(this, time, bytes, Long.BYTES);
  }

  @Override
  public Column readXFromBytesIntermediateInput(byte[] bytes, int offset) {
    // Use Column to preserve the existing null handling when updating the selected x value.
    final TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(xDataType));
    final ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
    final boolean isXNull = BytesUtils.bytesToBool(bytes, offset);
    if (isXNull) {
      columnBuilder.appendNull();
    } else {
      xStrategy.writeSerializedValue(bytes, offset + Byte.BYTES, columnBuilder);
    }
    return columnBuilder.build();
  }

  private void ensureSupported() {
    if (!xStrategy.isXSupported() || !yStrategy.isYSupported()) {
      final TSDataType unsupportedType = xStrategy.isXSupported() ? yDataType : xDataType;
      throw new UnSupportedDataTypeException(
          String.format(
              DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE_IN_MAX_MIN_BY_FMT, unsupportedType));
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
