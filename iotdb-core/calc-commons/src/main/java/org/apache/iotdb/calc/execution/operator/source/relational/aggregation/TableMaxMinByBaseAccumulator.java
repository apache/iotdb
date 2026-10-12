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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.Utils.calcTypeSize;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.Utils.serializeValue;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public abstract class TableMaxMinByBaseAccumulator
    implements TableAccumulator, TypeServices.MaxMinByValueUpdater {

  protected final TSDataType xDataType;

  protected final TSDataType yDataType;

  private final TsPrimitiveType yExtremeValue;

  private final TsPrimitiveType xResult;
  private final Type xType;
  private final TypeServices.PrimitiveColumnValueSetter xValueSetter;

  private boolean xNull = true;

  private boolean initResult;

  protected TableMaxMinByBaseAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xType = Type.fromTsDataType(xDataType);
    this.xResult = xType.getTsPrimitiveType();
    this.yExtremeValue = Type.fromTsDataType(yDataType).getTsPrimitiveType();
    this.xValueSetter = TypeServices.PRIMITIVE_COLUMN_VALUE_SETTER_SERVICE.call(xType);
  }

  // Column should be like: | x | y |
  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 2, "Length of input Column[] for MAX_BY/MIN_BY should be 2");
    TypeServices.MAX_MIN_BY_INPUT_SERVICE
        .call(Type.fromTsDataType(yDataType))
        .add(arguments, mask, this);
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MAX_BY_SLASH_MIN_BY_SHOULD_BE_BINARYCOLUMN_82B1BE6B);

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
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MAX_BY_SLASH_MIN_BY_SHOULD_BE_BINARYCOLUMN_82B1BE6B);

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

  @Override
  public void updateInt(int yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getInt())) {
      initResult = true;
      yExtremeValue.setInt(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateLong(long yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getLong())) {
      initResult = true;
      yExtremeValue.setLong(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateFloat(float yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getFloat())) {
      initResult = true;
      yExtremeValue.setFloat(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateDouble(double yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getDouble())) {
      initResult = true;
      yExtremeValue.setDouble(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateBinary(Binary yValue, Column xColumn, int xIndex) {
    if (!initResult || check(yValue, yExtremeValue.getBinary())) {
      initResult = true;
      yExtremeValue.setBinary(yValue);
      updateX(xColumn, xIndex);
    }
  }

  @Override
  public void updateBoolean(boolean yValue, Column xColumn, int xIndex) {
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
    xType.write(columnBuilder, xResult);
  }

  private void updateX(Column xColumn, int xIndex) {
    if (xColumn.isNull(xIndex)) {
      xNull = true;
    } else {
      xNull = false;
      xValueSetter.set(xResult, xColumn, xIndex);
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
    // Use a one-row column so the shared updater applies the same null and x-value semantics as
    // regular input processing.
    TypeServices.MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE
        .call(Type.fromTsDataType(yDataType))
        .update(bytes, xType, xType.createColumnBuilder(null, 1), this);
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
