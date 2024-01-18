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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public class MaxByAccumulator implements Accumulator {

  private final TSDataType xDataType;

  private final TSDataType yDataType;

  private final TsPrimitiveType yMaxValue;

  private final TsPrimitiveType xResult;

  private boolean xNull = true;

  private boolean initResult;

  private static final String UNSUPPORTED_TYPE_MESSAGE = "Unsupported data type in MaxBy: %s";

  public MaxByAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xResult = TsPrimitiveType.getByType(xDataType);
    this.yMaxValue = TsPrimitiveType.getByType(yDataType);
  }

  // Column should be like: | Time | x | y |
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    switch (yDataType) {
      case INT32:
        addIntInput(column, bitMap, lastIndex);
        return;
      case INT64:
        addLongInput(column, bitMap, lastIndex);
        return;
      case FLOAT:
        addFloatInput(column, bitMap, lastIndex);
        return;
      case DOUBLE:
        addDoubleInput(column, bitMap, lastIndex);
        return;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
  }

  // partialResult should be like: | partialX | partialY |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of MaxBy should be 2");
    // Return if y is null.
    if (partialResult[1].isNull(0)) {
      return;
    }
    switch (yDataType) {
      case INT32:
        updateIntResult(partialResult[1].getInt(0), partialResult[0], 0);
        break;
      case INT64:
        updateLongResult(partialResult[1].getLong(0), partialResult[0], 0);
        break;
      case FLOAT:
        updateFloatResult(partialResult[1].getFloat(0), partialResult[0], 0);
        break;
      case DOUBLE:
        updateDoubleResult(partialResult[1].getDouble(0), partialResult[0], 0);
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
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
    if (finalResult.isNull(0)) {
      xNull = true;
      return;
    }
    switch (xDataType) {
      case INT32:
        xResult.setInt(finalResult.getInt(0));
        break;
      case INT64:
        xResult.setLong(finalResult.getLong(0));
        break;
      case FLOAT:
        xResult.setFloat(finalResult.getFloat(0));
        break;
      case DOUBLE:
        xResult.setDouble(finalResult.getDouble(0));
        break;
      case TEXT:
        xResult.setBinary(finalResult.getBinary(0));
        break;
      case BOOLEAN:
        xResult.setBoolean(finalResult.getBoolean(0));
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, xDataType));
    }
  }

  // columnBuilders should be like | xColumnBuilder | yColumnBuilder |
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of MaxValue should be 2");
    if (!initResult) {
      columnBuilders[0].appendNull();
      columnBuilders[1].appendNull();
      return;
    }
    switch (yDataType) {
      case INT32:
        writeX(columnBuilders[0]);
        columnBuilders[1].writeInt(yMaxValue.getInt());
        break;
      case INT64:
        writeX(columnBuilders[0]);
        columnBuilders[1].writeLong(yMaxValue.getLong());
        break;
      case FLOAT:
        writeX(columnBuilders[0]);
        columnBuilders[1].writeFloat(yMaxValue.getFloat());
        break;
      case DOUBLE:
        writeX(columnBuilders[0]);
        columnBuilders[1].writeDouble(yMaxValue.getDouble());
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(String.format(UNSUPPORTED_TYPE_MESSAGE, yDataType));
    }
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
    this.yMaxValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {xDataType, yDataType};
  }

  @Override
  public TSDataType getFinalType() {
    return xDataType;
  }

  private void addIntInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateIntResult(column[2].getInt(i), column[1], i);
      }
    }
  }

  private void updateIntResult(int yMaxVal, Column xColumn, int xIndex) {
    if (!initResult || yMaxVal > yMaxValue.getInt()) {
      initResult = true;
      yMaxValue.setInt(yMaxVal);
      updateX(xColumn, xIndex);
    }
  }

  private void addLongInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateLongResult(column[2].getLong(i), column[1], i);
      }
    }
  }

  private void updateLongResult(long yMaxVal, Column xColumn, int xIndex) {
    if (!initResult || yMaxVal > yMaxValue.getLong()) {
      initResult = true;
      yMaxValue.setLong(yMaxVal);
      updateX(xColumn, xIndex);
    }
  }

  private void addFloatInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateFloatResult(column[2].getFloat(i), column[1], i);
      }
    }
  }

  private void updateFloatResult(float yMaxVal, Column xColumn, int xIndex) {
    if (!initResult || yMaxVal > yMaxValue.getFloat()) {
      initResult = true;
      yMaxValue.setFloat(yMaxVal);
      updateX(xColumn, xIndex);
    }
  }

  private void addDoubleInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[2].isNull(i)) {
        updateDoubleResult(column[2].getDouble(i), column[1], i);
      }
    }
  }

  private void updateDoubleResult(double yMaxVal, Column xColumn, int xIndex) {
    if (!initResult || yMaxVal > yMaxValue.getDouble()) {
      initResult = true;
      yMaxValue.setDouble(yMaxVal);
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
        columnBuilder.writeInt(xResult.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(xResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(xResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(xResult.getDouble());
        break;
      case TEXT:
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
          xResult.setInt(xColumn.getInt(xIndex));
          break;
        case INT64:
          xResult.setLong(xColumn.getLong(xIndex));
          break;
        case FLOAT:
          xResult.setFloat(xColumn.getFloat(xIndex));
          break;
        case DOUBLE:
          xResult.setDouble(xColumn.getDouble(xIndex));
          break;
        case TEXT:
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
}
