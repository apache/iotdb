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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class MinValueAccumulator implements Accumulator {

  private final TSDataType seriesDataType;
  private final TsPrimitiveType minResult;
  private boolean initResult = false;

  public MinValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.minResult = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] columns, BitMap bitMap) {
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(columns, bitMap);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(columns, bitMap);
        return;
      case FLOAT:
        addFloatInput(columns, bitMap);
        return;
      case DOUBLE:
        addDoubleInput(columns, bitMap);
        return;
      case STRING:
        addBinaryInput(columns, bitMap);
        return;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  // partialResult should be like: | partialMinValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MinValue should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        updateIntResult(partialResult[0].getInt(0));
        break;
      case INT64:
      case TIMESTAMP:
        updateLongResult(partialResult[0].getLong(0));
        break;
      case FLOAT:
        updateFloatResult(partialResult[0].getFloat(0));
        break;
      case DOUBLE:
        updateDoubleResult(partialResult[0].getDouble(0));
        break;
      case STRING:
        updateBinaryResult(partialResult[0].getBinary(0));
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        updateIntResult((int) statistics.getMinValue());
        break;
      case INT64:
      case TIMESTAMP:
        updateLongResult((long) statistics.getMinValue());
        break;
      case FLOAT:
        updateFloatResult((float) statistics.getMinValue());
        break;
      case DOUBLE:
        updateDoubleResult((double) statistics.getMinValue());
        break;
      case STRING:
        updateBinaryResult((Binary) statistics.getMinValue());
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    switch (seriesDataType) {
      case INT32:
      case DATE:
        minResult.setInt(finalResult.getInt(0));
        break;
      case INT64:
      case TIMESTAMP:
        minResult.setLong(finalResult.getLong(0));
        break;
      case FLOAT:
        minResult.setFloat(finalResult.getFloat(0));
        break;
      case DOUBLE:
        minResult.setDouble(finalResult.getDouble(0));
        break;
      case STRING:
        minResult.setBinary(finalResult.getBinary(0));
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  // columnBuilder should be single in MinValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MinValue should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        columnBuilders[0].writeInt(minResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilders[0].writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilders[0].writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilders[0].writeDouble(minResult.getDouble());
        break;
      case STRING:
        columnBuilders[0].writeBinary(minResult.getBinary());
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
      case DATE:
        columnBuilder.writeInt(minResult.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(minResult.getDouble());
        break;
      case STRING:
        columnBuilder.writeBinary(minResult.getBinary());
        break;
      case TEXT:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.minResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {minResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return minResult.getDataType();
  }

  private void addIntInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateIntResult(column[1].getInt(i));
      }
    }
  }

  private void updateIntResult(int minVal) {
    if (!initResult || minVal < minResult.getInt()) {
      initResult = true;
      minResult.setInt(minVal);
    }
  }

  private void addLongInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      // skip null value in control column
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateLongResult(column[1].getLong(i));
      }
    }
  }

  private void updateLongResult(long minVal) {
    if (!initResult || minVal < minResult.getLong()) {
      initResult = true;
      minResult.setLong(minVal);
    }
  }

  private void addFloatInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateFloatResult(column[1].getFloat(i));
      }
    }
  }

  private void updateFloatResult(float minVal) {
    if (!initResult || minVal < minResult.getFloat()) {
      initResult = true;
      minResult.setFloat(minVal);
    }
  }

  private void addDoubleInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateDoubleResult(column[1].getDouble(i));
      }
    }
  }

  private void updateDoubleResult(double minVal) {
    if (!initResult || minVal < minResult.getDouble()) {
      initResult = true;
      minResult.setDouble(minVal);
    }
  }

  private void addBinaryInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBinaryResult(column[1].getBinary(i));
      }
    }
  }

  private void updateBinaryResult(Binary minVal) {
    if (!initResult || minVal.compareTo(minResult.getBinary()) < 0) {
      initResult = true;
      minResult.setBinary(minVal);
    }
  }
}
