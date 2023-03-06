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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class FirstValueAccumulator implements Accumulator {

  protected final TSDataType seriesDataType;
  protected boolean hasCandidateResult;
  protected TsPrimitiveType firstValue;
  protected long minTime = Long.MAX_VALUE;

  public FirstValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    firstValue = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    switch (seriesDataType) {
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
        addBinaryInput(column, bitMap, lastIndex);
        return;
      case BOOLEAN:
        addBooleanInput(column, bitMap, lastIndex);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FirstValue: %s", seriesDataType));
    }
  }

  // partialResult should be like: | FirstValue | MinTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of FirstValue should be 2");
    if (partialResult[0].isNull(0)) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
        updateIntFirstValue(partialResult[0].getInt(0), partialResult[1].getLong(0));
        break;
      case INT64:
        updateLongFirstValue(partialResult[0].getLong(0), partialResult[1].getLong(0));
        break;
      case FLOAT:
        updateFloatFirstValue(partialResult[0].getFloat(0), partialResult[1].getLong(0));
        break;
      case DOUBLE:
        updateDoubleFirstValue(partialResult[0].getDouble(0), partialResult[1].getLong(0));
        break;
      case TEXT:
        updateBinaryFirstValue(partialResult[0].getBinary(0), partialResult[1].getLong(0));
        break;
      case BOOLEAN:
        updateBooleanFirstValue(partialResult[0].getBoolean(0), partialResult[1].getLong(0));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FirstValue: %s", seriesDataType));
    }
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
        updateIntFirstValue((int) statistics.getFirstValue(), statistics.getStartTime());
        break;
      case INT64:
        updateLongFirstValue((long) statistics.getFirstValue(), statistics.getStartTime());
        break;
      case FLOAT:
        updateFloatFirstValue((float) statistics.getFirstValue(), statistics.getStartTime());
        break;
      case DOUBLE:
        updateDoubleFirstValue((double) statistics.getFirstValue(), statistics.getStartTime());
        break;
      case TEXT:
        updateBinaryFirstValue((Binary) statistics.getFirstValue(), statistics.getStartTime());
        break;
      case BOOLEAN:
        updateBooleanFirstValue((boolean) statistics.getFirstValue(), statistics.getStartTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FirstValue: %s", seriesDataType));
    }
  }

  // finalResult should be single column, like: | finalFirstValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (!finalResult.isNull(0)) {
      hasCandidateResult = true;
      switch (seriesDataType) {
        case INT32:
          firstValue.setInt(finalResult.getInt(0));
          break;
        case INT64:
          firstValue.setLong(finalResult.getLong(0));
          break;
        case FLOAT:
          firstValue.setFloat(finalResult.getFloat(0));
          break;
        case DOUBLE:
          firstValue.setDouble(finalResult.getDouble(0));
          break;
        case TEXT:
          firstValue.setBinary(finalResult.getBinary(0));
          break;
        case BOOLEAN:
          firstValue.setBoolean(finalResult.getBoolean(0));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in FirstValue: %s", seriesDataType));
      }
    }
  }

  // columnBuilder should be double in FirstValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of FirstValue should be 2");
    if (!hasCandidateResult) {
      columnBuilders[0].appendNull();
      columnBuilders[1].appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilders[0].writeInt(firstValue.getInt());
        break;
      case INT64:
        columnBuilders[0].writeLong(firstValue.getLong());
        break;
      case FLOAT:
        columnBuilders[0].writeFloat(firstValue.getFloat());
        break;
      case DOUBLE:
        columnBuilders[0].writeDouble(firstValue.getDouble());
        break;
      case TEXT:
        columnBuilders[0].writeBinary(firstValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilders[0].writeBoolean(firstValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Extreme: %s", seriesDataType));
    }
    columnBuilders[1].writeLong(minTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!hasCandidateResult) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt(firstValue.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(firstValue.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(firstValue.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(firstValue.getDouble());
        break;
      case TEXT:
        columnBuilder.writeBinary(firstValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(firstValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Extreme: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    this.minTime = Long.MAX_VALUE;
    this.firstValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {firstValue.getDataType(), TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return firstValue.getDataType();
  }

  protected void addIntInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateIntFirstValue(column[1].getInt(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateIntFirstValue(int value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setInt(value);
    }
  }

  protected void addLongInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateLongFirstValue(column[1].getLong(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateLongFirstValue(long value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setLong(value);
    }
  }

  protected void addFloatInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateFloatFirstValue(column[1].getFloat(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateFloatFirstValue(float value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateDoubleFirstValue(column[1].getDouble(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateDoubleFirstValue(double value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setDouble(value);
    }
  }

  protected void addBooleanInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBooleanFirstValue(column[1].getBoolean(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateBooleanFirstValue(boolean value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBoolean(value);
    }
  }

  protected void addBinaryInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBinaryFirstValue(column[1].getBinary(i), column[0].getLong(i));
        return;
      }
    }
  }

  protected void updateBinaryFirstValue(Binary value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setBinary(value);
    }
  }
}
