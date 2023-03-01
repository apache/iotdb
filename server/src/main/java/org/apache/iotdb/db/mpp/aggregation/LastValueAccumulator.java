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

public class LastValueAccumulator implements Accumulator {

  protected final TSDataType seriesDataType;
  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;

  public LastValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    lastValue = TsPrimitiveType.getByType(seriesDataType);
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
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  // partialResult should be like: | LastValue | MaxTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of LastValue should be 2");
    if (partialResult[0].isNull(0)) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
        updateIntLastValue(partialResult[0].getInt(0), partialResult[1].getLong(0));
        break;
      case INT64:
        updateLongLastValue(partialResult[0].getLong(0), partialResult[1].getLong(0));
        break;
      case FLOAT:
        updateFloatLastValue(partialResult[0].getFloat(0), partialResult[1].getLong(0));
        break;
      case DOUBLE:
        updateDoubleLastValue(partialResult[0].getDouble(0), partialResult[1].getLong(0));
        break;
      case TEXT:
        updateBinaryLastValue(partialResult[0].getBinary(0), partialResult[1].getLong(0));
        break;
      case BOOLEAN:
        updateBooleanLastValue(partialResult[0].getBoolean(0), partialResult[1].getLong(0));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    switch (seriesDataType) {
      case INT32:
        updateIntLastValue((int) statistics.getLastValue(), statistics.getEndTime());
        break;
      case INT64:
        updateLongLastValue((long) statistics.getLastValue(), statistics.getEndTime());
        break;
      case FLOAT:
        updateFloatLastValue((float) statistics.getLastValue(), statistics.getEndTime());
        break;
      case DOUBLE:
        updateDoubleLastValue((double) statistics.getLastValue(), statistics.getEndTime());
        break;
      case TEXT:
        updateBinaryLastValue((Binary) statistics.getLastValue(), statistics.getEndTime());
        break;
      case BOOLEAN:
        updateBooleanLastValue((boolean) statistics.getLastValue(), statistics.getEndTime());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  // finalResult should be single column, like: | finalLastValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (!finalResult.isNull(0)) {
      initResult = true;
      switch (seriesDataType) {
        case INT32:
          lastValue.setInt(finalResult.getInt(0));
          break;
        case INT64:
          lastValue.setLong(finalResult.getLong(0));
          break;
        case FLOAT:
          lastValue.setFloat(finalResult.getFloat(0));
          break;
        case DOUBLE:
          lastValue.setDouble(finalResult.getDouble(0));
          break;
        case TEXT:
          lastValue.setBinary(finalResult.getBinary(0));
          break;
        case BOOLEAN:
          lastValue.setBoolean(finalResult.getBoolean(0));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in LastValue: %s", seriesDataType));
      }
    }
  }

  // columnBuilder should be double in LastValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of LastValue should be 2");
    if (!initResult) {
      columnBuilders[0].appendNull();
      columnBuilders[1].appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilders[0].writeInt(lastValue.getInt());
        break;
      case INT64:
        columnBuilders[0].writeLong(lastValue.getLong());
        break;
      case FLOAT:
        columnBuilders[0].writeFloat(lastValue.getFloat());
        break;
      case DOUBLE:
        columnBuilders[0].writeDouble(lastValue.getDouble());
        break;
      case TEXT:
        columnBuilders[0].writeBinary(lastValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilders[0].writeBoolean(lastValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Extreme: %s", seriesDataType));
    }
    columnBuilders[1].writeLong(maxTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt(lastValue.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(lastValue.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(lastValue.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(lastValue.getDouble());
        break;
      case TEXT:
        columnBuilder.writeBinary(lastValue.getBinary());
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(lastValue.getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Extreme: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.maxTime = Long.MIN_VALUE;
    this.lastValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {lastValue.getDataType(), TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return lastValue.getDataType();
  }

  protected void addIntInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateIntLastValue(column[1].getInt(i), column[0].getLong(i));
      }
    }
  }

  protected void updateIntLastValue(int value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setInt(value);
    }
  }

  protected void addLongInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateLongLastValue(column[1].getLong(i), column[0].getLong(i));
      }
    }
  }

  protected void updateLongLastValue(long value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setLong(value);
    }
  }

  protected void addFloatInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateFloatLastValue(column[1].getFloat(i), column[0].getLong(i));
      }
    }
  }

  protected void updateFloatLastValue(float value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateDoubleLastValue(column[1].getDouble(i), column[0].getLong(i));
      }
    }
  }

  protected void updateDoubleLastValue(double value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setDouble(value);
    }
  }

  protected void addBooleanInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBooleanLastValue(column[1].getBoolean(i), column[0].getLong(i));
      }
    }
  }

  protected void updateBooleanLastValue(boolean value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBoolean(value);
    }
  }

  protected void addBinaryInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBinaryLastValue(column[1].getBinary(i), column[0].getLong(i));
      }
    }
  }

  protected void updateBinaryLastValue(Binary value, long curTime) {
    initResult = true;
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBinary(value);
    }
  }
}
