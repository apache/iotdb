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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class LastValueAccumulator implements Accumulator {

  protected final TSDataType seriesDataType;
  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;

  public LastValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    lastValue = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(column, timeRange);
        break;
      case INT64:
        addLongInput(column, timeRange);
        break;
      case FLOAT:
        addFloatInput(column, timeRange);
        break;
      case DOUBLE:
        addDoubleInput(column, timeRange);
        break;
      case TEXT:
        addBinaryInput(column, timeRange);
        break;
      case BOOLEAN:
        addBooleanInput(column, timeRange);
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  // partialResult should be like: | LastValue | MaxTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of LastValue should be 2");
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
      case BOOLEAN:
        updateBooleanLastValue((boolean) statistics.getLastValue(), statistics.getEndTime());
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  // finalResult should be single column, like: | finalLastValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    lastValue.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be double in LastValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of LastValue should be 2");
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

  protected void addIntInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateIntLastValue(column[1].getInt(i), curTime);
      }
    }
  }

  protected void updateIntLastValue(int value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setInt(value);
    }
  }

  protected void addLongInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateLongLastValue(column[1].getLong(i), curTime);
      }
    }
  }

  protected void updateLongLastValue(long value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setLong(value);
    }
  }

  protected void addFloatInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateFloatLastValue(column[1].getFloat(i), curTime);
      }
    }
  }

  protected void updateFloatLastValue(float value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setFloat(value);
    }
  }

  protected void addDoubleInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateDoubleLastValue(column[1].getDouble(i), curTime);
      }
    }
  }

  protected void updateDoubleLastValue(double value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setDouble(value);
    }
  }

  protected void addBooleanInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateBooleanLastValue(column[1].getBoolean(i), curTime);
      }
    }
  }

  protected void updateBooleanLastValue(boolean value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBoolean(value);
    }
  }

  protected void addBinaryInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax() && !column[1].isNull(i)) {
        updateBinaryLastValue(column[1].getBinary(i), curTime);
      }
    }
  }

  protected void updateBinaryLastValue(Binary value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setBinary(value);
    }
  }
}
