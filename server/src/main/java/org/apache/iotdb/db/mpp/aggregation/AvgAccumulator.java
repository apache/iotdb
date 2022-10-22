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

import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import static com.google.common.base.Preconditions.checkArgument;

public class AvgAccumulator implements Accumulator {

  private TSDataType seriesDataType;
  private long countValue;
  private double sumValue;
  private boolean initResult = false;

  public AvgAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public int addInput(Column[] column, IWindow curWindow) {
    switch (seriesDataType) {
      case INT32:
        return addIntInput(column, curWindow);
      case INT64:
        return addLongInput(column, curWindow);
      case FLOAT:
        return addFloatInput(column, curWindow);
      case DOUBLE:
        return addDoubleInput(column, curWindow);
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", seriesDataType));
    }
  }

  // partialResult should be like: | countValue1 | sumValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 2, "partialResult of Avg should be 2");
    if (partialResult[0].isNull(0)) {
      return;
    }
    initResult = true;
    countValue += partialResult[0].getLong(0);
    sumValue += partialResult[1].getDouble(0);
    if (countValue == 0) {
      initResult = false;
    }
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    initResult = true;
    countValue += statistics.getCount();
    if (statistics instanceof IntegerStatistics) {
      sumValue += statistics.getSumLongValue();
    } else {
      sumValue += statistics.getSumDoubleValue();
    }
    if (countValue == 0) {
      initResult = false;
    }
  }

  // Set sumValue to finalResult and keep countValue equals to 1
  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    countValue = 1;
    sumValue = finalResult.getDouble(0);
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 2, "partialResult of Avg should be 2");
    if (!initResult) {
      columnBuilders[0].appendNull();
      columnBuilders[1].appendNull();
    } else {
      columnBuilders[0].writeLong(countValue);
      columnBuilders[1].writeDouble(sumValue);
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValue / countValue);
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.countValue = 0;
    this.sumValue = 0.0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.INT64, TSDataType.DOUBLE};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.DOUBLE;
  }

  private int addIntInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column[2].getInt(i);
      }
    }
    return curPositionCount;
  }

  private int addLongInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column[2].getLong(i);
      }
    }
    return curPositionCount;
  }

  private int addFloatInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column[2].getFloat(i);
      }
    }
    return curPositionCount;
  }

  private int addDoubleInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();

    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);
      if (!column[2].isNull(i)) {
        initResult = true;
        countValue++;
        sumValue += column[2].getDouble(i);
      }
    }
    return curPositionCount;
  }
}
