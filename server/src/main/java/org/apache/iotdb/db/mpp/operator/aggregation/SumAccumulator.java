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

package org.apache.iotdb.db.mpp.operator.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class SumAccumulator implements Accumulator {

  private TSDataType seriesDataType;
  private double sumValue = 0;

  public SumAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      updateSumValue(column[1].getObject(i));
    }
  }

  // partialResult should be like: | partialSumValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of Sum should be 1");
    }
    updateSumValue(partialResult[0].getObject(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics instanceof IntegerStatistics) {
      sumValue += statistics.getSumLongValue();
    } else {
      sumValue += statistics.getSumDoubleValue();
    }
  }

  // finalResult should be single column, like: | finalSumValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    updateSumValue(finalResult.getObject(0));
  }

  // columnBuilder should be single in countAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeDouble(sumValue);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeDouble(sumValue);
  }

  @Override
  public void reset() {
    this.sumValue = 0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.DOUBLE};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.DOUBLE;
  }

  private void updateSumValue(Object sumVal) throws UnSupportedDataTypeException {
    switch (seriesDataType) {
      case INT32:
        sumValue += (int) sumVal;
        break;
      case INT64:
        sumValue = (long) sumVal;
        break;
      case FLOAT:
        sumValue = (float) sumVal;
        break;
      case DOUBLE:
        sumValue = (double) sumVal;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", seriesDataType));
    }
  }
}
