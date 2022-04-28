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
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

public class AvgAccumulator implements Accumulator {

  private TSDataType seriesDataType;
  private long countValue;
  private double sumValue;

  @Override
  public void addInput(Column[] column, TimeRange timeRange) {}

  @Override
  public void addIntermediate(Column[] partialResult) {}

  @Override
  public void addStatistics(Statistics statistics) {}

  @Override
  public void setFinal(Column finalResult) {}

  @Override
  public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {}

  @Override
  public void outputFinal(ColumnBuilder tsBlockBuilder) {}

  @Override
  public void reset() {
    this.countValue = 0;
    this.sumValue = 0.0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  private void updateAvg(TSDataType type, Object sumVal) throws UnSupportedDataTypeException {
    double val;
    switch (type) {
      case INT32:
        val = (int) sumVal;
        break;
      case INT64:
        val = (long) sumVal;
        break;
      case FLOAT:
        val = (float) sumVal;
        break;
      case DOUBLE:
        val = (double) sumVal;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", type));
    }
  }
}
