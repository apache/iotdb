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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

public class MaxTimeAccumulator implements Accumulator {

  private long maxTime = Long.MIN_VALUE;

  public MaxTimeAccumulator() {}

  // Column should be like: | Time |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax()) {
        updateMaxTime(curTime);
      }
    }
  }

  // partialResult should be like: | partialMaxTimeValue |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of MaxTime should be 1");
    }
    updateMaxTime(partialResult[0].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateMaxTime(statistics.getEndTime());
  }

  // finalResult should be single column, like: | finalMaxTime |
  @Override
  public void setFinal(Column finalResult) {
    maxTime = finalResult.getLong(0);
  }

  // columnBuilder should be single in maxTimeAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeLong(maxTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(maxTime);
  }

  @Override
  public void reset() {
    this.maxTime = Long.MIN_VALUE;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.INT64;
  }

  private void updateMaxTime(long curTime) {
    maxTime = Math.max(maxTime, curTime);
  }
}
