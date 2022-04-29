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

public class MinTimeAccumulator implements Accumulator {

  private boolean hasCandidateResult;
  private long minTime = Long.MAX_VALUE;

  public MinTimeAccumulator() {}

  // Column should be like: | Time |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    long curTime = column[0].getLong(0);
    if (curTime < timeRange.getMax() && curTime >= timeRange.getMin()) {
      updateMinTime(curTime);
    }
  }

  // partialResult should be like: | partialMinTimeValue |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of MinTime should be 1");
    }
    updateMinTime(partialResult[0].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateMinTime(statistics.getStartTime());
  }

  // finalResult should be single column, like: | finalMinTime |
  @Override
  public void setFinal(Column finalResult) {
    minTime = finalResult.getLong(0);
  }

  // columnBuilder should be single in minTimeAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeLong(minTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(minTime);
  }

  @Override
  public void reset() {
    this.minTime = Long.MAX_VALUE;
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.INT64;
  }

  private void updateMinTime(long curTime) {
    hasCandidateResult = true;
    minTime = Math.min(minTime, curTime);
  }
}
