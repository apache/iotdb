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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;

import static com.google.common.base.Preconditions.checkArgument;

public class MaxTimeAccumulator implements Accumulator {

  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;

  public MaxTimeAccumulator() {}

  // Column should be like: | Time | Value |
  // Value is used to judge isNull()
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateMaxTime(column[0].getLong(i));
      }
    }
  }

  // partialResult should be like: | partialMaxTimeValue |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MaxTime should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    updateMaxTime(partialResult[0].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    updateMaxTime(statistics.getEndTime());
  }

  // finalResult should be single column, like: | finalMaxTime |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    maxTime = finalResult.getLong(0);
  }

  // columnBuilder should be single in maxTimeAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MaxTime should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
    } else {
      columnBuilders[0].writeLong(maxTime);
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeLong(maxTime);
    }
  }

  @Override
  public void reset() {
    initResult = false;
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

  protected void updateMaxTime(long curTime) {
    initResult = true;
    maxTime = Math.max(maxTime, curTime);
  }
}
