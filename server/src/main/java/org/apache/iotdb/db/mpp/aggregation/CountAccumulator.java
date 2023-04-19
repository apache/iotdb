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

public class CountAccumulator implements Accumulator {

  private long countValue = 0;

  public CountAccumulator() {}

  // Column should be like: | Time | Value |

  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    int curPositionCount = column[0].getPositionCount();

    if (!column[1].mayHaveNull()
        && lastIndex == curPositionCount - 1
        && ((bitMap == null) || bitMap.isAllMarked())) {
      countValue += curPositionCount;
    } else {
      for (int i = 0; i <= lastIndex; i++) {
        if (bitMap != null && !bitMap.isMarked(i)) {
          continue;
        }
        if (!column[1].isNull(i)) {
          countValue++;
        }
      }
    }
  }

  // partialResult should be like: | partialCountValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Count should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    countValue += partialResult[0].getLong(0);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    countValue += statistics.getCount();
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    countValue = finalResult.getLong(0);
  }

  // columnBuilder should be single in countAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Count should be 1");
    columnBuilders[0].writeLong(countValue);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countValue);
  }

  @Override
  public void reset() {
    this.countValue = 0;
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
}
