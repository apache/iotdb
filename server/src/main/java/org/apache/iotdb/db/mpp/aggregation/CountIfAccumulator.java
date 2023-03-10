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

import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory.KeepEvaluator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;

import static com.google.common.base.Preconditions.checkArgument;

public class CountIfAccumulator implements Accumulator {

  // number of the point segment that satisfies the KEEP expression
  private long countValue = 0;

  // number of the continues data points satisfy IF expression
  private long keep;

  private final KeepEvaluator keepEvaluator;

  private final boolean ignoreNull;

  private boolean lastPointIsSatisfy;

  public CountIfAccumulator(KeepEvaluator keepEvaluator, boolean ignoreNull) {
    this.keepEvaluator = keepEvaluator;
    this.ignoreNull = ignoreNull;
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      // the input parameter 'bitMap' and 'lastIndex' effects on ControlColumn
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (column[1].isNull(i)) {
        if (!this.ignoreNull) {
          // data point segment was over, judge whether to count
          if (lastPointIsSatisfy && keepEvaluator.apply(keep)) {
            countValue++;
          }
          keep = 0;
          lastPointIsSatisfy = false;
        }
      } else {
        if (column[1].getBoolean(i)) {
          keep++;
          lastPointIsSatisfy = true;
        } else {
          // data point segment was over, judge whether to count
          if (lastPointIsSatisfy && keepEvaluator.apply(keep)) {
            countValue++;
          }
          keep = 0;
          lastPointIsSatisfy = false;
        }
      }
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of count_if should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    countValue += partialResult[0].getLong(0);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    countValue = finalResult.getLong(0);
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of count_if should be 1");
    // judge whether the last data point segment need to count
    if (lastPointIsSatisfy && keepEvaluator.apply(keep)) {
      countValue++;
    }
    columnBuilders[0].writeLong(countValue);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    // judge whether the last data point segment need to count
    if (lastPointIsSatisfy && keepEvaluator.apply(keep)) {
      countValue++;
    }
    columnBuilder.writeLong(countValue);
  }

  @Override
  public void reset() {
    this.countValue = 0;
    this.keep = 0;
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
