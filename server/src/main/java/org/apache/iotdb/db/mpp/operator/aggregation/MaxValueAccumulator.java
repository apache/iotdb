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
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class MaxValueAccumulator implements Accumulator {

  private TsPrimitiveType maxResult;
  private boolean hasCandidateResult;

  public MaxValueAccumulator(TSDataType seriesDataType) {
    this.maxResult = TsPrimitiveType.getByType(seriesDataType);
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
      updateResult((Comparable<Object>) column[1].getObject(i));
    }
  }

  // partialResult should be like: | partialMaxValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of MaxValue should be 1");
    }
    updateResult((Comparable<Object>) partialResult[0].getObject(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    Comparable<Object> maxValue = (Comparable<Object>) statistics.getMaxValue();
    updateResult(maxValue);
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    maxResult.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be single in countAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeObject(maxResult.getValue());
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeObject(maxResult.getValue());
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    this.maxResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {maxResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return maxResult.getDataType();
  }

  private void updateResult(Comparable<Object> minVal) {
    if (minVal == null) {
      return;
    }
    if (!hasCandidateResult || minVal.compareTo(maxResult.getValue()) > 0) {
      hasCandidateResult = true;
      maxResult.setObject(minVal);
    }
  }
}
