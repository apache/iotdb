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

public class MinValueAccumulator implements Accumulator {

  private TsPrimitiveType minResult;
  private boolean hasCandidateResult = false;

  public MinValueAccumulator(TSDataType seriesDataType) {
    this.minResult = TsPrimitiveType.getByType(seriesDataType);
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

  // partialResult should be like: | partialMinValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 1) {
      throw new IllegalArgumentException("partialResult of MinValue should be 1");
    }
    updateResult((Comparable<Object>) partialResult[0].getObject(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    updateResult(minVal);
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    minResult.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be single in MinValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeObject(minResult.getValue());
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeObject(minResult.getValue());
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    this.minResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {minResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return minResult.getDataType();
  }

  private void updateResult(Comparable<Object> minVal) {
    if (minVal == null) {
      return;
    }
    if (!hasCandidateResult || minVal.compareTo(minResult.getValue()) < 0) {
      hasCandidateResult = true;
      minResult.setObject(minVal);
    }
  }
}
