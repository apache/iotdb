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
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class FirstValueAccumulator implements Accumulator {

  protected boolean hasCandidateResult;
  protected TsPrimitiveType firstValue;
  protected long minTime = Long.MAX_VALUE;

  public FirstValueAccumulator(TSDataType seriesDataType) {
    firstValue = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    long curTime = column[0].getLong(0);
    if (curTime < timeRange.getMax() && curTime >= timeRange.getMin()) {
      updateFirstValue(column[1].getObject(0), curTime);
    }
  }

  // partialResult should be like: | FirstValue | MinTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 2) {
      throw new IllegalArgumentException("partialResult of FirstValue should be 2");
    }
    updateFirstValue(partialResult[0].getObject(0), partialResult[1].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateFirstValue(statistics.getFirstValue(), statistics.getStartTime());
  }

  // finalResult should be single column, like: | finalFirstValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    firstValue.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be double in FirstValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeObject(firstValue.getValue());
    columnBuilders[1].writeLong(minTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeObject(firstValue.getValue());
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    this.minTime = Long.MAX_VALUE;
    this.firstValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {firstValue.getDataType(), TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return firstValue.getDataType();
  }

  protected void updateFirstValue(Object value, long curTime) {
    hasCandidateResult = true;
    if (curTime < minTime) {
      minTime = curTime;
      firstValue.setObject(value);
    }
  }
}
