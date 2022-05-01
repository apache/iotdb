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

public class LastValueAccumulator implements Accumulator {

  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;

  public LastValueAccumulator(TSDataType seriesDataType) {
    lastValue = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax()) {
        updateLastValue(column[1].getObject(i), curTime);
      }
    }
  }

  // partialResult should be like: | LastValue | MaxTime |
  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult.length != 2) {
      throw new IllegalArgumentException("partialResult of LastValue should be 2");
    }
    updateLastValue(partialResult[0].getObject(0), partialResult[1].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateLastValue(statistics.getLastValue(), statistics.getEndTime());
  }

  // finalResult should be single column, like: | finalLastValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    lastValue.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be double in LastValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    columnBuilders[0].writeObject(lastValue.getValue());
    columnBuilders[1].writeLong(maxTime);
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeObject(lastValue.getValue());
  }

  @Override
  public void reset() {
    this.maxTime = Long.MIN_VALUE;
    this.lastValue.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {lastValue.getDataType(), TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return lastValue.getDataType();
  }

  protected void updateLastValue(Object value, long curTime) {
    if (curTime > maxTime) {
      maxTime = curTime;
      lastValue.setObject(value);
    }
  }
}
