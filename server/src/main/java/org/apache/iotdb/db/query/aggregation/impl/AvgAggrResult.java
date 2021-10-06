/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.BooleanStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class AvgAggrResult extends AggregateResult {

  private TSDataType seriesDataType;
  private double avg = 0.0;
  private long cnt = 0;

  public AvgAggrResult(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE, AggregationType.AVG);
    this.seriesDataType = seriesDataType;
    reset();
    avg = 0.0;
    cnt = 0;
  }

  @Override
  protected boolean hasCandidateResult() {
    return cnt > 0;
  }

  @Override
  public Double getResult() {
    if (cnt > 0) {
      setDoubleValue(avg);
    }
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    long preCnt = cnt;
    if (statistics.getType().equals(TSDataType.BOOLEAN)) {
      throw new StatisticsClassException("Boolean statistics does not support: avg");
    }
    if (statistics.getType().equals(TSDataType.TEXT)) {
      throw new StatisticsClassException("Binary statistics does not support: avg");
    }
    cnt += statistics.getCount();
    double sum;
    if (statistics instanceof IntegerStatistics || statistics instanceof BooleanStatistics) {
      sum = statistics.getSumLongValue();
    } else {
      sum = statistics.getSumDoubleValue();
    }
    avg = (avg * preCnt + sum) / cnt;
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    while (batchIterator.hasNext()) {
      if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
        break;
      }
      updateAvg(seriesDataType, batchIterator.currentValue());
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateAvg(seriesDataType, values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateAvg(seriesDataType, values[i]);
      }
    }
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
    avg = (avg * cnt + val) / (cnt + 1);
    cnt++;
  }

  public void setAvgResult(TSDataType type, Object val) throws UnSupportedDataTypeException {
    cnt = 1;
    switch (type) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        avg = (double) val;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", type));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    AvgAggrResult anotherAvg = (AvgAggrResult) another;
    if (anotherAvg.cnt == 0) {
      // avoid two empty results producing an NaN
      return;
    }
    avg = (avg * cnt + anotherAvg.avg * anotherAvg.cnt) / (cnt + anotherAvg.cnt);
    cnt += anotherAvg.cnt;
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.avg = buffer.getDouble();
    this.cnt = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(avg, outputStream);
    ReadWriteIOUtils.write(cnt, outputStream);
  }

  public long getCnt() {
    return cnt;
  }

  @Override
  public void reset() {
    super.reset();
    cnt = 0;
    avg = 0;
  }
}
