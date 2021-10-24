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

public class SumAggrResult extends AggregateResult {

  private TSDataType seriesDataType;

  public SumAggrResult(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE, AggregationType.SUM);
    this.seriesDataType = seriesDataType;
    reset();
    setDoubleValue(0.0);
  }

  @Override
  public Double getResult() {
    return getDoubleValue();
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    double preValue = getDoubleValue();
    if (statistics instanceof IntegerStatistics || statistics instanceof BooleanStatistics) {
      preValue += statistics.getSumLongValue();
    } else {
      preValue += statistics.getSumDoubleValue();
    }
    setDoubleValue(preValue);
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
      updateSum(batchIterator.currentValue());
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateSum(values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateSum(values[i]);
      }
    }
  }

  private void updateSum(Object sumVal) throws UnSupportedDataTypeException {
    double preValue = getDoubleValue();
    switch (seriesDataType) {
      case INT32:
        preValue += (int) sumVal;
        break;
      case INT64:
        preValue += (long) sumVal;
        break;
      case FLOAT:
        preValue += (float) sumVal;
        break;
      case DOUBLE:
        preValue += (double) sumVal;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation SUM : %s", seriesDataType));
    }
    setDoubleValue(preValue);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    SumAggrResult anotherSum = (SumAggrResult) another;
    setDoubleValue(getDoubleValue() + anotherSum.getDoubleValue());
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    seriesDataType = TSDataType.deserialize(buffer.get());
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
  }
}
