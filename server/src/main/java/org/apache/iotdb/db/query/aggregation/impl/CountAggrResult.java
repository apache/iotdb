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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CountAggrResult extends AggregateResult {

  public CountAggrResult() {
    super(TSDataType.INT64, AggregationType.COUNT);
    reset();
    setLongValue(0);
  }

  @Override
  public Long getResult() {
    return getLongValue();
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    setLongValue(getLongValue() + statistics.getCount());
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    setLongValue(getLongValue() + batchIterator.totalLength());
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    int cnt = 0;
    while (batchIterator.hasNext()) {
      if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
        break;
      }
      cnt++;
      batchIterator.next();
    }
    setLongValue(getLongValue() + cnt);
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    int cnt = 0;
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        cnt++;
      }
    }
    setLongValue(getLongValue() + cnt);
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    int cnt = 0;
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        cnt++;
      }
    }
    setLongValue(getLongValue() + cnt);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    CountAggrResult anotherCount = (CountAggrResult) another;
    setLongValue(anotherCount.getResult() + this.getResult());
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {}

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) {}
}
