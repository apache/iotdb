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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.DescSeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.DescBatchData;

public class MinTimeAggrResult extends AggregateResult {

  public MinTimeAggrResult() {
    super(TSDataType.INT64, AggregationType.MIN_TIME);
    reset();
  }

  @Override
  public Long getResult() {
    return hasResult() || isChanged ? getLongValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasResult()) {
      return;
    }
    long minTimestamp = statistics.getStartTime();
    updateMinTimeResult(minTimestamp);
    hasResult = false;
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    updateResultFromPageData(dataInThisPage, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound) {
    if (hasResult()) {
      return;
    }
    if (dataInThisPage instanceof DescBatchData || dataInThisPage.isFromDescMergeReader()) {
      while (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        updateMinTimeResult(dataInThisPage.currentTime());
        dataInThisPage.next();
      }
      hasResult = false;
    } else {
      if (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        updateMinTimeResult(dataInThisPage.currentTime());
      }
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (length == 0) {
      return;
    }
    long time = -1;
    Object value = null;
    int cnt = 0;
    while (value == null && cnt < length) {
      if (dataReader instanceof DescSeriesReaderByTimestamp) {
        time = timestamps[length - cnt - 1];
      } else {
        time = timestamps[cnt];
      }
      value = dataReader.getValueInTimestamp(time);
      cnt++;
    }
    if (value != null) {
      updateMinTimeResult(time);
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult();
  }

  @Override
  public void merge(AggregateResult another) {
    MinTimeAggrResult anotherMinTime = (MinTimeAggrResult) another;
    if (anotherMinTime.getResult() != null) {
      updateMinTimeResult(anotherMinTime.getResult());
    }
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
  }

  private void updateMinTimeResult(long value) {
    if (!isChanged || value <= getLongValue()) {
      setLongValue(value);
      isChanged = true;
    }
  }
}
