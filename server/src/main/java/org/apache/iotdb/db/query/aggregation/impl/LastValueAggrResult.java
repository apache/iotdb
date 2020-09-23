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
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.DescBatchData;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class LastValueAggrResult extends AggregateResult {

  //timestamp of current value
  private long timestamp = Long.MIN_VALUE;

  public LastValueAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.LAST_VALUE);
    reset();
    this.needAscReader = false;
  }

  @Override
  public void reset() {
    super.reset();
    timestamp = Long.MIN_VALUE;
  }

  @Override
  public Object getResult() {
    return hasResult() || isChanged ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics, boolean ascending) {
    if (hasResult()) {
      return;
    }
    updateLastValueResult(statistics.getEndTime(), statistics.getLastValue());
    if (ascending) {
      hasResult = false;
    }
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
      if (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        updateLastValueResult(dataInThisPage.currentTime(), dataInThisPage.currentValue());
      }
    } else {
      while (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        updateLastValueResult(dataInThisPage.currentTime(), dataInThisPage.currentValue());
        dataInThisPage.next();
      }
      hasResult = false;
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    long time;
    Object lastVal;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
        lastVal = value;
        updateLastValueResult(time, lastVal);
      }
    }
    hasResult = false;
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult();
  }

  @Override
  public void merge(AggregateResult another) {
    LastValueAggrResult anotherLast = (LastValueAggrResult) another;
    if (this.getValue() == null || this.timestamp < anotherLast.timestamp) {
      this.setValue(anotherLast.getValue());
      this.timestamp = anotherLast.timestamp;
    }
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    timestamp = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
  }

  private void updateLastValueResult(long newTime, Object newValue) {
    if (!isChanged || newTime >= timestamp) {
      timestamp = newTime;
      setValue(newValue);
      isChanged = true;
    }
  }
}
