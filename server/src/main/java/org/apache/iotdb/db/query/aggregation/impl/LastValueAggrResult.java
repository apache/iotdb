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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class LastValueAggrResult extends AggregateResult {

  // timestamp of current value
  protected long timestamp = Long.MIN_VALUE;

  public LastValueAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.LAST_VALUE);
    reset();
  }

  @Override
  public void reset() {
    super.reset();
    timestamp = Long.MIN_VALUE;
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    Object lastVal = statistics.getLastValue();
    setValue(lastVal);
    timestamp = statistics.getEndTime();
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    long time = Long.MIN_VALUE;
    Object lastVal = null;
    while (batchIterator.hasNext()
        && batchIterator.currentTime() < maxBound
        && batchIterator.currentTime() >= minBound) {
      time = batchIterator.currentTime();
      lastVal = batchIterator.currentValue();
      batchIterator.next();
    }

    if (time != Long.MIN_VALUE) {
      setValue(lastVal);
      timestamp = time;
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = length - 1; i >= 0; i--) {
      if (values[i] != null) {
        timestamp = timestamps[i];
        setValue(values[i]);
        return;
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    for (int i = length - 1; i >= 0; i--) {
      if (values[i] != null) {
        timestamp = timestamps[i];
        setValue(values[i]);
        return;
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
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
}
