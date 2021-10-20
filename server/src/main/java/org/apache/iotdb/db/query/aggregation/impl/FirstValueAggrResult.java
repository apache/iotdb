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

public class FirstValueAggrResult extends AggregateResult {

  // timestamp of current value
  protected long timestamp = Long.MAX_VALUE;

  public FirstValueAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.FIRST_VALUE);
    reset();
  }

  @Override
  public void reset() {
    super.reset();
    timestamp = Long.MAX_VALUE;
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasFinalResult()) {
      return;
    }

    Object firstVal = statistics.getFirstValue();
    setValue(firstVal);
    timestamp = statistics.getStartTime();
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    if (hasFinalResult()) {
      return;
    }
    if (batchIterator.hasNext()) {
      setValue(batchIterator.currentValue());
      timestamp = batchIterator.currentTime();
    }
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    if (hasFinalResult()) {
      return;
    }
    if (batchIterator.hasNext()
        && batchIterator.currentTime() < maxBound
        && batchIterator.currentTime() >= minBound) {
      setValue(batchIterator.currentValue());
      timestamp = batchIterator.currentTime();
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    if (hasFinalResult()) {
      return;
    }
    int currentPos = 0;
    long[] timesForFirstValue = new long[TIME_LENGTH_FOR_FIRST_VALUE];
    while (currentPos < length) {
      int timeLength = Math.min(length - currentPos, TIME_LENGTH_FOR_FIRST_VALUE);
      System.arraycopy(timestamps, currentPos, timesForFirstValue, 0, timeLength);
      Object[] values = dataReader.getValuesInTimestamps(timesForFirstValue, timeLength);
      for (int i = 0; i < timeLength; i++) {
        if (values[i] != null) {
          setValue(values[i]);
          timestamp = timesForFirstValue[i];
          return;
        }
      }
      currentPos += timeLength;
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    if (hasFinalResult()) {
      return;
    }
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        setValue(values[i]);
        timestamp = timestamps[i];
        break;
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public void merge(AggregateResult another) {
    FirstValueAggrResult anotherFirst = (FirstValueAggrResult) another;
    if (this.getValue() == null || this.timestamp > anotherFirst.timestamp) {
      setValue(anotherFirst.getValue());
      timestamp = anotherFirst.timestamp;
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
