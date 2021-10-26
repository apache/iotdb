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

import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import java.io.IOException;

public class LastValueDescAggrResult extends LastValueAggrResult {

  public LastValueDescAggrResult(TSDataType dataType) {
    super(dataType);
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasFinalResult()) {
      return;
    }
    Object lastVal = statistics.getLastValue();
    setValue(lastVal);
    timestamp = statistics.getEndTime();
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    if (hasFinalResult()) {
      return;
    }
    long time = Long.MIN_VALUE;
    Object lastVal = null;
    if (batchIterator.hasNext()
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
        timestamp = timestamps[i];
        setValue(values[i]);
        return;
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public boolean isAscending() {
    return false;
  }
}
