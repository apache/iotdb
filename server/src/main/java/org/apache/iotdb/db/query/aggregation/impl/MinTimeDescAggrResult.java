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
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import java.io.IOException;

public class MinTimeDescAggrResult extends MinTimeAggrResult {

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    long time = statistics.getStartTime();
    setValue(time);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    while (batchIterator.hasNext() && batchIterator.currentTime() >= minBound) {
      setValue(batchIterator.currentTime());
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = length - 1; i >= 0; i--) {
      if (values[i] != null) {
        setLongValue(timestamps[i]);
        return;
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    for (int i = length - 1; i >= 0; i--) {
      if (values[i] != null) {
        setLongValue(timestamps[i]);
        return;
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }
}
