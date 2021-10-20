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

public class MaxValueAggrResult extends AggregateResult {

  public MaxValueAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.MAX_VALUE);
    reset();
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    updateResult(maxVal);
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    Comparable<Object> maxVal = null;

    while (batchIterator.hasNext()
        && batchIterator.currentTime() < maxBound
        && batchIterator.currentTime() >= minBound) {
      if (maxVal == null || maxVal.compareTo(batchIterator.currentValue()) < 0) {
        maxVal = (Comparable<Object>) batchIterator.currentValue();
      }
      batchIterator.next();
    }
    updateResult(maxVal);
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Comparable<Object> maxVal = null;
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null && (maxVal == null || maxVal.compareTo(values[i]) < 0)) {
        maxVal = (Comparable<Object>) values[i];
      }
    }
    updateResult(maxVal);
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    Comparable<Object> maxVal = null;
    for (int i = 0; i < length; i++) {
      if (values[i] != null && (maxVal == null || maxVal.compareTo(values[i]) < 0)) {
        maxVal = (Comparable<Object>) values[i];
      }
    }
    updateResult(maxVal);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void merge(AggregateResult another) {
    this.updateResult((Comparable<Object>) another.getResult());
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {}

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) {}

  private void updateResult(Comparable<Object> maxVal) {
    if (maxVal == null) {
      return;
    }
    if (!hasCandidateResult() || maxVal.compareTo(getValue()) > 0) {
      setValue(maxVal);
    }
  }
}
