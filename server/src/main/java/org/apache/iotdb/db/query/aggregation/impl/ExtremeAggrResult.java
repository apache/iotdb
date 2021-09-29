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
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ExtremeAggrResult extends AggregateResult {

  // timestamp of current value
  protected long timestamp = Long.MIN_VALUE;

  public ExtremeAggrResult(TSDataType dataType) {
    super(dataType, AggregationType.EXTREME);
    reset();
  }

  public Object getAbsValue(Object v) {

    switch (resultDataType) {
      case DOUBLE:
        return Math.abs((Double) v);
      case FLOAT:
        return Math.abs((Float) v);
      case INT32:
        return Math.abs((Integer) v);
      case INT64:
        return Math.abs((Long) v);
      default:
        throw new UnSupportedDataTypeException(String.valueOf(resultDataType));
    }
  }

  public Comparable<Object> getExtremeValue(
      Comparable<Object> extVal, Comparable<Object> currentValue) {
    if (currentValue != null) {
      Comparable<Object> absCurrentValue = (Comparable<Object>) getAbsValue(currentValue);
      if (extVal == null) {
        extVal = currentValue;
      } else {
        Comparable<Object> absExtVal = (Comparable<Object>) getAbsValue(extVal);
        if (absExtVal.compareTo(absCurrentValue) < 0
            || (absExtVal.compareTo(absCurrentValue) == 0 && extVal.compareTo(currentValue) < 0)) {
          extVal = currentValue;
        }
      }
    }
    return extVal;
  }

  @Override
  public Object getResult() {
    return hasCandidateResult() ? getValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();

    Comparable<Object> absMaxVal = (Comparable<Object>) getAbsValue(maxVal);
    Comparable<Object> absMinVal = (Comparable<Object>) getAbsValue(minVal);

    Comparable<Object> extVal = absMaxVal.compareTo(absMinVal) >= 0 ? maxVal : minVal;
    updateResult(extVal);
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    Comparable<Object> extVal = null;

    while (batchIterator.hasNext()
        && batchIterator.currentTime() < maxBound
        && batchIterator.currentTime() >= minBound) {

      extVal = getExtremeValue(extVal, (Comparable<Object>) batchIterator.currentValue());
      batchIterator.next();
    }
    updateResult(extVal);
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Comparable<Object> extVal = null;

    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      extVal = getExtremeValue(extVal, (Comparable<Object>) values[i]);
    }
    updateResult(extVal);
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, Object[] values) {
    Comparable<Object> extVal = null;
    for (int i = 0; i < length; i++) {
      extVal = getExtremeValue(extVal, (Comparable<Object>) values[i]);
    }
    updateResult(extVal);
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
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    timestamp = buffer.getLong();
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
  }

  private void updateResult(Comparable<Object> extVal) {
    if (extVal == null) {
      return;
    }

    Comparable<Object> absExtVal = (Comparable<Object>) getAbsValue(extVal);
    Comparable<Object> candidateResult = (Comparable<Object>) getValue();
    Comparable<Object> absCandidateResult = (Comparable<Object>) getAbsValue(getValue());

    if (!hasCandidateResult()
        || (absExtVal.compareTo(absCandidateResult) > 0
            || (absExtVal.compareTo(absCandidateResult) == 0
                && extVal.compareTo(candidateResult) > 0))) {
      setValue(extVal);
    }
  }
}
