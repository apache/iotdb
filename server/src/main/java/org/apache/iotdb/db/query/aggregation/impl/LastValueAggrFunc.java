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
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class LastValueAggrFunc extends AggregateFunction {

  public LastValueAggrFunc(TSDataType dataType) {
    super(dataType);
  }

  @Override
  public void init() {
    resultData.reset();
  }

  @Override
  public AggreResultData getResult() {
    if (resultData.isSetTime()) {
      resultData.setTimestamp(0);
    }
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    Object lastVal = pageHeader.getStatistics().getLastValue();
    updateLastResult(pageHeader.getEndTime(), lastVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    calculateValueFromPageData(dataInThisPage, unsequenceReader, Long.MAX_VALUE);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    long time = -1;
    Object lastVal = null;
    while (dataInThisPage.hasNext() && dataInThisPage.currentTime() < bound) {
      time = dataInThisPage.currentTime();
      lastVal = dataInThisPage.currentValue();
      dataInThisPage.next();
    }

    while (unsequenceReader.hasNext()) {
      if (unsequenceReader.current().getTimestamp() < time) {
        unsequenceReader.next();
      } else if (unsequenceReader.current().getTimestamp() == time) {
        lastVal = unsequenceReader.current().getValue().getValue();
        unsequenceReader.next();
      } else {
        break;
      }
    }

    // has inited lastVal and time in the batch(dataInThisPage).
    if (time != -1) {
      updateLastResult(time, lastVal);
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext()) {
      pair = unsequenceReader.next();
    }

    if (pair != null) {
      updateLastResult(pair.getTimestamp(), pair.getValue().getValue());
    }

  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      pair = unsequenceReader.next();
    }

    if (pair != null) {
      updateLastResult(pair.getTimestamp(), pair.getValue().getValue());
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {

    long time = -1;
    Object lastVal = null;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
        lastVal = value;
      }
    }
    if (time != -1) {
      updateLastResult(time, lastVal);
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  private void updateLastResult(long time, Object value) {
    if (!resultData.isSetTime()) {
      resultData.putTimeAndValue(time, value);
    } else {
      if (time >= resultData.getTimestamp()) {
        resultData.putTimeAndValue(time, value);
      }
    }
  }

}
