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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class LastValueAggrFunc extends AggregateResult {

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
  public void updateResultFromStatistics(Statistics statistics) throws QueryProcessException {
    Object lastVal = statistics.getLastValue();
    updateLastResult(statistics.getEndTime(), lastVal);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) throws IOException {
    updateResultFromPageData(dataInThisPage, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) throws IOException {
    long time = -1;
    Object lastVal = null;
    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      time = dataInThisPage.currentTime();
      lastVal = dataInThisPage.currentValue();
      dataInThisPage.next();
    }

    if (time != -1) {
      updateLastResult(time, lastVal);
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
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
