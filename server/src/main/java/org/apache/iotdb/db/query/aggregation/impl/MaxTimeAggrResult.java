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
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MaxTimeAggrResult extends AggregateResult {

  public MaxTimeAggrResult() {
    super(TSDataType.INT64);
    reset();
  }

  @Override
  public Long getResult() {
    return hasResult() ? getLongRet() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    long maxTimestamp = statistics.getEndTime();
    updateMaxTimeResult(maxTimestamp);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    int maxIndex = dataInThisPage.length() - 1;
    if (maxIndex < 0) {
      return;
    }
    long time = dataInThisPage.getTimeByIndex(maxIndex);
    updateMaxTimeResult(time);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) {
    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      updateMaxTimeResult(dataInThisPage.currentTime());
      dataInThisPage.next();
    }
  }

  //TODO Consider how to reverse order in dataReader(IReaderByTimeStamp)
  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    long time = -1;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
      }
    }

    if (time == -1) {
      return;
    }
    updateMaxTimeResult(time);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  private void updateMaxTimeResult(long value) {
    if (!hasResult() || value >= getLongRet()) {
      setLongRet(value);
    }
  }
}
