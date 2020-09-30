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
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MaxTimeDescAggrResult extends MaxTimeAggrResult {

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasFinalResult()) {
      return;
    }
    super.updateResultFromStatistics(statistics);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound) {
    if (hasFinalResult()) {
      return;
    }
    if (dataInThisPage.hasCurrent()
        && dataInThisPage.currentTime() < maxBound
        && dataInThisPage.currentTime() >= minBound) {
      updateMaxTimeResult(dataInThisPage.currentTime());
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (hasFinalResult()) {
      return;
    }
    long time = -1;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
        break;
      }
    }

    if (time != -1) {
      updateMaxTimeResult(time);
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
