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
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class AvgAggrResult extends SumAggrResult {

  private int cnt = 0;

  public AvgAggrResult() {
    super(TSDataType.DOUBLE);
    reset();
    cnt = 0;
  }

  @Override
  public Double getResult() {
    if (cnt > 0) {
      setDoubleRet(sum / cnt);
    }
    return getDoubleRet();
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    super.updateResultFromStatistics(statistics);
    cnt += statistics.getCount();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) throws IOException {
    super.updateResultFromPageData(dataInThisPage, bound);
    while (dataInThisPage.hasCurrent()) {
      if (dataInThisPage.currentTime() >= bound) {
        break;
      }
      cnt++;
      dataInThisPage.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    super.updateResultUsingTimestamps(timestamps, length, dataReader);
    for (int i = 0; i < length; i++) {
      if (dataReader.getValueInTimestamp(timestamps[i]) != null) {
        cnt++;
      }
    }
  }
}
