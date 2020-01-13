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

import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;

public class AvgAggrFunc extends AggregateResult {

  protected double sum = 0.0;
  private int cnt = 0;
  private TSDataType seriesDataType;
  private static final String AVG_AGGR_NAME = "AVG";

  public AvgAggrFunc(TSDataType seriesDataType) {
    super(TSDataType.DOUBLE);
    this.seriesDataType = seriesDataType;
  }

  @Override
  public void init() {
    resultData.reset();
    sum = 0.0;
    cnt = 0;
  }

  @Override
  public AggreResultData getResult() {
    if (cnt > 0) {
      resultData.setTimestamp(0);
      resultData.setDoubleRet(sum / cnt);
    }
    return resultData;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    sum += statistics.getSumValue();
    cnt += statistics.getCount();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage)
      throws IOException {
    updateResultFromPageData(dataInThisPage, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) throws IOException {
    while (dataInThisPage.hasCurrent()) {
      if (dataInThisPage.currentTime() >= bound) {
        break;
      }
      updateMean(seriesDataType, dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  private void updateMean(TSDataType type, Object sumVal) throws IOException {
    switch (type) {
      case INT32:
        sum += (int) sumVal;
        break;
      case INT64:
        sum += (long) sumVal;
        break;
      case FLOAT:
        sum += (float) sumVal;
        break;
      case DOUBLE:
        sum += (double) sumVal;
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new IOException(
            String
                .format("Unsupported data type in aggregation %s : %s", getAggreTypeName(), type));
    }
    cnt++;
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        updateMean(seriesDataType, value);
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  /**
   * Return type name of aggregation
   */
  public String getAggreTypeName() {
    return AVG_AGGR_NAME;
  }
}
