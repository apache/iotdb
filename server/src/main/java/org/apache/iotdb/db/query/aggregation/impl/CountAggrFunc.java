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
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CountAggrFunc extends AggregateFunction {

  private static final Logger logger = LoggerFactory.getLogger(CountAggrFunc.class);

  public CountAggrFunc() {
    super(TSDataType.INT64);
  }

  @Override
  public void init() {
    resultData.reset();
    resultData.setTimestamp(0);
    resultData.setLongRet(0);
  }

  @Override
  public AggreResultData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    if (logger.isDebugEnabled()) {
      logger.debug("PageHeader>>>>>>>>>>>>num of rows:{}, minTimeStamp:{}, maxTimeStamp{}",
          pageHeader.getNumOfValues(), pageHeader.getStartTime(), pageHeader.getEndTime());
    }
    long preValue = resultData.getLongRet();
    preValue += pageHeader.getNumOfValues();
    resultData.setLongRet(preValue);

  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    calculateValueFromPageData(dataInThisPage, unsequenceReader, false, 0);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    calculateValueFromPageData(dataInThisPage, unsequenceReader, true, bound);
  }

  private void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      boolean hasBound, long bound) throws IOException {
    int cnt = 0;
    while (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      long minTimestamp = Math
          .min(dataInThisPage.currentTime(), unsequenceReader.current().getTimestamp());
      if (hasBound && minTimestamp >= bound) {
        break;
      }
      if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        dataInThisPage.next();
        unsequenceReader.next();
      } else if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        dataInThisPage.next();
      } else {
        unsequenceReader.next();
      }
      cnt++;
    }

    while (dataInThisPage.hasCurrent()) {
      if (hasBound && dataInThisPage.currentTime() >= bound) {
        break;
      }
      dataInThisPage.next();
      cnt++;
    }
    long preValue = resultData.getLongRet();
    preValue += cnt;
    resultData.setLongRet(preValue);
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    int cnt = 0;
    while (unsequenceReader.hasNext()) {
      unsequenceReader.next();
      cnt++;
    }
    long preValue = resultData.getLongRet();
    preValue += cnt;
    resultData.setLongRet(preValue);
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    int cnt = 0;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      unsequenceReader.next();
      cnt++;
    }
    long preValue = resultData.getLongRet();
    preValue += cnt;
    resultData.setLongRet(preValue);
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    int cnt = 0;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        cnt++;
      }
    }

    long preValue = resultData.getLongRet();
    preValue += cnt;
    resultData.setLongRet(preValue);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }
}
