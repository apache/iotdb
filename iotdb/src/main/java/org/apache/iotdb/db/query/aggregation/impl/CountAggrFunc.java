/**
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
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.AggregationConstant;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class CountAggrFunc extends AggregateFunction {

  public CountAggrFunc() {
    super(AggregationConstant.COUNT, TSDataType.INT64);
  }

  @Override
  public void init() {
    if (resultData.length() == 0) {
      resultData.putTime(0);
      resultData.putLong(0);
    }
  }

  @Override
  public BatchData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    System.out.println("PageHeader>>>>>>>>>>>>" + pageHeader.getNumOfValues() + " " + pageHeader
        .getMinTimestamp()
        + "," + pageHeader.getMaxTimestamp());
    long preValue = resultData.getLong();
    preValue += pageHeader.getNumOfValues();
    resultData.setLong(0, preValue);

  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        dataInThisPage.next();
        unsequenceReader.next();
      } else if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        dataInThisPage.next();
      } else {
        unsequenceReader.next();
      }
      long preValue = resultData.getLong();
      preValue += 1;
      resultData.setLong(0, preValue);
    }

    if (dataInThisPage.hasNext()) {
      long preValue = resultData.getLong();
      preValue += (resultData.length() - resultData.getCurIdx());
      resultData.setLong(0, preValue);
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    int cnt = 0;
    while (unsequenceReader.hasNext()) {
      unsequenceReader.next();
      cnt++;
    }
    long preValue = resultData.getLong();
    preValue += cnt;
    resultData.setLong(0, preValue);
  }

  @Override
  public boolean calcAggregationUsingTimestamps(EngineTimeGenerator timeGenerator,
      EngineReaderByTimeStamp sequenceReader, EngineReaderByTimeStamp unsequenceReader)
      throws IOException, ProcessorException {
    return false;
  }

  @Override
  public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart,
      long intervalEnd, BatchData data) throws ProcessorException {

  }
}
