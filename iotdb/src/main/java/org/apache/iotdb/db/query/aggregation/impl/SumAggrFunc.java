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
import java.util.List;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.AggregationConstant;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class SumAggrFunc extends AggregateFunction {

  private double sum = 0.0;
  private TSDataType seriesDataType;

  public SumAggrFunc(TSDataType seriesDataType) {
    super(AggregationConstant.SUM, TSDataType.DOUBLE);
    this.seriesDataType = seriesDataType;
  }

  @Override
  public void init() {

  }

  @Override
  public BatchData getResult() {
    resultData.putDouble(sum);
    resultData.putTime(0);
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException {
    sum += pageHeader.getStatistics().getSum();
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      Object sumVal = null;
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        sumVal = dataInThisPage.currentValue();
        dataInThisPage.next();
      } else if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        sumVal = unsequenceReader.current().getValue().getValue();
        dataInThisPage.next();
        unsequenceReader.next();
      } else {
        sumVal = unsequenceReader.current().getValue().getValue();
        unsequenceReader.next();
      }
      updateSum(seriesDataType, sumVal);
    }

    while (dataInThisPage.hasNext()) {
      updateSum(seriesDataType, dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  private void updateSum(TSDataType type, Object sumVal) throws ProcessorException {
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
        throw new ProcessorException("Unsupported data type in aggregation MEAN : " + type);
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    while (unsequenceReader.hasNext()) {
      TimeValuePair pair = unsequenceReader.next();
      updateSum(seriesDataType, pair.getValue().getValue());
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(List<Long> timestamps,
      EngineReaderByTimeStamp dataReader) throws IOException, ProcessorException {
    for (long time : timestamps) {
      TsPrimitiveType value = dataReader.getValueInTimestamp(time);
      if (value != null) {
        updateSum(seriesDataType, value.getValue());
      }
    }
  }

  @Override
  public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart,
      long intervalEnd, BatchData data) throws ProcessorException {

  }
}
