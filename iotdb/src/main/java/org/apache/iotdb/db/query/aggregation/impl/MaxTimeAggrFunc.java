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

public class MaxTimeAggrFunc extends AggregateFunction {

  public MaxTimeAggrFunc() {
    super(AggregationConstant.MAX_TIME, TSDataType.INT64);
  }

  @Override
  public void init() {

  }

  @Override
  public BatchData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException {
    long maxTimestamp = pageHeader.getMaxTimestamp();

    //has not set value
    if (resultData.length() == 0) {
      resultData.putTime(0);
      resultData.putLong(maxTimestamp);
      return;
    }

    if (resultData.getLong() < maxTimestamp) {
      resultData.setLong(0, maxTimestamp);
    }
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    long time = -1;
    int maxIndex = dataInThisPage.length() - 1;
    if (maxIndex < 0) {
      return;
    }
    time = dataInThisPage.getTimeByIndex(maxIndex);
    if (resultData.length() == 0) {
      if (time != -1) {
        resultData.putTime(0);
        resultData.putAnObject(time);
      }
    } else {
      //has set value
      if (time != -1 && time > resultData.getLong()) {
        resultData.setAnObject(0, time);
      }
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException, ProcessorException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext()) {
      pair = unsequenceReader.next();
    }
    if (resultData.length() == 0) {
      if (pair != null) {
        resultData.putTime(0);
        resultData.putAnObject(pair.getTimestamp());
      }
    } else {
      //has set value
      if (pair != null && pair.getTimestamp() > resultData.getLong()) {
        resultData.setAnObject(0, pair.getTimestamp());
      }
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      pair = unsequenceReader.next();
    }
    if (resultData.length() == 0) {
      if (pair != null) {
        resultData.putTime(0);
        resultData.putAnObject(pair.getTimestamp());
      }
    } else {
      //has set value
      if (pair != null && pair.getTimestamp() > resultData.getLong()) {
        resultData.setAnObject(0, pair.getTimestamp());
      }
    }
  }

  //TODO Consider how to reverse order in dataReader(EngineReaderByTimeStamp)
  @Override
  public void calcAggregationUsingTimestamps(List<Long> timestamps,
      EngineReaderByTimeStamp dataReader) throws IOException, ProcessorException {
    long time = -1;
    for (int i = 0; i < timestamps.size(); i++) {
      TsPrimitiveType value = dataReader.getValueInTimestamp(timestamps.get(i));
      if (value != null) {
        time = timestamps.get(i);
      }
    }

    if (time == -1) {
      return;
    }

    if (resultData.length() == 0) {
      resultData.putTime(0);
      resultData.putLong(time);
    } else {
      if (resultData.getLong() < time) {
        resultData.setLong(0, time);
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  @Override
  public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart,
      long intervalEnd, BatchData data) throws ProcessorException {

  }
}
