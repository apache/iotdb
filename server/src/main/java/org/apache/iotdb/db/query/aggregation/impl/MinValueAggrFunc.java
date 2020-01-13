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
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MinValueAggrFunc extends AggregateFunction {

  public MinValueAggrFunc(TSDataType dataType) {
    super(dataType);
  }

  @Override
  public void init() {
    resultData.reset();
  }

  @Override
  public AggreResultData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromStatistics(Statistics statistics)
      throws QueryProcessException {
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    updateResult(minVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage) throws IOException {
    calculateValueFromPageData(dataInThisPage, Long.MAX_VALUE);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, long bound) throws IOException {
    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      updateResult((Comparable<Object>) dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    while (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        updateResult((Comparable<Object>) dataInThisPage.currentValue());
        dataInThisPage.next();
      } else if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        updateResult((Comparable<Object>) unsequenceReader.current().getValue().getValue());
        dataInThisPage.next();
        unsequenceReader.next();
      } else {
        updateResult((Comparable<Object>) unsequenceReader.current().getValue().getValue());
        unsequenceReader.next();
      }
    }

    Comparable<Object> minVal = null;
    while (dataInThisPage.hasCurrent()) {
      if (minVal == null
          || minVal.compareTo(dataInThisPage.currentValue()) > 0) {
        minVal = (Comparable<Object>) dataInThisPage.currentValue();
      }
      dataInThisPage.next();
    }
    updateResult(minVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    while (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      long time = Math.min(dataInThisPage.currentTime(), unsequenceReader.current().getTimestamp());
      if (time >= bound) {
        break;
      }

      if (dataInThisPage.currentTime() == time) {
        updateResult((Comparable<Object>) dataInThisPage.currentValue());
        dataInThisPage.next();
      }

      if (unsequenceReader.current().getTimestamp() == time) {
        updateResult((Comparable<Object>) unsequenceReader.current().getValue().getValue());
        unsequenceReader.next();
      }

    }

    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      updateResult((Comparable<Object>) dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    Comparable<Object> minVal = null;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value == null) {
        continue;
      }
      if (minVal == null || minVal.compareTo(value) > 0) {
        minVal = (Comparable<Object>) value;
      }
    }
    updateResult(minVal);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  private void updateResult(Comparable<Object> minVal) {
    if (minVal == null) {
      return;
    }
    if (!resultData.isSetValue() || minVal.compareTo(resultData.getValue()) < 0) {
      resultData.putTimeAndValue(0, minVal);
    }
  }

}
