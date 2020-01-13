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
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MaxValueAggrFunc extends AggregateFunction {

  public MaxValueAggrFunc(TSDataType dataType) {
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
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    Comparable<Object> maxVal = (Comparable<Object>) pageHeader.getStatistics().getMaxValue();
    updateResult(maxVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    Comparable<Object> maxVal = null;
    Object tmpVal = null;
    while (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        tmpVal = dataInThisPage.currentValue();
        dataInThisPage.next();
      } else if (dataInThisPage.currentTime() > unsequenceReader.current().getTimestamp()) {
        tmpVal = unsequenceReader.current().getValue().getValue();
        unsequenceReader.next();
      } else {
        tmpVal = unsequenceReader.current().getValue().getValue();
        dataInThisPage.next();
        unsequenceReader.next();
      }

      if (maxVal == null || maxVal.compareTo(tmpVal) < 0) {
        maxVal = (Comparable<Object>) tmpVal;
      }
    }

    while (dataInThisPage.hasCurrent()) {
      if (maxVal == null || maxVal.compareTo(dataInThisPage.currentValue()) < 0) {
        maxVal = (Comparable<Object>) dataInThisPage.currentValue();
      }
      dataInThisPage.next();
    }
    updateResult(maxVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    Object tmpVal = null;
    while (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      long time = Math.min(dataInThisPage.currentTime(), unsequenceReader.current().getTimestamp());
      if (time >= bound) {
        break;
      }

      if (dataInThisPage.currentTime() == time) {
        tmpVal = dataInThisPage.currentValue();
        dataInThisPage.next();
      }

      if (unsequenceReader.current().getTimestamp() == time) {
        tmpVal = unsequenceReader.current().getValue().getValue();
        unsequenceReader.next();
      }
      updateResult((Comparable<Object>) tmpVal);
    }

    while (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      updateResult((Comparable<Object>) dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    Comparable<Object> maxVal = null;
    while (unsequenceReader.hasNext()) {
      if (maxVal == null
          || maxVal.compareTo(unsequenceReader.current().getValue().getValue()) < 0) {
        maxVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
      }
      unsequenceReader.next();
    }
    updateResult(maxVal);
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    Comparable<Object> maxVal = null;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      if (maxVal == null
          || maxVal.compareTo(unsequenceReader.current().getValue().getValue()) < 0) {
        maxVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
      }
      unsequenceReader.next();
    }
    updateResult(maxVal);
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    Comparable<Object> maxVal = null;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value == null) {
        continue;
      }
      if (maxVal == null || maxVal.compareTo(value) < 0) {
        maxVal = (Comparable<Object>) value;
      }
    }
    updateResult(maxVal);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  private void updateResult(Comparable<Object> maxVal) {
    if (maxVal == null) {
      return;
    }
    if (!resultData.isSetValue() || maxVal.compareTo(resultData.getValue()) > 0) {
      resultData.putTimeAndValue(0, maxVal);
    }
  }
}
