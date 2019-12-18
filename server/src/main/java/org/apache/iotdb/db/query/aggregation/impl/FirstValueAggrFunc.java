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
import org.apache.iotdb.db.query.reader.IPointReaderByTimestamp;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class FirstValueAggrFunc extends AggregateFunction {

  public FirstValueAggrFunc(TSDataType dataType) {
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
  public void calculateValueFromPageHeader(PageHeader pageHeader) throws QueryProcessException {
    if (resultData.isSetTime()) {
      return;
    }

    Object firstVal = pageHeader.getStatistics().getFirstValue();
    if (firstVal == null) {
      throw new QueryProcessException("PageHeader contains no FIRST value");
    }
    resultData.putTimeAndValue(0, firstVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    if (resultData.isSetTime()) {
      return;
    }
    if (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() >= unsequenceReader.current().getTimestamp()) {
        resultData.putTimeAndValue(0, unsequenceReader.current().getValue().getValue());
        unsequenceReader.next();
        return;
      } else {
        resultData.putTimeAndValue(0, dataInThisPage.currentValue());
        dataInThisPage.next();
        return;
      }
    }

    if (dataInThisPage.hasCurrent()) {
      resultData.putTimeAndValue(0, dataInThisPage.currentValue());
    }
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    if (resultData.isSetTime()) {
      return;
    }
    if (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() >= unsequenceReader.current().getTimestamp()) {
        if (unsequenceReader.current().getTimestamp() < bound) {
          resultData.putTimeAndValue(0, unsequenceReader.current().getValue().getValue());
          unsequenceReader.next();
          return;
        }
      } else {
        if (dataInThisPage.currentTime() < bound) {
          resultData.putTimeAndValue(0, dataInThisPage.currentValue());
          dataInThisPage.next();
          return;
        }
      }
    }

    if (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      resultData.putTimeAndValue(0, dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    if (resultData.isSetTime()) {
      return;
    }
    if (unsequenceReader.hasNext()) {
      resultData.putTimeAndValue(0, unsequenceReader.current().getValue().getValue());
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    if (resultData.isSetTime()) {
      return;
    }
    if (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      resultData.putTimeAndValue(0, unsequenceReader.current().getValue().getValue());
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IPointReaderByTimestamp dataReader) throws IOException {
    if (resultData.isSetTime()) {
      return;
    }

    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        resultData.putTimeAndValue(0, value);
        break;
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return resultData.isSetTime();
  }
}
