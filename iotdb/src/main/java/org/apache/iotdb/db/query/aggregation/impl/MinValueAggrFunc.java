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
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.AggregationConstant;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MinValueAggrFunc extends AggregateFunction {

  public MinValueAggrFunc(TSDataType dataType) {
    super(AggregationConstant.MIN_VALUE, dataType);
  }

  @Override
  public void init() {
    resultData.reSet();
  }

  @Override
  public AggreResultData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException {
    Comparable<Object> minVal = (Comparable<Object>) pageHeader.getStatistics().getMin();
    updateResult(minVal);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    Comparable<Object> minVal = null;
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        if (minVal == null || minVal.compareTo(dataInThisPage.currentValue()) > 0) {
          minVal = (Comparable<Object>) dataInThisPage.currentValue();
        }
        dataInThisPage.next();
      } else if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        if (minVal == null
            || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
          minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
        }
        dataInThisPage.next();
        unsequenceReader.next();
      } else {
        if (minVal == null
            || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
          minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
        }
        unsequenceReader.next();
      }
    }

    while (dataInThisPage.hasNext()) {
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
      long bound) throws IOException, ProcessorException {
    Comparable<Object> minVal = null;
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        if (dataInThisPage.currentTime() >= bound) {
          break;
        }
        if (minVal == null || minVal.compareTo(dataInThisPage.currentValue()) > 0) {
          minVal = (Comparable<Object>) dataInThisPage.currentValue();
        }
        dataInThisPage.next();
      } else if (dataInThisPage.currentTime() == unsequenceReader.current().getTimestamp()) {
        if (dataInThisPage.currentTime() >= bound) {
          break;
        }
        if (minVal == null
            || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
          minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
        }
        dataInThisPage.next();
        unsequenceReader.next();
      } else {
        if (unsequenceReader.current().getTimestamp() >= bound) {
          break;
        }
        if (minVal == null
            || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
          minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
        }
        unsequenceReader.next();
      }
    }

    while (dataInThisPage.hasNext() && dataInThisPage.currentTime() < bound) {
      if (minVal == null
          || minVal.compareTo(dataInThisPage.currentValue()) > 0) {
        minVal = (Comparable<Object>) dataInThisPage.currentValue();
      }
      dataInThisPage.next();
    }
    updateResult(minVal);
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    Comparable<Object> minVal = null;
    while (unsequenceReader.hasNext()) {
      if (minVal == null
          || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
        minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
      }
      unsequenceReader.next();
    }
    updateResult(minVal);
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    Comparable<Object> minVal = null;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      if (minVal == null
          || minVal.compareTo(unsequenceReader.current().getValue().getValue()) > 0) {
        minVal = (Comparable<Object>) unsequenceReader.current().getValue().getValue();
      }
      unsequenceReader.next();
    }
    updateResult(minVal);
  }

  @Override
  public void calcAggregationUsingTimestamps(List<Long> timestamps,
      EngineReaderByTimeStamp dataReader) throws IOException {
    Comparable<Object> minVal = null;
    for (long time : timestamps) {
      TsPrimitiveType value = dataReader.getValueInTimestamp(time);
      if (value == null) {
        continue;
      }
      if (minVal == null || minVal.compareTo(value.getValue()) > 0) {
        minVal = (Comparable<Object>) value.getValue();
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
