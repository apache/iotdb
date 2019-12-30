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
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MinTimeAggrFunc extends AggregateFunction {

  public MinTimeAggrFunc() {
    super(TSDataType.INT64);
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
  public void calculateValueFromChunkMetaData(ChunkMetaData chunkMetaData) {
    if (resultData.isSetValue()) {
      return;
    }
    long time = chunkMetaData.getStartTime();
    resultData.putTimeAndValue(0, time);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage) throws IOException {
    if (resultData.isSetValue()) {
      return;
    }
    if (dataInThisPage.hasCurrent()) {
      resultData.setTimestamp(0);
      resultData.setLongRet(dataInThisPage.currentTime());
    }
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    if (resultData.isSetValue()) {
      return;
    }
    long time = pageHeader.getStartTime();
    resultData.putTimeAndValue(0, time);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader)
      throws IOException {
    if (resultData.isSetValue()) {
      return;
    }

    if (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        resultData.setTimestamp(0);
        resultData.setLongRet(dataInThisPage.currentTime());
      } else {
        resultData.setTimestamp(0);
        resultData.setLongRet(unsequenceReader.current().getTimestamp());
      }
      return;
    }

    if (dataInThisPage.hasCurrent()) {
      resultData.setTimestamp(0);
      resultData.setLongRet(dataInThisPage.currentTime());
    }
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) throws IOException {
    if (resultData.isSetValue()) {
      return;
    }

    if (dataInThisPage.hasCurrent() && unsequenceReader.hasNext()) {
      if (dataInThisPage.currentTime() < unsequenceReader.current().getTimestamp()) {
        if (dataInThisPage.currentTime() >= bound) {
          return;
        }
        resultData.setTimestamp(0);
        resultData.setLongRet(dataInThisPage.currentTime());
      } else {
        if (unsequenceReader.current().getTimestamp() >= bound) {
          return;
        }
        resultData.setTimestamp(0);
        resultData.setLongRet(unsequenceReader.current().getTimestamp());
      }
      return;
    }

    if (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      resultData.setTimestamp(0);
      resultData.setLongRet(dataInThisPage.currentTime());
      dataInThisPage.next();
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    if (resultData.isSetValue()) {
      return;
    }
    if (unsequenceReader.hasNext()) {
      resultData.setTimestamp(0);
      resultData.setLongRet(unsequenceReader.current().getTimestamp());
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    if (resultData.isSetValue()) {
      return;
    }
    if (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      resultData.setTimestamp(0);
      resultData.setLongRet(unsequenceReader.current().getTimestamp());
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (resultData.isSetValue()) {
      return;
    }
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        resultData.setTimestamp(0);
        resultData.setLongRet(timestamps[i]);
        return;
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return resultData.isSetValue();
  }

}
