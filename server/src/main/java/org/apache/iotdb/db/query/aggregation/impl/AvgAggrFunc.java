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
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class AvgAggrFunc extends AggregateFunction {

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
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    sum += pageHeader.getStatistics().getSumValue();
    cnt += pageHeader.getNumOfValues();
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
    while (dataInThisPage.hasNext() && unsequenceReader.hasNext()) {
      Object sumVal = null;
      long time = Math.min(dataInThisPage.currentTime(), unsequenceReader.current().getTimestamp());
      if (hasBound && time >= bound) {
        break;
      }
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
      updateMean(seriesDataType, sumVal);
    }

    while (dataInThisPage.hasNext()) {
      if (hasBound && dataInThisPage.currentTime() >= bound) {
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
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    while (unsequenceReader.hasNext()) {
      TimeValuePair pair = unsequenceReader.next();
      updateMean(seriesDataType, pair.getValue().getValue());
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      TimeValuePair pair = unsequenceReader.next();
      updateMean(seriesDataType, pair.getValue().getValue());
    }
  }

  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
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

  @Override
  public void calculateValueFromChunkData(ChunkMetaData chunkMetaData) {
    sum += chunkMetaData.getStatistics().getSumValue();
    cnt += chunkMetaData.getNumOfPoints();
  }

  /**
   * Return type name of aggregation
   */
  public String getAggreTypeName() {
    return AVG_AGGR_NAME;
  }
}
