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
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class FirstValueAggrResult extends AggregateResult {

  public FirstValueAggrResult(TSDataType dataType) {
    super(dataType);
    reset();
  }

  @Override
  public Object getResult() {
    return getValue();
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics)
      throws QueryProcessException {
    if (hasResult()) {
      return;
    }

    Object firstVal = statistics.getFirstValue();
    if (firstVal == null) {
      throw new QueryProcessException("ChunkMetaData contains no FIRST value");
    }
    setValue(firstVal);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    if (hasResult()) {
      return;
    }
    if (dataInThisPage.hasCurrent()) {
      setValue(dataInThisPage.currentValue());
    }
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long bound) {
    if (hasResult()) {
      return;
    }
    if (dataInThisPage.hasCurrent() && dataInThisPage.currentTime() < bound) {
      setValue(dataInThisPage.currentValue());
      dataInThisPage.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    if (hasResult()) {
      return;
    }

    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        setValue(value);
        break;
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult();
  }
}
