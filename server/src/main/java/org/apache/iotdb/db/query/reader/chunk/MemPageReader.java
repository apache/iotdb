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
package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

public class MemPageReader implements IPageReader {

  private BatchData batchData;
  private Statistics statistics;
  private Filter valueFilter;

  public MemPageReader(BatchData batchData, Statistics statistics) {
    this.batchData = batchData;
    this.statistics = statistics;
  }

  @Override
  public BatchData getAllSatisfiedPageData() {
    if (valueFilter == null) {
      return batchData;
    }
    BatchData filteredBatchData = new BatchData(batchData.getDataType());
    while (batchData.hasCurrent()) {
      if (valueFilter.satisfy(batchData.currentTime(), batchData.currentValue())) {
        filteredBatchData.putAnObject(batchData.currentTime(), batchData.currentValue());
      }
      batchData.next();
    }
    return filteredBatchData;
  }

  @Override
  public Statistics getStatistics() {
    return statistics;
  }

  @Override
  public void setFilter(Filter filter) {
    this.valueFilter = filter;
  }

  @Override
  public boolean isModified() {
    return false;
  }
}
