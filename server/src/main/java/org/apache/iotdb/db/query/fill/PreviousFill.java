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
package org.apache.iotdb.db.query.fill;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.InvertedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.io.IOException;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class PreviousFill extends IFill {

  private long beforeRange;
  private BatchData batchData;
  private InvertedSeriesReader dataReader;

  public PreviousFill(TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    batchData = new BatchData();
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy() {
    return new PreviousFill(dataType, queryTime, beforeRange);
  }

  @Override
  Filter constructFilter() {
    Filter lowerBound = beforeRange == -1 ? TimeFilter.gtEq(Long.MIN_VALUE)
        : TimeFilter.gtEq(queryTime - beforeRange);
    // time in [queryTime - beforeRange, queryTime]
    return FilterFactory.and(lowerBound, TimeFilter.ltEq(queryTime));
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  @Override
  public void constructReaders(Path path, QueryContext context)
      throws StorageEngineException {
    Filter timeFilter = constructFilter();
    dataReader = new InvertedSeriesReader(path, dataType, context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter),
        timeFilter, null, null);
  }

  @Override
  public TimeValuePair getFillResult() throws IOException {
    TimeValuePair fillPair = null;

    while (dataReader.hasNextChunk()) {
      // cal by chunk statistics
      if (dataReader.canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = dataReader.currentChunkStatistics();
        if (fillPair == null || fillPair.getTimestamp() < chunkStatistics.getEndTime()) {
          fillPair = new TimeValuePair(
                  chunkStatistics.getEndTime(),
                  TsPrimitiveType.getByType(dataType, chunkStatistics.getLastValue()));
        }
        dataReader.skipCurrentChunk();
      } else {
        BatchData lastBatchData = null;
        while (dataReader.hasNextPage()) {
          lastBatchData = dataReader.nextPage();
        }

        if (lastBatchData != null && lastBatchData.length() > 0) {
          if (fillPair == null || fillPair.getTimestamp() < lastBatchData.getMaxTimestamp()) {
            fillPair = new TimeValuePair(
                lastBatchData.getMaxTimestamp(),
                lastBatchData.getTsPrimitiveTypeByIndex(lastBatchData.length() - 1));
          }
        }
      }
    }

    if (fillPair != null) {
      fillPair.setTimestamp(queryTime);
    } else {
      fillPair = new TimeValuePair(queryTime, null);
    }
    return fillPair;
  }

}
