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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import java.io.IOException;

public class SeriesReaderByTimestamp implements IReaderByTimestamp {

  private SeriesReader seriesReader;
  private BatchData batchData;
  private long currentTime = Long.MIN_VALUE;

  public SeriesReaderByTimestamp(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, TsFileFilter fileFilter) {
    seriesReader = new SeriesReader(seriesPath, dataType, context,
        dataSource, TimeFilter.gtEq(Long.MIN_VALUE), null, fileFilter);
  }

  public SeriesReaderByTimestamp(SeriesReader seriesReader) {
    this.seriesReader = seriesReader;
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps) throws IOException {
    Object[] result = new Object[timestamps.length];

    for (int i = 0; i < timestamps.length; i++) {
      if (timestamps[i] < currentTime) {
        throw new IOException("time must be increasing when use ReaderByTimestamp");
      }
      currentTime = timestamps[i];
      seriesReader.setTimeFilter(currentTime);
      if ((batchData == null || batchData.getMaxTimestamp() < currentTime)
          && !hasNext(currentTime)) {
        result[i] = null;
        continue;
      }
      result[i] = batchData.getValueInTimestamp(currentTime);
    }
    return result;
  }

  private boolean hasNext(long timestamp) throws IOException {

    /*
     * consume pages firstly
     */
    if (readPageData(timestamp)) {
      return true;
    }

    /*
     * consume chunk secondly
     */
    while (seriesReader.hasNextChunk()) {
      Statistics statistics = seriesReader.currentChunkStatistics();
      if (!satisfyTimeFilter(statistics)) {
        seriesReader.skipCurrentChunk();
        continue;
      }
      if (readPageData(timestamp)) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData(long timestamp) throws IOException {
    while (seriesReader.hasNextPage()) {
      if (!seriesReader.isPageOverlapped()) {
        if (!satisfyTimeFilter(seriesReader.currentPageStatistics())) {
          seriesReader.skipCurrentPage();
          continue;
        }
      }
      batchData = seriesReader.nextPage();
      if (isEmpty(batchData)) {
        continue;
      }
      if (batchData.getTimeByIndex(batchData.length() - 1) >= timestamp) {
        return true;
      }
    }
    return false;
  }

  private boolean satisfyTimeFilter(Statistics statistics) {
    return seriesReader.getTimeFilter().satisfy(statistics);
  }

  private boolean isEmpty(BatchData batchData) {
    return batchData == null || !batchData.hasCurrent();
  }
}
