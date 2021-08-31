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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;

import java.io.IOException;
import java.util.Set;

public class SeriesReaderByTimestamp implements IReaderByTimestamp {

  private SeriesReader seriesReader;
  private BatchData batchData;
  private boolean ascending;

  public SeriesReaderByTimestamp(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      TsFileFilter fileFilter,
      boolean ascending) {
    UnaryFilter timeFilter =
        ascending ? TimeFilter.gtEq(Long.MIN_VALUE) : TimeFilter.ltEq(Long.MAX_VALUE);
    this.seriesReader =
        new SeriesReader(
            seriesPath,
            allSensors,
            dataType,
            context,
            dataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);
    this.ascending = ascending;
  }

  public SeriesReaderByTimestamp(SeriesReader seriesReader, boolean ascending) {
    this.seriesReader = seriesReader;
    this.ascending = ascending;
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps, int length) throws IOException {
    if (length <= 0) {
      return null;
    }
    Object[] results = new Object[length];
    seriesReader.setTimeFilter(timestamps[0]);
    for (int i = 0; i < length; i++) {
      if ((batchData == null || !hasAvailableData(batchData, timestamps[i]))
          && !hasNext(timestamps[i])) {
        // there is no more data
        break;
      }
      results[i] = batchData.getValueInTimestamp(timestamps[i]);
    }

    return results;
  }

  @Override
  public boolean readerIsEmpty() throws IOException {
    return seriesReader.isEmpty() && isEmpty(batchData);
  }

  protected boolean hasNext(long timestamp) throws IOException {

    /*
     * consume pages firstly
     */
    if (readPageData(timestamp)) {
      return true;
    }

    /*
     * consume chunk secondly
     */
    if (readChunkData(timestamp)) {
      return true;
    }

    /*
     * consume file thirdly
     */
    while (seriesReader.hasNextFile()) {
      Statistics statistics = seriesReader.currentFileStatistics();
      if (!satisfyTimeFilter(statistics)) {
        seriesReader.skipCurrentFile();
        continue;
      }
      if (readChunkData(timestamp)) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData(long timestamp) throws IOException {
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
      if (hasAvailableData(batchData, timestamp)) {
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

  private boolean hasAvailableData(BatchData data, long time) {
    return ascending ? data.getMaxTimestamp() >= time : data.getMinTimestamp() <= time;
  }
}
