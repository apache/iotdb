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
package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class AggregateReader implements IAggregateReader {

  private final SeriesReader seriesReader;

  public AggregateReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, dataSource, timeFilter,
        valueFilter);
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return seriesReader.hasNextChunk();
  }

  /**
   * only be used for aggregate without value filter
   *
   * @return
   */
  @Override
  public boolean canUseCurrentChunkStatistics() {
    Statistics chunkStatistics = currentChunkStatistics();
    return !seriesReader.isChunkOverlapped() && satisfyTimeFilter(chunkStatistics);
  }

  @Override
  public Statistics currentChunkStatistics() {
    return seriesReader.currentChunkStatistics();
  }

  @Override
  public void skipCurrentChunk() throws IOException {
    seriesReader.skipCurrentChunk();
  }

  @Override
  public boolean hasNextPage() throws IOException {
    return seriesReader.hasNextPage();
  }


  @Override
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    return !seriesReader.isPageOverlapped() && satisfyTimeFilter(currentPageStatistics);
  }

  @Override
  public Statistics currentPageStatistics() throws IOException {
    return seriesReader.currentPageStatistics();
  }

  @Override
  public void skipCurrentPage() {
    seriesReader.skipCurrentPage();
  }

  @Override
  public boolean hasNextOverlappedPage() throws IOException {
    return seriesReader.hasNextOverlappedPage();
  }

  @Override
  public BatchData nextOverlappedPage() throws IOException {
    return seriesReader.nextOverlappedPage();
  }


  private boolean satisfyTimeFilter(Statistics statistics) {
    Filter timeFilter = seriesReader.getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }
}
