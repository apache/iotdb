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
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Set;

public class VectorSeriesAggregateReader implements IAggregateReader {

  private final SeriesReader seriesReader;
  /**
   * Used to locate the subSensor that we are traversing now. Use hasNextSubSeries() method to check
   * if we have more sub series in one loop. And use nextSeries() method to move to next sub series.
   */
  private int curIndex = 0;

  private final int subSensorSize;

  public VectorSeriesAggregateReader(
      VectorPartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    this.seriesReader =
        new SeriesReader(
            seriesPath,
            allSensors,
            dataType,
            context,
            dataSource,
            timeFilter,
            valueFilter,
            fileFilter,
            ascending);
    this.subSensorSize = seriesPath.getSubSensorsList().size();
  }

  @Override
  public boolean isAscending() {
    return seriesReader.getOrderUtils().getAscending();
  }

  @Override
  public boolean hasNextFile() throws IOException {
    return seriesReader.hasNextFile();
  }

  @Override
  public boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = currentFileStatistics();
    return !seriesReader.isFileOverlapped()
        && containedByTimeFilter(fileStatistics)
        && !seriesReader.currentFileModified();
  }

  @Override
  public Statistics currentFileStatistics() throws IOException {
    return seriesReader.currentFileStatistics(curIndex);
  }

  @Override
  public void skipCurrentFile() {
    seriesReader.skipCurrentFile();
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return seriesReader.hasNextChunk();
  }

  @Override
  public boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = currentChunkStatistics();
    return !seriesReader.isChunkOverlapped()
        && containedByTimeFilter(chunkStatistics)
        && !seriesReader.currentChunkModified();
  }

  @Override
  public Statistics currentChunkStatistics() throws IOException {
    return seriesReader.currentChunkStatistics(curIndex);
  }

  @Override
  public void skipCurrentChunk() {
    seriesReader.skipCurrentChunk();
  }

  @Override
  public boolean hasNextPage() throws IOException {
    return seriesReader.hasNextPage();
  }

  @Override
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesReader.isPageOverlapped()
        && containedByTimeFilter(currentPageStatistics)
        && !seriesReader.currentPageModified();
  }

  @Override
  public Statistics currentPageStatistics() throws IOException {
    return seriesReader.currentPageStatistics(curIndex);
  }

  @Override
  public void skipCurrentPage() {
    seriesReader.skipCurrentPage();
  }

  @Override
  public BatchData nextPage() throws IOException {
    return seriesReader.nextPage().flip();
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    Filter timeFilter = seriesReader.getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  public boolean hasNextSubSeries() {
    if (getCurIndex() < subSensorSize) {
      return true;
    } else {
      resetIndex();
      return false;
    }
  }

  public void nextSeries() {
    curIndex++;
  }

  public int getCurIndex() {
    return curIndex;
  }

  public void resetIndex() {
    curIndex = 0;
  }
}
