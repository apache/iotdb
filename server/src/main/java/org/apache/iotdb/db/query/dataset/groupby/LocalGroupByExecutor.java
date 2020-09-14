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

package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class LocalGroupByExecutor implements GroupByExecutor {

  private final IAggregateReader reader;
  private BatchData preCachedData;

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();
  private final TimeRange timeRange;

  // used for resetting the batch data to the last index
  private int lastReadCurArrayIndex;
  private int lastReadCurListIndex;

  private QueryDataSource queryDataSource;

  public LocalGroupByExecutor(PartialPath path, Set<String> allSensors, TSDataType dataType,
      QueryContext context, Filter timeFilter, TsFileFilter fileFilter)
      throws StorageEngineException, QueryProcessException {
    queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context, timeFilter);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
    this.reader = new SeriesAggregateReader(path, allSensors, dataType, context, queryDataSource,
        timeFilter, null, fileFilter);
    this.preCachedData = null;
    timeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
    lastReadCurArrayIndex = 0;
    lastReadCurListIndex = 0;
  }

  public boolean isEmpty() {
    return queryDataSource.getSeqResources().isEmpty() && queryDataSource.getUnseqResources()
        .isEmpty();
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  private boolean isEndCalc() {
    for (AggregateResult result : results) {
      if (!result.isCalculatedAggregationResult()) {
        return false;
      }
    }
    return true;
  }

  private boolean calcFromCacheData(long curStartTime, long curEndTime) throws IOException {
    calcFromBatch(preCachedData, curStartTime, curEndTime);
    // The result is calculated from the cache
    return (preCachedData != null && preCachedData.getMaxTimestamp() >= curEndTime) || isEndCalc();
  }

  private void calcFromBatch(BatchData batchData, long curStartTime, long curEndTime)
      throws IOException {
    // is error data
    if (batchData == null || !batchData.hasCurrent() || batchData.getMaxTimestamp() < curStartTime
        || batchData.currentTime() >= curEndTime) {
      return;
    }

    for (AggregateResult result : results) {
      // current agg method has been calculated
      if (result.isCalculatedAggregationResult()) {
        continue;
      }
      // lazy reset batch data for calculation
      batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
      // skip points that cannot be calculated
      while (batchData.hasCurrent() && batchData.currentTime() < curStartTime) {
        batchData.next();
      }
      if (batchData.hasCurrent()) {
        result.updateResultFromPageData(batchData, curEndTime);
      }
    }
    lastReadCurArrayIndex = batchData.getReadCurArrayIndex();
    lastReadCurListIndex = batchData.getReadCurListIndex();
    // can calc for next interval
    if (batchData.getMaxTimestamp() >= curEndTime) {
      preCachedData = batchData;
    }
  }

  private void calcFromStatistics(Statistics pageStatistics) throws QueryProcessException {
    for (AggregateResult result : results) {
      // cacl is compile
      if (result.isCalculatedAggregationResult()) {
        continue;
      }
      result.updateResultFromStatistics(pageStatistics);
    }
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    timeRange.set(curStartTime, curEndTime - 1);
    if (calcFromCacheData(curStartTime, curEndTime)) {
      return results;
    }

    // read page data firstly
    if (readAndCalcFromPage(curStartTime, curEndTime)) {
      return results;
    }

    // read chunk data secondly
    if (readAndCalcFromChunk(curStartTime, curEndTime)) {
      return results;
    }

    // read from file first
    while (reader.hasNextFile()) {
      Statistics fileStatistics = reader.currentFileStatistics();
      if (fileStatistics.getStartTime() >= curEndTime) {
        return results;
      }
      // calc from fileMetaData
      if (reader.canUseCurrentFileStatistics()
          && timeRange.contains(fileStatistics.getStartTime(), fileStatistics.getEndTime())) {
        calcFromStatistics(fileStatistics);
        reader.skipCurrentFile();
        continue;
      }

      // read chunk
      if (readAndCalcFromChunk(curStartTime, curEndTime)) {
        return results;
      }
    }

    return results;
  }

  private boolean readAndCalcFromChunk(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    while (reader.hasNextChunk()) {
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        return true;
      }
      // calc from chunkMetaData
      if (reader.canUseCurrentChunkStatistics()
          && timeRange.contains(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
        calcFromStatistics(chunkStatistics);
        reader.skipCurrentChunk();
        continue;
      }
      if (readAndCalcFromPage(curStartTime, curEndTime)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean readAndCalcFromPage(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    while (reader.hasNextPage()) {
      Statistics pageStatistics = reader.currentPageStatistics();
      // must be non overlapped page
      if (pageStatistics != null) {
        // current page max than time range
        if (pageStatistics.getStartTime() >= curEndTime) {
          return true;
        }
        // can use pageHeader
        if (reader.canUseCurrentPageStatistics()
            && timeRange.contains(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
          calcFromStatistics(pageStatistics);
          reader.skipCurrentPage();
          if (isEndCalc()) {
            return true;
          }
          continue;
        }
      }
      // calc from page data
      BatchData batchData = reader.nextPage();
      if (batchData == null || !batchData.hasCurrent()) {
        continue;
      }
      // stop calc and cached current batchData
      if (batchData.currentTime() >= curEndTime) {
        preCachedData = batchData;
        return true;
      }

      // reset the last position to zero
      lastReadCurArrayIndex = 0;
      lastReadCurListIndex = 0;
      calcFromBatch(batchData, curStartTime, curEndTime);

      // judge whether the calculation finished
      if (isEndCalc() || (batchData.hasCurrent() && batchData.currentTime() >= curEndTime)) {
        return true;
      }
    }
    return false;
  }
}
