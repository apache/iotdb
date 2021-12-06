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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.AlignedSeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocalAlignedGroupByExecutor implements AlignedGroupByExecutor {

  private final AlignedSeriesAggregateReader reader;
  private BatchData preCachedData;

  // Aggregate result buffer
  private final List<List<AggregateResult>> results = new ArrayList<>();

  private final TimeRange timeRange;

  // used for resetting the batch data to the last index
  private int lastReadCurArrayIndex;
  private int lastReadCurListIndex;

  private final boolean ascending;

  private final QueryDataSource queryDataSource;

  public LocalAlignedGroupByExecutor(
      PartialPath path,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    // init AlignedSeriesAggregateReader for aligned series
    Set<String> allSensors = new HashSet<>(((AlignedPath) path).getMeasurementList());
    reader =
        new AlignedSeriesAggregateReader(
            (AlignedPath) path,
            allSensors,
            TSDataType.VECTOR,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);
    preCachedData = null;
    timeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
    this.ascending = ascending;
  }

  @Override
  public void addAggregateResult(List<AggregateResult> aggregateResults) {
    results.add(aggregateResults);
  }

  @Override
  public List<List<AggregateResult>> calcAlignedResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {

    // clear result cache
    for (List<AggregateResult> resultsOfOneMeasurement : results) {
      for (AggregateResult result : resultsOfOneMeasurement) {
        result.reset();
      }
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

    // read from file
    while (reader.hasNextFile()) {
      // try to calc from fileMetaData
      Statistics fileTimeStatistics = reader.currentFileTimeStatistics();
      if (fileTimeStatistics.getStartTime() >= curEndTime) {
        if (ascending) {
          return results;
        } else {
          reader.skipCurrentFile();
          continue;
        }
      }
      if (reader.canUseCurrentFileStatistics()
          && timeRange.contains(
              fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
        // calc from fileMetaData
        while (reader.hasNextSubSeries()) {
          Statistics currentFileStatistics = reader.currentFileStatistics();
          calcFromStatistics(currentFileStatistics, results.get(reader.getCurIndex()));
          reader.nextSeries();
        }
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

  private void calcFromStatistics(Statistics statistics, List<AggregateResult> aggregateResultList)
      throws QueryProcessException {
    // statistics may be null for aligned time series
    if (statistics == null) {
      return;
    }
    if (statistics.getStartTime() == Long.MAX_VALUE && statistics.getEndTime() == Long.MIN_VALUE) {
      return;
    }
    for (AggregateResult result : aggregateResultList) {
      if (result.hasFinalResult()) {
        continue;
      }
      result.updateResultFromStatistics(statistics);
    }
  }

  private boolean readAndCalcFromChunk(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    while (reader.hasNextChunk()) {
      // try to calc from chunkMetaData
      Statistics chunkTimeStatistics = reader.currentChunkTimeStatistics();
      if (chunkTimeStatistics.getStartTime() >= curEndTime) {
        if (ascending) {
          return true;
        } else {
          reader.skipCurrentChunk();
          continue;
        }
      }
      if (reader.canUseCurrentChunkStatistics()
          && timeRange.contains(
              chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
        // calc from chunkMetaData
        while (reader.hasNextSubSeries()) {
          Statistics currentChunkStatistics = reader.currentChunkStatistics();
          calcFromStatistics(currentChunkStatistics, results.get(reader.getCurIndex()));
          reader.nextSeries();
        }
        reader.skipCurrentChunk();
        continue;
      }
      // read page
      if (readAndCalcFromPage(curStartTime, curEndTime)) {
        return true;
      }
    }
    return false;
  }

  private boolean readAndCalcFromPage(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    while (reader.hasNextPage()) {
      // try to calc from pageHeader
      Statistics pageTimeStatistics = reader.currentPageTimeStatistics();
      if (pageTimeStatistics != null) {
        // current page max than time range
        if (pageTimeStatistics.getStartTime() >= curEndTime) {
          if (ascending) {
            return true;
          } else {
            reader.skipCurrentPage();
            continue;
          }
        }
        if (reader.canUseCurrentPageStatistics()
            && timeRange.contains(
                pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
          // calc from pageHeader
          while (reader.hasNextSubSeries()) {
            int subIndex = reader.getCurIndex();
            Statistics currentPageStatistics = reader.currentPageStatistics();
            calcFromStatistics(currentPageStatistics, results.get(subIndex));
            reader.nextSeries();
          }
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

      // set initial Index
      lastReadCurArrayIndex = batchData.getReadCurArrayIndex();
      ;
      lastReadCurListIndex = batchData.getReadCurListIndex();
      ;

      // stop calc and cached current batchData
      if (ascending && batchData.currentTime() >= curEndTime) {
        preCachedData = batchData;
        return true;
      }

      // calc from batch data
      calcFromBatch(batchData, curStartTime, curEndTime);

      // judge whether the calculation finished
      if (isEndCalc()
          || (batchData.hasCurrent()
              && (ascending
                  ? batchData.currentTime() >= curEndTime
                  : batchData.currentTime() < curStartTime))) {
        return true;
      }
    }
    return false;
  }

  private boolean calcFromCacheData(long curStartTime, long curEndTime) throws IOException {
    calcFromBatch(preCachedData, curStartTime, curEndTime);
    // The result is calculated from the cache, judge whether the calculation finished
    return ((preCachedData != null
            && (ascending
                ? preCachedData.getMaxTimestamp() >= curEndTime
                : preCachedData.getMinTimestamp() < curStartTime))
        || isEndCalc());
  }

  private void calcFromBatch(BatchData batchData, long curStartTime, long curEndTime)
      throws IOException {
    // check if the batchData does not contain points in current interval
    if (!satisfied(batchData, curStartTime, curEndTime)) {
      return;
    }

    boolean hasCached = false;
    int curReadCurArrayIndex = lastReadCurArrayIndex;
    while (reader.hasNextSubSeries()) {
      int subIndex = reader.getCurIndex();
      batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
      List<AggregateResult> aggregateResultList = results.get(subIndex);
      for (AggregateResult result : aggregateResultList) {
        // current agg method has been calculated
        if (result.hasFinalResult()) {
          continue;
        }
        // lazy reset batch data for calculation
        batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
        IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator(subIndex);
        if (ascending) {
          // skip points that cannot be calculated
          while (batchDataIterator.hasNext(curStartTime, curEndTime)
              && batchDataIterator.currentTime() < curStartTime) {
            batchDataIterator.next();
          }
        } else {
          while (batchDataIterator.hasNext(curStartTime, curEndTime)
              && batchDataIterator.currentTime() >= curEndTime) {
            batchDataIterator.next();
          }
        }
        if (batchDataIterator.hasNext(curStartTime, curEndTime)) {
          result.updateResultFromPageData(batchDataIterator, curStartTime, curEndTime);
        }
        curReadCurArrayIndex =
            ascending
                ? Math.max(curReadCurArrayIndex, batchData.getReadCurArrayIndex())
                : Math.min(curReadCurArrayIndex, batchData.getReadCurArrayIndex());
      }
      // can calc for next interval
      if (!hasCached && batchData.hasCurrent()) {
        preCachedData = batchData;
        hasCached = true;
      }
      reader.nextSeries();
    }

    // reset the last position to current Index
    lastReadCurArrayIndex = curReadCurArrayIndex;
    lastReadCurListIndex = batchData.getReadCurListIndex();
    batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
  }

  private boolean isEndCalc() {
    for (List<AggregateResult> resultsOfOneMeasurement : results) {
      for (AggregateResult result : resultsOfOneMeasurement) {
        if (!result.hasFinalResult()) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean satisfied(BatchData batchData, long curStartTime, long curEndTime) {
    if (batchData == null || !batchData.hasCurrent()) {
      return false;
    }
    if (ascending
        && (batchData.getMaxTimestamp() < curStartTime || batchData.currentTime() >= curEndTime)) {
      return false;
    }
    if (!ascending
        && (batchData.getTimeByIndex(0) >= curEndTime || batchData.currentTime() < curStartTime)) {
      preCachedData = batchData;
      return false;
    }
    return true;
  }
}
