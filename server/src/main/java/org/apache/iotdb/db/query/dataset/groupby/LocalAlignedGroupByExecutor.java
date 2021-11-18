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
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.*;

public class LocalAlignedGroupByExecutor implements GroupByExecutor {

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
    lastReadCurArrayIndex = 0;
    lastReadCurListIndex = 0;
    this.ascending = ascending;
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    throw new UnsupportedOperationException(
        "This method is not supported in LocalAlignedGroupByExecutor");
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    throw new UnsupportedOperationException(
        "This method is not supported in LocalAlignedGroupByExecutor");
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    throw new UnsupportedOperationException(
        "This method is not supported in LocalAlignedGroupByExecutor");
  }

  public void addAggregateResultList(List<AggregateResult> aggrResultList) {
    results.add(aggrResultList);
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
      int remainingToCalculate = reader.getSubSensorSize();
      while (reader.hasNextSubSeries()) {
        Statistics fileStatistics = reader.currentFileStatistics();
        if (reader.canUseCurrentFileStatistics()
            && timeRange.contains(fileStatistics.getStartTime(), fileStatistics.getEndTime())) {
          calcFromStatistics(fileStatistics, results.get(reader.getCurIndex()));
          remainingToCalculate--;
        }
        reader.nextSeries();
      }
      if (remainingToCalculate == 0) {
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
      int remainingToCalculate = reader.getSubSensorSize();
      while (reader.hasNextSubSeries()) {
        Statistics chunkStatistics = reader.currentChunkStatistics();
        if (reader.canUseCurrentChunkStatistics()
            && timeRange.contains(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
          calcFromStatistics(chunkStatistics, results.get(reader.getCurIndex()));
          remainingToCalculate--;
        }
        reader.nextSeries();
      }
      if (remainingToCalculate == 0) {
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
      int remainingToCalculate = reader.getSubSensorSize();
      while (reader.hasNextSubSeries()) {
        Statistics pageStatistics = reader.currentPageStatistics();
        if (pageStatistics != null
            && reader.canUseCurrentPageStatistics()
            && timeRange.contains(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
          calcFromStatistics(pageStatistics, results.get(reader.getCurIndex()));
          remainingToCalculate--;
        }
        reader.nextSeries();
      }
      if (remainingToCalculate == 0) {
        reader.skipCurrentPage();
        continue;
      }

      // calc from page data
      BatchData batchData = reader.nextPage();
      if (batchData == null || !batchData.hasCurrent()) {
        continue;
      }

      // stop calc and cached current batchData
      if (ascending && batchData.currentTime() >= curEndTime) {
        preCachedData = batchData;
        // reset the last position to current Index
        lastReadCurArrayIndex = batchData.getReadCurArrayIndex();
        lastReadCurListIndex = batchData.getReadCurListIndex();
        return true;
      }

      // reset the last position to current Index
      lastReadCurArrayIndex = batchData.getReadCurArrayIndex();
      lastReadCurListIndex = batchData.getReadCurListIndex();

      // calc from batch data
      while (reader.hasNextSubSeries()) {
        int subIndex = reader.getCurIndex();
        batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
        calcFromBatch(batchData, subIndex, curStartTime, curEndTime, results.get(subIndex));
        reader.nextSeries();
      }

      // reset the last position to current Index
      lastReadCurArrayIndex = batchData.getReadCurArrayIndex();
      lastReadCurListIndex = batchData.getReadCurListIndex();

      // can calc for next interval
      if (batchData.hasCurrent()) {
        preCachedData = batchData;
      }

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
    if (preCachedData == null) return false;
    while (reader.hasNextSubSeries()) {
      int subIndex = reader.getCurIndex();
      preCachedData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
      calcFromBatch(preCachedData, subIndex, curStartTime, curEndTime, results.get(subIndex));
      reader.nextSeries();
    }
    // The result is calculated from the cache
    return (preCachedData != null
            && (ascending
                ? preCachedData.getMaxTimestamp() >= curEndTime
                : preCachedData.getMinTimestamp() < curStartTime))
        || isEndCalc();
  }

  private void calcFromBatch(
      BatchData batchData,
      int curIndex,
      long curStartTime,
      long curEndTime,
      List<AggregateResult> aggregateResultList)
      throws IOException {
    // check if the batchData does not contain points in current interval
    if (!satisfied(batchData, curStartTime, curEndTime)) {
      return;
    }

    for (AggregateResult result : aggregateResultList) {
      // current agg method has been calculated
      if (result.hasFinalResult()) {
        continue;
      }
      // lazy reset batch data for calculation
      batchData.resetBatchData(lastReadCurArrayIndex, lastReadCurListIndex);
      IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator(curIndex);
      if (ascending) {
        // skip points that cannot be calculated
        while (batchDataIterator.hasNext() && batchDataIterator.currentTime() < curStartTime) {
          batchDataIterator.next();
        }
      } else {
        while (batchDataIterator.hasNext() && batchDataIterator.currentTime() >= curEndTime) {
          batchDataIterator.next();
        }
      }

      if (batchDataIterator.hasNext()) {
        result.updateResultFromPageData(batchDataIterator, curStartTime, curEndTime);
      }
    }
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
