/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.utils.timerangeiterator.SingleTimeWindowIterator;
import org.apache.iotdb.db.utils.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This operator is responsible to do the aggregation calculation for one series based on global
 * time range and time split parameter.
 *
 * <p>Every time next() is invoked, one tsBlock which contains current time window will be returned.
 * In sliding window situation, current time window is a pre-aggregation window. If there is no time
 * split parameter, i.e. aggregation without groupBy, just one tsBlock will be returned.
 */
public class SeriesAggregateScanOperator implements DataSourceOperator {

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;
  private final SeriesScanUtil seriesScanUtil;
  private final boolean ascending;
  private List<AggregateResult> aggregateResultList;

  private ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private TsBlockSingleColumnIterator preCachedData;
  // used for resetting the preCachedData to the last read index
  private int lastReadIndex;

  private TsBlockBuilder tsBlockBuilder;
  private TsBlock resultTsBlock;
  private boolean hasCachedTsBlock = false;
  private boolean finished = false;

  public SeriesAggregateScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      OperatorContext context,
      List<AggregationType> aggregateFuncList,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.seriesScanUtil =
        new SeriesScanUtil(
            seriesPath,
            allSensors,
            seriesPath.getSeriesType(),
            context.getInstanceContext(),
            timeFilter,
            null,
            ascending);
    aggregateResultList = new ArrayList<>(aggregateFuncList.size());
    for (AggregationType aggregationType : aggregateFuncList) {
      aggregateResultList.add(
          AggregateResultFactory.getAggrResultByType(
              aggregationType,
              seriesPath.getSeriesType(),
              seriesScanUtil.getOrderUtils().getAscending()));
    }
    tsBlockBuilder =
        new TsBlockBuilder(
            aggregateFuncList.stream()
                .map(
                    functionType ->
                        SchemaUtils.getSeriesTypeByPath(seriesPath, functionType.name()))
                .collect(Collectors.toList()));
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter);
  }

  /**
   * If groupByTimeParameter is null, which means it's an aggregation query without down sampling.
   * Aggregation query has only one time window and the result set of it does not contain a
   * timestamp, so it doesn't matter what the time range returns.
   */
  public ITimeRangeIterator initTimeRangeIterator(GroupByTimeParameter groupByTimeParameter) {
    if (groupByTimeParameter == null) {
      return new SingleTimeWindowIterator(0, Long.MAX_VALUE);
    } else {
      return TimeRangeIteratorFactory.getTimeRangeIterator(
          groupByTimeParameter.getStartTime(),
          groupByTimeParameter.getEndTime(),
          groupByTimeParameter.getInterval(),
          groupByTimeParameter.getSlidingStep(),
          ascending,
          groupByTimeParameter.isIntervalByMonth(),
          groupByTimeParameter.isSlidingStepByMonth(),
          groupByTimeParameter.getInterval() > groupByTimeParameter.getSlidingStep());
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  // TODO
  @Override
  public ListenableFuture<Void> isBlocked() {
    return DataSourceOperator.super.isBlocked();
  }

  @Override
  public TsBlock next() {
    if (hasCachedTsBlock || hasNext()) {
      hasCachedTsBlock = false;
      return resultTsBlock;
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    if (hasCachedTsBlock) {
      return true;
    }
    try {
      if (!timeRangeIterator.hasNextTimeRange()) {
        return false;
      }
      curTimeRange = timeRangeIterator.nextTimeRange();

      // 1. Clear previous aggregation result
      for (AggregateResult result : aggregateResultList) {
        result.reset();
      }

      // 2. Calculate aggregation result based on current time window
      if (calcFromCacheData(curTimeRange)) {
        updateResultTsBlockUsingAggregateResult();
        return true;
      }

      // read page data firstly
      if (readAndCalcFromPage(curTimeRange)) {
        updateResultTsBlockUsingAggregateResult();
        return true;
      }

      // read chunk data secondly
      if (readAndCalcFromChunk(curTimeRange)) {
        updateResultTsBlockUsingAggregateResult();
        return true;
      }

      // read from file first
      while (seriesScanUtil.hasNextFile()) {
        Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
        if (fileStatistics.getStartTime() >= curTimeRange.getMax()) {
          if (ascending) {
            updateResultTsBlockUsingAggregateResult();
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (canUseCurrentFileStatistics()
            && curTimeRange.contains(fileStatistics.getStartTime(), fileStatistics.getEndTime())) {
          calcFromStatistics(fileStatistics);
          seriesScanUtil.skipCurrentFile();
          continue;
        }

        // read chunk
        if (readAndCalcFromChunk(curTimeRange)) {
          updateResultTsBlockUsingAggregateResult();
          return true;
        }
      }

      updateResultTsBlockUsingAggregateResult();
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  private void updateResultTsBlockUsingAggregateResult() {
    // TODO AVG
    tsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    // Use start time of current time range as time column
    timeColumnBuilder.writeLong(curTimeRange.getMin());
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = 0; i < aggregateResultList.size(); i++) {
      columnBuilders[i].writeObject(aggregateResultList.get(i).getResult());
    }
    tsBlockBuilder.declarePosition();
    resultTsBlock = tsBlockBuilder.build();
    hasCachedTsBlock = true;
  }

  // TODO Implement it later?
  @Override
  public void close() throws Exception {
    DataSourceOperator.super.close();
  }

  @Override
  public boolean isFinished() {
    return finished || (finished = hasNext());
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }

  /** @return if already get the result */
  private boolean calcFromCacheData(TimeRange curTimeRange) throws IOException {
    calcFromBatch(preCachedData, curTimeRange);
    // The result is calculated from the cache
    return (preCachedData != null
            && (ascending
                ? preCachedData.getEndTime() >= curTimeRange.getMax()
                : preCachedData.getStartTime() < curTimeRange.getMin()))
        || isEndCalc();
  }

  @SuppressWarnings("squid:S3776")
  private void calcFromBatch(TsBlockSingleColumnIterator blockIterator, TimeRange curTimeRange)
      throws IOException {
    // check if the batchData does not contain points in current interval
    if (!satisfied(blockIterator, curTimeRange)) {
      return;
    }

    for (AggregateResult result : aggregateResultList) {
      // current agg method has been calculated
      if (result.hasFinalResult()) {
        continue;
      }
      // lazy reset batch data for calculation
      blockIterator.setRowIndex(lastReadIndex);
      // skip points that cannot be calculated
      skipOutOfTimeRangePoints(blockIterator, curTimeRange);

      if (blockIterator.hasNext()) {
        result.updateResultFromPageData(
            blockIterator, curTimeRange.getMin(), curTimeRange.getMax());
      }
    }

    // reset the last position to current Index
    lastReadIndex = blockIterator.getRowIndex();

    // can calc for next interval
    if (blockIterator.hasNext()) {
      preCachedData = blockIterator;
    }
  }

  // skip points that cannot be calculated
  private void skipOutOfTimeRangePoints(
      TsBlockSingleColumnIterator tsBlockIterator, TimeRange curTimeRange) {
    if (ascending) {
      while (tsBlockIterator.hasNext() && tsBlockIterator.currentTime() < curTimeRange.getMin()) {
        tsBlockIterator.next();
      }
    } else {
      while (tsBlockIterator.hasNext() && tsBlockIterator.currentTime() >= curTimeRange.getMax()) {
        tsBlockIterator.next();
      }
    }
  }

  private boolean satisfied(TsBlockSingleColumnIterator tsBlockIterator, TimeRange timeRange) {
    if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
      return false;
    }

    if (ascending
        && (tsBlockIterator.getEndTime() < timeRange.getMin()
            || tsBlockIterator.currentTime() >= timeRange.getMax())) {
      return false;
    }
    if (!ascending
        && (tsBlockIterator.getStartTime() >= timeRange.getMax()
            || tsBlockIterator.currentTime() < timeRange.getMin())) {
      preCachedData = tsBlockIterator;
      return false;
    }
    return true;
  }

  private boolean isEndCalc() {
    for (AggregateResult result : aggregateResultList) {
      if (!result.hasFinalResult()) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean readAndCalcFromPage(TimeRange curTimeRange) throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      Statistics pageStatistics = seriesScanUtil.currentPageStatistics();
      // must be non overlapped page
      if (pageStatistics != null) {
        // There is no more eligible points in current time range
        if (pageStatistics.getStartTime() >= curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentPage();
            continue;
          }
        }
        // can use pageHeader
        if (canUseCurrentPageStatistics()
            && curTimeRange.contains(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
          calcFromStatistics(pageStatistics);
          seriesScanUtil.skipCurrentPage();
          if (isEndCalc()) {
            return true;
          }
          continue;
        }
      }

      // calc from page data
      TsBlockSingleColumnIterator tsBlockIterator =
          seriesScanUtil.nextPage().getTsBlockSingleColumnIterator();
      if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
        continue;
      }

      // reset the last position to current Index
      lastReadIndex = tsBlockIterator.getRowIndex();

      // stop calc and cached current batchData
      if (ascending && tsBlockIterator.currentTime() >= curTimeRange.getMax()) {
        preCachedData = tsBlockIterator;
        return true;
      }

      // calc from batch data
      calcFromBatch(tsBlockIterator, curTimeRange);

      // judge whether the calculation finished
      if (isEndCalc()
          || (tsBlockIterator.hasNext()
              && (ascending
                  ? tsBlockIterator.currentTime() >= curTimeRange.getMax()
                  : tsBlockIterator.currentTime() < curTimeRange.getMin()))) {
        return true;
      }
    }
    return false;
  }

  private boolean readAndCalcFromChunk(TimeRange curTimeRange) throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curTimeRange.getMax()) {
        if (ascending) {
          return true;
        } else {
          seriesScanUtil.skipCurrentChunk();
          continue;
        }
      }
      // calc from chunkMetaData
      if (canUseCurrentChunkStatistics()
          && curTimeRange.contains(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
        calcFromStatistics(chunkStatistics);
        seriesScanUtil.skipCurrentChunk();
        continue;
      }
      // read page
      if (readAndCalcFromPage(curTimeRange)) {
        return true;
      }
    }
    return false;
  }

  private void calcFromStatistics(Statistics statistics) {
    try {
      for (AggregateResult result : aggregateResultList) {
        if (result.hasFinalResult()) {
          continue;
        }
        result.updateResultFromStatistics(statistics);
      }
    } catch (QueryProcessException e) {
      throw new RuntimeException("Error while updating result using statistics", e);
    }
  }

  public boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
    return !seriesScanUtil.isFileOverlapped()
        && containedByTimeFilter(fileStatistics)
        && !seriesScanUtil.currentFileModified();
  }

  public boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics();
    return !seriesScanUtil.isChunkOverlapped()
        && containedByTimeFilter(chunkStatistics)
        && !seriesScanUtil.currentChunkModified();
  }

  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
        && containedByTimeFilter(currentPageStatistics)
        && !seriesScanUtil.currentPageModified();
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    Filter timeFilter = seriesScanUtil.getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }
}
