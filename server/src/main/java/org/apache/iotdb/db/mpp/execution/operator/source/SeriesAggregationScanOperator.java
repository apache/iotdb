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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.isEndCalc;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.skipOutOfTimeRangePoints;

/**
 * This operator is responsible to do the aggregation calculation for one series based on global
 * time range and time split parameter.
 *
 * <p>Every time next() is invoked, one tsBlock which contains current time window will be returned.
 * In sliding window situation, current time window is a pre-aggregation window. If there is no time
 * split parameter, i.e. aggregation without groupBy, just one tsBlock will be returned.
 */
public class SeriesAggregationScanOperator implements DataSourceOperator {

  protected final OperatorContext operatorContext;
  protected final PlanNodeId sourceId;
  protected final SeriesScanUtil seriesScanUtil;
  protected final boolean ascending;
  // We still think aggregator in SeriesAggregateScanOperator is a inputRaw step.
  // But in facing of statistics, it will invoke another method processStatistics()
  protected final List<Aggregator> aggregators;

  protected final ITimeRangeIterator timeRangeIterator;
  // current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;
  protected final boolean isGroupByQuery;

  protected TsBlock preCachedData;

  protected final TsBlockBuilder resultTsBlockBuilder;
  protected boolean finished = false;

  public SeriesAggregationScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      OperatorContext context,
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;

    if (seriesPath instanceof MeasurementPath) {
      this.seriesScanUtil =
          new SeriesScanUtil(
              seriesPath,
              allSensors,
              seriesPath.getSeriesType(),
              context.getInstanceContext(),
              timeFilter,
              null,
              ascending);
    } else if (seriesPath instanceof AlignedPath) {
      this.seriesScanUtil =
          new AlignedSeriesScanUtil(
              seriesPath,
              new HashSet<>(((AlignedPath) seriesPath).getMeasurementList()),
              context.getInstanceContext(),
              timeFilter,
              null,
              ascending);
    } else {
      throw new IllegalArgumentException(
          "The seriesPath should be MeasurementPath or AlignedPath.");
    }

    this.aggregators = aggregators;
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.timeRangeIterator = initTimeRangeIterator(groupByTimeParameter, ascending, true);
    this.isGroupByQuery = groupByTimeParameter != null;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public boolean hasNext() {
    return timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() {
    resultTsBlockBuilder.reset();
    while (timeRangeIterator.hasNextTimeRange() && !resultTsBlockBuilder.isFull()) {
      curTimeRange = timeRangeIterator.nextTimeRange();

      // 1. Clear previous aggregation result
      for (Aggregator aggregator : aggregators) {
        aggregator.reset();
        aggregator.updateTimeRange(curTimeRange);
      }

      // 2. Calculate aggregation result based on current time window
      calculateNextResult();
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      return resultTsBlockBuilder.build();
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() {
    return finished || (finished = !hasNext());
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }

  protected void calculateNextResult() {
    try {
      if (calcFromCacheData(curTimeRange)) {
        updateResultTsBlock();
        return;
      }

      // read page data firstly
      if (readAndCalcFromPage(curTimeRange)) {
        updateResultTsBlock();
        return;
      }

      // read chunk data secondly
      if (readAndCalcFromChunk(curTimeRange)) {
        updateResultTsBlock();
        return;
      }

      // read from file first
      while (seriesScanUtil.hasNextFile()) {
        if (canUseCurrentFileStatistics()) {
          Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
          if (fileStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              updateResultTsBlock();
              return;
            } else {
              seriesScanUtil.skipCurrentFile();
              continue;
            }
          }
          // calc from fileMetaData
          if (curTimeRange.contains(fileStatistics.getStartTime(), fileStatistics.getEndTime())) {
            calcFromStatistics(fileStatistics);
            seriesScanUtil.skipCurrentFile();
            if (isEndCalc(aggregators) && !isGroupByQuery) {
              break;
            } else {
              continue;
            }
          }
        }

        // read chunk
        if (readAndCalcFromChunk(curTimeRange)) {
          updateResultTsBlock();
          return;
        }
      }

      updateResultTsBlock();
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  protected void updateResultTsBlock() {
    appendAggregationResult(resultTsBlockBuilder, aggregators, timeRangeIterator);
  }

  /** @return if already get the result */
  protected boolean calcFromCacheData(TimeRange curTimeRange) throws IOException {
    calcFromBatch(preCachedData, curTimeRange);
    // The result is calculated from the cache
    return (preCachedData != null
            && (ascending
                ? preCachedData.getEndTime() > curTimeRange.getMax()
                : preCachedData.getEndTime() < curTimeRange.getMin()))
        || isEndCalc(aggregators);
  }

  protected void calcFromBatch(TsBlock tsBlock, TimeRange curTimeRange) {
    // check if the batchData does not contain points in current interval
    if (tsBlock != null && satisfied(tsBlock, curTimeRange, ascending)) {
      // skip points that cannot be calculated
      if ((ascending && tsBlock.getStartTime() < curTimeRange.getMin())
          || (!ascending && tsBlock.getStartTime() > curTimeRange.getMax())) {
        tsBlock = skipOutOfTimeRangePoints(tsBlock, curTimeRange, ascending);
      }

      int lastReadRowIndex = 0;
      for (Aggregator aggregator : aggregators) {
        // current agg method has been calculated
        if (aggregator.hasFinalResult()) {
          continue;
        }

        lastReadRowIndex = Math.max(lastReadRowIndex, aggregator.processTsBlock(tsBlock));
      }
      if (lastReadRowIndex >= tsBlock.getPositionCount()) {
        tsBlock = null;
      } else {
        tsBlock = tsBlock.subTsBlock(lastReadRowIndex);
      }

      // can calc for next interval
      if (tsBlock != null && tsBlock.getTsBlockSingleColumnIterator().hasNext()) {
        preCachedData = tsBlock;
      }
    }
  }

  protected boolean satisfied(TsBlock tsBlock, TimeRange timeRange, boolean ascending) {
    TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
    if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
      return false;
    }

    if (ascending
        && (tsBlockIterator.getEndTime() < timeRange.getMin()
            || tsBlockIterator.currentTime() > timeRange.getMax())) {
      return false;
    }
    if (!ascending
        && (tsBlockIterator.getEndTime() > timeRange.getMax()
            || tsBlockIterator.currentTime() < timeRange.getMin())) {
      preCachedData = tsBlock;
      return false;
    }
    return true;
  }

  private void calcFromStatistics(Statistics statistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(statistics);
    }
  }

  private boolean readAndCalcFromPage(TimeRange curTimeRange) throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      if (canUseCurrentPageStatistics()) {
        Statistics pageStatistics = seriesScanUtil.currentPageStatistics();
        // There is no more eligible points in current time range
        if (pageStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentPage();
            continue;
          }
        }
        // can use pageHeader
        if (curTimeRange.contains(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
          calcFromStatistics(pageStatistics);
          seriesScanUtil.skipCurrentPage();
          if (isEndCalc(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // calc from page data
      TsBlock tsBlock = seriesScanUtil.nextPage();
      TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
      if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
        continue;
      }

      // stop calc and cached current batchData
      if (ascending && tsBlockIterator.currentTime() > curTimeRange.getMax()) {
        preCachedData = tsBlock;
        return true;
      }

      // calc from batch data
      calcFromBatch(tsBlock, curTimeRange);

      // judge whether the calculation finished
      boolean isTsBlockOutOfBound =
          ascending
              ? tsBlock.getEndTime() > curTimeRange.getMax()
              : tsBlock.getEndTime() < curTimeRange.getMin();
      if (isEndCalc(aggregators) || isTsBlockOutOfBound) {
        return true;
      }
    }
    return false;
  }

  private boolean readAndCalcFromChunk(TimeRange curTimeRange) throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics();
        if (chunkStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }
        // calc from chunkMetaData
        if (curTimeRange.contains(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
          calcFromStatistics(chunkStatistics);
          seriesScanUtil.skipCurrentChunk();
          if (isEndCalc(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read page
      if (readAndCalcFromPage(curTimeRange)) {
        return true;
      }
    }
    return false;
  }

  private boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
    return !seriesScanUtil.isFileOverlapped()
        && fileStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentFileModified();
  }

  private boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics();
    return !seriesScanUtil.isChunkOverlapped()
        && chunkStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentChunkModified();
  }

  private boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
        && currentPageStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentPageModified();
  }
}
