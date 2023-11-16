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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

public abstract class AbstractSeriesAggregationScanOperator extends AbstractDataSourceOperator {

  protected final boolean ascending;
  protected final boolean isGroupByQuery;

  protected int subSensorSize;

  protected TsBlock inputTsBlock;

  protected final ITimeRangeIterator timeRangeIterator;
  // Current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;

  // We still think aggregator in SeriesAggregateScanOperator is a inputRaw step.
  // But in facing of statistics, it will invoke another method processStatistics()
  protected final List<Aggregator> aggregators;

  // Using for building result tsBlock
  protected final TsBlockBuilder resultTsBlockBuilder;

  protected boolean finished = false;

  private final long cachedRawDataSize;

  /** Time slice for one next call in total, shared by the inner methods of the next() method */
  private long leftRuntimeOfOneNextCall;

  @SuppressWarnings("squid:S107")
  protected AbstractSeriesAggregationScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      SeriesScanUtil seriesScanUtil,
      int subSensorSize,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.isGroupByQuery = groupByTimeParameter != null;
    this.seriesScanUtil = seriesScanUtil;
    this.subSensorSize = subSensorSize;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.cachedRawDataSize =
        (1L + subSensorSize) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return cachedRawDataSize + maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return isGroupByQuery ? cachedRawDataSize : 0;
  }

  @Override
  public boolean hasNext() throws Exception {
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() throws Exception {
    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    leftRuntimeOfOneNextCall = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long maxRuntime = leftRuntimeOfOneNextCall;

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (curTimeRange == null) {
        // move to the next time window
        curTimeRange = timeRangeIterator.nextTimeRange();
        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      // calculate aggregation result on current time window
      // Keep curTimeRange if the calculation of this timeRange is not done
      if (calculateAggregationResultForCurrentTimeRange()) {
        curTimeRange = null;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      return resultTsBlock;
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  @SuppressWarnings("squid:S112")
  /** Return true if we have the result of this timeRange. */
  protected boolean calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        return true;
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()
          && !seriesScanUtil.hasNextChunk()
          && readAndCalcFromFile()) {
        updateResultTsBlock();
        return true;
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()
          || seriesScanUtil.hasNextChunk()
          || seriesScanUtil.hasNextFile()) {
        return false;
      }
      updateResultTsBlock();
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  protected void updateResultTsBlock() {
    appendAggregationResult(
        resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  private boolean calcFromRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult =
        calculateAggregationFromRawData(tsBlock, aggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }

  protected void calcFromStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(timeStatistics, valueStatistics);
    }
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextFile()) {
      if (canUseCurrentFileStatistics()) {
        Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();
        if (fileTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (curTimeRange.contains(
            fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentFileStatistics(i);
          }
          calcFromStatistics(fileTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentFile();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read chunk
      if (readAndCalcFromChunk()) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromChunk() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextChunk()) {
      if (canUseCurrentChunkStatistics()) {
        Statistics chunkTimeStatistics = seriesScanUtil.currentChunkTimeStatistics();
        if (chunkTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }
        // calc from chunkMetaData
        if (curTimeRange.contains(
            chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
          // calc from chunkMetaData
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentChunkStatistics(i);
          }
          calcFromStatistics(chunkTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentChunk();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read page
      if (readAndCalcFromPage()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromPage() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    try {
      while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextPage()) {
        if (canUseCurrentPageStatistics()) {
          Statistics pageTimeStatistics = seriesScanUtil.currentPageTimeStatistics();
          // There is no more eligible points in current time range
          if (pageTimeStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              return true;
            } else {
              seriesScanUtil.skipCurrentPage();
              continue;
            }
          }
          // can use pageHeader
          if (curTimeRange.contains(
              pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
            Statistics[] statisticsList = new Statistics[subSensorSize];
            for (int i = 0; i < subSensorSize; i++) {
              statisticsList[i] = seriesScanUtil.currentPageStatistics(i);
            }
            calcFromStatistics(pageTimeStatistics, statisticsList);
            seriesScanUtil.skipCurrentPage();
            if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
              return true;
            } else {
              continue;
            }
          }
        }

        // calc from page data
        TsBlock tsBlock = seriesScanUtil.nextPage();
        if (tsBlock == null || tsBlock.isEmpty()) {
          continue;
        }

        // calc from raw data
        if (calcFromRawData(tsBlock)) {
          return true;
        }
      }
      return false;
    } finally {
      leftRuntimeOfOneNextCall -= (System.nanoTime() - start);
    }
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileTimeStatistics();
    return !seriesScanUtil.isFileOverlapped()
        && fileStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
        && !seriesScanUtil.currentFileModified();
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkTimeStatistics();
    return !seriesScanUtil.isChunkOverlapped()
        && chunkStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
        && !seriesScanUtil.currentChunkModified();
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageTimeStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
        && currentPageStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
        && !seriesScanUtil.currentPageModified();
  }
}
