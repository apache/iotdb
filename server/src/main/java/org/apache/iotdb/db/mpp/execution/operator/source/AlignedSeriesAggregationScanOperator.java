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

import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

/** This operator is responsible to do the aggregation calculation especially for aligned series. */
public class AlignedSeriesAggregationScanOperator extends SeriesAggregationScanOperator
    implements DataSourceOperator {

  private final int subSensorSize;

  public AlignedSeriesAggregationScanOperator(
      PlanNodeId sourceId,
      AlignedPath seriesPath,
      OperatorContext context,
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    super(
        sourceId,
        seriesPath,
        Collections.emptySet(),
        context,
        aggregators,
        timeFilter,
        ascending,
        groupByTimeParameter);
    this.subSensorSize = seriesPath.getMeasurementList().size();
  }

  @Override
  protected void calculateNextAggregationResult() {
    try {
      if (calcFromCacheData()) {
        updateResultTsBlock();
        return;
      }

      // read page data firstly
      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return;
      }

      // read chunk data secondly
      if (readAndCalcFromChunk()) {
        updateResultTsBlock();
        return;
      }

      // read from file first
      while (seriesScanUtil.hasNextFile()) {
        if (canUseCurrentFileStatistics()) {
          Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();
          if (fileTimeStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              updateResultTsBlock();
              return;
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
            calcFromStatisticsArray(statisticsList);
            seriesScanUtil.skipCurrentFile();
            if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
              break;
            } else {
              continue;
            }
          }
        }

        // read chunk
        if (readAndCalcFromChunk()) {
          updateResultTsBlock();
          return;
        }
      }

      updateResultTsBlock();
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  private void calcFromStatisticsArray(Statistics[] statistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(statistics);
    }
  }

  private boolean readAndCalcFromPage() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
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
          calcFromStatisticsArray(statisticsList);
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
      TsBlockSingleColumnIterator tsBlockIterator = tsBlock.getTsBlockSingleColumnIterator();
      if (tsBlockIterator == null || !tsBlockIterator.hasNext()) {
        continue;
      }

      // stop calc and cached current batchData
      if (ascending && tsBlockIterator.currentTime() > curTimeRange.getMax()) {
        cachedData = tsBlock;
        return true;
      }

      // calc from batch data
      calcFromBatch(tsBlock);

      // judge whether the calculation finished
      boolean isTsBlockOutOfBound =
          ascending
              ? tsBlock.getEndTime() > curTimeRange.getMax()
              : tsBlock.getEndTime() < curTimeRange.getMin();
      if (isAllAggregatorsHasFinalResult(aggregators) || isTsBlockOutOfBound) {
        return true;
      }
    }
    return false;
  }

  private boolean readAndCalcFromChunk() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
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
          calcFromStatisticsArray(statisticsList);
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

  private boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileTimeStatistics();
    return !seriesScanUtil.isFileOverlapped()
        && fileStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentFileModified();
  }

  private boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkTimeStatistics();
    return !seriesScanUtil.isChunkOverlapped()
        && chunkStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentChunkModified();
  }

  private boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageTimeStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
        && currentPageStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentPageModified();
  }
}
