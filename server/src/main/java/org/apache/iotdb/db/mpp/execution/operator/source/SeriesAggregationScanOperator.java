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
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

/**
 * This operator is responsible to do the aggregation calculation for one series based on global
 * time range and time split parameter.
 *
 * <p>Every time next() is invoked, one tsBlock which contains current time window will be returned.
 * In sliding window situation, current time window is a pre-aggregation window. If there is no time
 * split parameter, i.e. aggregation without groupBy, just one tsBlock will be returned.
 */
public class SeriesAggregationScanOperator extends AbstractSeriesAggregationScanOperator {

  public SeriesAggregationScanOperator(
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Set<String> allSensors,
      OperatorContext context,
      List<Aggregator> aggregators,
      Filter timeFilter,
      boolean ascending,
      GroupByTimeParameter groupByTimeParameter) {
    super(
        sourceId,
        seriesPath,
        allSensors,
        context,
        aggregators,
        timeFilter,
        ascending,
        groupByTimeParameter);
  }

  @Override
  protected void calcFromStatistics(Statistics[] statistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(statistics[0]);
    }
  }

  @Override
  protected boolean readAndCalcFromFile() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (canUseCurrentFileStatistics()) {
        Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
        if (fileStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (curTimeRange.contains(fileStatistics.getStartTime(), fileStatistics.getEndTime())) {
          calcFromStatistics(new Statistics[] {fileStatistics});
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

  @Override
  protected boolean readAndCalcFromChunk() throws IOException {
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
          calcFromStatistics(new Statistics[] {chunkStatistics});
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

  @Override
  protected boolean readAndCalcFromPage() throws IOException {
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
          calcFromStatistics(new Statistics[] {pageStatistics});
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

      // stop calc and cached current batchData
      if (ascending
          ? tsBlock.getStartTime() > curTimeRange.getMax()
          : tsBlock.getStartTime() < curTimeRange.getMin()) {
        inputTsBlock = tsBlock;
        return true;
      }

      // calc from raw data
      if (calcFromRawData(tsBlock)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileStatistics();
    return !seriesScanUtil.isFileOverlapped()
        && fileStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentFileModified();
  }

  @Override
  protected boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkStatistics();
    return !seriesScanUtil.isChunkOverlapped()
        && chunkStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentChunkModified();
  }

  @Override
  protected boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
        && currentPageStatistics.containedByTimeFilter(seriesScanUtil.getTimeFilter())
        && !seriesScanUtil.currentPageModified();
  }
}
