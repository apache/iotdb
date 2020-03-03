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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  /**
   * Merges same series to one map. For example: Given: paths: s1, s2, s3, s1 and aggregations:
   * count, sum, count, sum seriesMap: s1 -> 0, 3; s2 -> 1; s3 -> 2
   */
  private Map<Path, List<Integer>> pathToAggrIndexesMap;

  /**
   * Maps path and its aggregate reader
   */
  private Map<Path, IAggregateReader> aggregateReaders;
  private BatchData[] cachedBatchDataList;
  private GroupByPlan groupByPlan;
  private int remainingToCalculate;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);

    this.pathToAggrIndexesMap = new HashMap<>();
    this.aggregateReaders = new HashMap<>();
    this.cachedBatchDataList = new BatchData[paths.size()];
    initGroupBy(context, groupByPlan);
  }

  /**
   * init reader and aggregate function.
   */
  private void initGroupBy(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    IExpression expression = groupByPlan.getExpression();
    this.groupByPlan = groupByPlan;

    Filter timeFilter = null;
    // init reader
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      List<Integer> indexList = pathToAggrIndexesMap
          .computeIfAbsent(path, key -> new ArrayList<>());
      indexList.add(i);
      if (!aggregateReaders.containsKey(path)) {

        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(path, context, timeFilter);
        // update filter by TTL
        timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

        IAggregateReader seriesReader = new SeriesAggregateReader(path, dataTypes.get(i), context,
            queryDataSource, timeFilter, null, null);
        aggregateReaders.put(path, seriesReader);
      }
    }
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next() "
          + "in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record = new RowRecord(curStartTime);
    AggregateResult[] aggregateResultList = new AggregateResult[paths.size()];
    for (Map.Entry<Path, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
      List<AggregateResult> aggregateResults;
      try {
        aggregateResults = nextIntervalAggregation(entry);
      } catch (QueryProcessException e) {
        throw new IOException(e);
      }
      int index = 0;
      for (int i : entry.getValue()) {
        aggregateResultList[i] = aggregateResults.get(index);
        index++;
      }
    }
    if (aggregateResultList.length == 0) {
      record.addField(new Field(null));
    } else {
      for (AggregateResult res : aggregateResultList) {
        record.addField(res.getResult(), res.getResultDataType());
      }
    }
    return record;
  }

  /**
   * calculate the group by result of one series
   *
   * @param pathToAggrIndexes entry of path to aggregation indexes map
   */
  private List<AggregateResult> nextIntervalAggregation(Map.Entry<Path,
      List<Integer>> pathToAggrIndexes) throws IOException, QueryProcessException {
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    List<Integer> indexList = pathToAggrIndexes.getValue();
    boolean[] isCalculatedList = new boolean[indexList.size()];
    TSDataType tsDataType = groupByPlan.getDeduplicatedDataTypes().get(indexList.get(0));

    aggregateLastBatches(indexList, aggregateResultList, tsDataType, isCalculatedList);
    if (remainingToCalculate == 0) {
      return aggregateResultList;
    }

    TimeRange timeRange = new TimeRange(curStartTime, curEndTime - 1);
    IAggregateReader reader = aggregateReaders.get(pathToAggrIndexes.getKey());
    aggregateFromReader(reader, timeRange, aggregateResultList, isCalculatedList, pathToAggrIndexes);

    return aggregateResultList;
  }

  /**
   * Execute the aggregations using the batches from last execution.
   */
  private void aggregateLastBatches(List<Integer> indexList,
      List<AggregateResult> aggregateResultList, TSDataType tsDataType,
      boolean[] isCalculatedList) throws IOException {
    remainingToCalculate = indexList.size();
    for (int i = 0; i < indexList.size(); i++) {
      int index = indexList.get(i);
      AggregateResult result = AggregateResultFactory
          .getAggrResultByName(groupByPlan.getDeduplicatedAggregations().get(index), tsDataType);
      aggregateResultList.add(result);

      BatchData lastBatch = cachedBatchDataList[index];

      calcBatchData(result, lastBatch);
      if (isEndCalc(result, lastBatch)) {
        isCalculatedList[i] = true;
        remainingToCalculate--;
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  private void aggregateFromReader(IAggregateReader reader, TimeRange timeRange,
      List<AggregateResult> aggregateResultList, boolean[] isCalculatedList, Map.Entry<Path,
      List<Integer>> pathToAggrIndexes)
      throws IOException, QueryProcessException {
    while (reader.hasNextChunk()) {
      // cal by chunk statistics
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        return;
      }

      boolean statisticApplied = aggregateChunkStatistic(reader, timeRange, chunkStatistics,
          aggregateResultList, isCalculatedList);
      if (remainingToCalculate == 0) {
        return;
      } else if (!statisticApplied) {
        aggregatePages(reader, timeRange, aggregateResultList, isCalculatedList, pathToAggrIndexes);
      }
    }
  }

  private void aggregatePages(IAggregateReader reader, TimeRange timeRange,
      List<AggregateResult> aggregateResultList, boolean[] isCalculatedList, Map.Entry<Path,
      List<Integer>> pathToAggrIndexes) throws IOException, QueryProcessException {
    while (reader.hasNextPage()) {
      //cal by page statistics
      Statistics pageStatistics = reader.currentPageStatistics();
      if (pageStatistics.getStartTime() >= curEndTime) {
        return;
      }
      boolean statisticApplied = aggregatePageStatistic(reader, timeRange, pageStatistics,
          aggregateResultList, isCalculatedList);
      if (remainingToCalculate == 0) {
        return;
      } else if (!statisticApplied) {
        aggregateOverlappedPages(reader, aggregateResultList, isCalculatedList, pathToAggrIndexes);
      }
    }
  }

  /**
   * Try aggregating using the next chunk statistic from the reader
   * @param reader
   * @param timeRange
   * @param chunkStatistics
   * @param aggregateResultList
   * @param isCalculatedList
   * @return true if the statistic is applied, false otherwise
   * @throws QueryProcessException
   */
  private boolean aggregateChunkStatistic(IAggregateReader reader, TimeRange timeRange,
      Statistics chunkStatistics, List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedList) throws QueryProcessException {
    if (reader.canUseCurrentChunkStatistics() && timeRange.contains(
       chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
      aggregateStatistic(aggregateResultList, isCalculatedList, chunkStatistics);
      if (remainingToCalculate == 0) {
        return true;
      }
      reader.skipCurrentChunk();
      return true;
    }
    return false;
  }

  /**
   * Try aggregating using the next page statistic from the reader
   * @param reader
   * @param timeRange
   * @param pageStatistics
   * @param aggregateResultList
   * @param isCalculatedList
   * @return true if the statistic is applied, false otherwise
   * @throws QueryProcessException
   */
  private boolean aggregatePageStatistic(IAggregateReader reader, TimeRange timeRange,
      Statistics pageStatistics, List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedList) throws IOException, QueryProcessException {
    if (reader.canUseCurrentPageStatistics() && timeRange.contains(
        pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
      for (int i = 0; i < aggregateResultList.size(); i++) {
        aggregateStatistic(aggregateResultList, isCalculatedList, pageStatistics);
        if (remainingToCalculate == 0) {
          return true;
        }
      }
      reader.skipCurrentPage();
      return true;
    }
    return false;
  }

  private void aggregateOverlappedPages(IAggregateReader reader,
      List<AggregateResult> aggregateResultList, boolean[] isCalculatedList, Map.Entry<Path,
      List<Integer>> pathToAggrIndexes) throws IOException {
    while (reader.hasNextOverlappedPage()) {
      // cal by page data
      BatchData batchData = reader.nextOverlappedPage();
      aggregateBatch(aggregateResultList, isCalculatedList, batchData, pathToAggrIndexes);
    }
  }

  private void aggregateBatch(List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedList, BatchData batchData, Map.Entry<Path,
      List<Integer>> pathToAggrIndexes) throws IOException {
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedList[i]) {
        AggregateResult result = aggregateResultList.get(i);
        calcBatchData(result, batchData);
        int idx = pathToAggrIndexes.getValue().get(i);
        if (batchData.hasCurrent()) {
          cachedBatchDataList[idx] = batchData;
        }
        if (isEndCalc(result, null)) {
          isCalculatedList[i] = true;
          remainingToCalculate--;
          if (remainingToCalculate == 0) {
            break;
          }
        }
      }
    }
  }

  private void aggregateStatistic(List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedList, Statistics statistics) throws QueryProcessException {
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedList[i]) {
        AggregateResult result = aggregateResultList.get(i);
        result.updateResultFromStatistics(statistics);
        if (result.isCalculatedAggregationResult()) {
          isCalculatedList[i] = true;
          remainingToCalculate--;
          if (remainingToCalculate == 0) {
            return;
          }
        }
      }
    }
  }

  private boolean isEndCalc(AggregateResult function, BatchData lastBatch) {
    return (lastBatch != null && lastBatch.hasCurrent() && lastBatch.currentTime() >= curEndTime)
        || function.isCalculatedAggregationResult();
  }

  /**
   * this batchData >= curEndTime
   */
  private void calcBatchData(AggregateResult result, BatchData batchData) throws IOException {
    if (batchData == null || !batchData.hasCurrent()) {
      return;
    }
    while (batchData.hasCurrent() && batchData.currentTime() < curStartTime) {
      // TODO: provide a fast skip in BatchData since it is always ordered
      batchData.next();
    }
    if (batchData.hasCurrent()) {
      result.updateResultFromPageData(batchData, curEndTime);
      // reset batch data for next calculation
      batchData.resetBatchData();
    }
  }
}