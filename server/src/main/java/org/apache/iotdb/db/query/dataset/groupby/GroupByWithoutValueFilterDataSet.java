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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
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
  private List<BatchData> cachedBatchDataList;
  private Filter timeFilter;
  private GroupByPlan groupByPlan;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);

    this.pathToAggrIndexesMap = new HashMap<>();
    this.aggregateReaders = new HashMap<>();
    this.timeFilter = null;
    this.cachedBatchDataList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      cachedBatchDataList.add(null);
    }
    initGroupBy(context, groupByPlan);
  }

  /**
   * init reader and aggregate function.
   */
  private void initGroupBy(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    IExpression expression = groupByPlan.getExpression();
    this.groupByPlan = groupByPlan;

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
        IAggregateReader seriesReader = new SeriesAggregateReader(path, dataTypes.get(i), context,
            QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter),
            timeFilter, null);
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
        record.addField(res.getResult(), res.getDataType());
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
    List<BatchData> batchDataList = new ArrayList<>();
    List<Boolean> isCalculatedList = new ArrayList<>();
    List<Integer> indexList = pathToAggrIndexes.getValue();

    int remainingToCalculate = indexList.size();
    TSDataType tsDataType = groupByPlan.getDeduplicatedDataTypes().get(indexList.get(0));

    for (int index : indexList) {
      AggregateResult result = AggreResultFactory
          .getAggrResultByName(groupByPlan.getDeduplicatedAggregations().get(index), tsDataType);
      aggregateResultList.add(result);

      BatchData lastBatch = cachedBatchDataList.get(index);
      batchDataList.add(lastBatch);

      calcBatchData(result, lastBatch);
      if (isEndCalc(result, lastBatch)) {
        isCalculatedList.add(true);
        remainingToCalculate--;
        if (remainingToCalculate == 0) {
          return aggregateResultList;
        }
      } else {
        isCalculatedList.add(false);
      }
    }
    TimeRange timeRange = new TimeRange(curStartTime, curEndTime - 1);
    IAggregateReader reader = aggregateReaders.get(pathToAggrIndexes.getKey());

    while (reader.hasNextChunk()) {
      // cal by chunk statistics
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        return aggregateResultList;
      }
      if (reader.canUseCurrentChunkStatistics() && timeRange.contains(
          new TimeRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime()))) {
        for (int i = 0; i < aggregateResultList.size(); i++) {
          if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
            AggregateResult result = aggregateResultList.get(i);
            result.updateResultFromStatistics(chunkStatistics);
            if (result.isCalculatedAggregationResult()) {
              isCalculatedList.set(i, true);
              remainingToCalculate--;
              if (remainingToCalculate == 0) {
                return aggregateResultList;
              }
            }
          }
        }
        reader.skipCurrentChunk();
        continue;
      }

      while (reader.hasNextPage()) {
        //cal by page statistics
        Statistics pageStatistics = reader.currentPageStatistics();
        if (pageStatistics.getStartTime() >= curEndTime) {
          return aggregateResultList;
        }
        if (reader.canUseCurrentPageStatistics() && timeRange.contains(
            new TimeRange(pageStatistics.getStartTime(), pageStatistics.getEndTime()))) {
          for (int i = 0; i < aggregateResultList.size(); i++) {
            if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
              AggregateResult result = aggregateResultList.get(i);
              result.updateResultFromStatistics(pageStatistics);
              if (result.isCalculatedAggregationResult()) {
                isCalculatedList.set(i, true);
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return aggregateResultList;
                }
              }
            }
          }
          reader.skipCurrentPage();
          continue;
        }
        while (reader.hasNextOverlappedPage()) {
          // cal by page data
          BatchData batchData = reader.nextOverlappedPage();
          for (int i = 0; i < aggregateResultList.size(); i++) {
            if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
              AggregateResult result = aggregateResultList.get(i);
              calcBatchData(result, batchData);
              int idx = pathToAggrIndexes.getValue().get(i);
              if (batchData.hasCurrent()) {
                cachedBatchDataList.set(idx, batchData);
              }
              if (isEndCalc(result, batchDataList.get(i))) {
                isCalculatedList.set(i, true);
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  break;
                }
              }
            }
          }
        }
      }
    }
    return aggregateResultList;
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
      batchData.next();
    }
    if (batchData.hasCurrent()) {
      result.updateResultFromPageData(batchData, curEndTime);
      // reset batch data for next calculation
      batchData.resetBatchData();
    }
  }
}