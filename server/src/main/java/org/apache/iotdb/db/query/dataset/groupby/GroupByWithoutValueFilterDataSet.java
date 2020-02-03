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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.PlannerException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.db.query.reader.seriesRelated.ISeriesReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private List<SeriesReader> seriesReaders;
  private List<BatchData> cachedBatchDataList;
  private Filter timeFilter;
  private GroupByPlan groupByPlan;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);

    this.seriesReaders = new ArrayList<>();
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
      SeriesReader seriesReader = new SeriesReader(path, dataTypes.get(i), context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter),
          timeFilter, null);
      seriesReaders.add(seriesReader);
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
    for (int i = 0; i < paths.size(); i++) {
      AggregateResult res;
      try {
        res = nextIntervalAggregation(i);
      } catch (PlannerException e) {
        throw new IOException(e);
      }
      if (res == null) {
        record.addField(new Field(null));
      } else {
        record.addField(res.getResult(), res.getDataType());
      }
    }
    return record;
  }

  /**
   * calculate the group by result of the series indexed by idx.
   *
   * @param idx series id
   */
  private AggregateResult nextIntervalAggregation(int idx) throws IOException, PlannerException {
    ISeriesReader reader = seriesReaders.get(idx);
    AggregateResult result = AggreResultFactory
        .getAggrResultByName(groupByPlan.getDeduplicatedAggregations().get(idx),
            groupByPlan.getDeduplicatedDataTypes().get(idx));

    TimeRange timeRange = new TimeRange(curStartTime, curEndTime - 1);

    BatchData lastBatch = cachedBatchDataList.get(idx);
    calcBatchData(result, lastBatch);
    if (isEndCalc(result, lastBatch)) {
      return result;
    }
    while (reader.hasNextChunk()) {
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        break;
      }
      if (reader.canUseCurrentChunkStatistics() && timeRange.contains(
          new TimeRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime()))) {
        result.updateResultFromStatistics(chunkStatistics);
        if (result.isCalculatedAggregationResult()) {
          break;
        }
        reader.skipCurrentChunk();
        continue;
      }

      while (reader.hasNextPage()) {
        Statistics pageStatistics = reader.currentPageStatistics();
        if (pageStatistics.getStartTime() >= curEndTime) {
          break;
        }
        if (reader.canUseCurrentPageStatistics() && timeRange.contains(
            new TimeRange(pageStatistics.getStartTime(), pageStatistics.getEndTime()))) {
          result.updateResultFromStatistics(pageStatistics);
          if (result.isCalculatedAggregationResult()) {
            break;
          }
          reader.skipCurrentPage();
          continue;
        }
        while (reader.hasNextOverlappedPage()) {
          BatchData batchData = reader.nextOverlappedPage();
          calcBatchData(result, batchData);
          if (batchData.hasCurrent()) {
            cachedBatchDataList.set(idx, batchData);
          }
          if (isEndCalc(result, lastBatch)) {
            break;
          }
        }
      }
    }
    return result;
  }

  private boolean isEndCalc(AggregateResult function, BatchData lastBatch) {
    return (lastBatch != null && lastBatch.hasCurrent() && lastBatch.currentTime() >= curEndTime)
        || function.isCalculatedAggregationResult();
  }

  /**
   * this batchData >= curEndTime
   */
  private void calcBatchData(AggregateResult result, BatchData batchData)
      throws IOException {
    if (batchData == null || !batchData.hasCurrent()) {
      return;
    }
    while (batchData.hasCurrent() && batchData.currentTime() < curStartTime) {
      batchData.next();
    }
    if (batchData.hasCurrent()) {
      result.updateResultFromPageData(batchData, curEndTime);
    }
  }
}