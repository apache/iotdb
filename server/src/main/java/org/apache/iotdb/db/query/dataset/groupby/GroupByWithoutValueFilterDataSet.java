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
import java.util.Map.Entry;
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
import org.apache.iotdb.tsfile.utils.Pair;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private Map<Path, Aggregations> pathAggregationsMap = new HashMap<>();

  private GroupByPlan groupByPlan;

  private TimeRange timeRange;

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByPlan groupByPlan)
      throws StorageEngineException {
    super(context, groupByPlan);

    initGroupBy(context, groupByPlan);
  }

  private class Aggregations {

    private IAggregateReader reader;
    private BatchData preCachedData;
    private List<Pair<AggregateResult, Integer>> results = new ArrayList<>();

    public Aggregations(Path path, TSDataType dataType, QueryContext context,
        QueryDataSource dataSource, Filter timeFilter) {
      this.reader = new SeriesAggregateReader(path, dataType, context,
          dataSource, timeFilter, null, null);
      this.preCachedData = null;
    }

    public IAggregateReader getReader() {
      return reader;
    }

    public void addAggregateResult(AggregateResult aggrResult, int index) {
      results.add(new Pair<>(aggrResult, index));
    }

    public List<Pair<AggregateResult, Integer>> getResults() {
      return results;
    }

    public boolean isEndCalc() {
      for (Pair<AggregateResult, Integer> result : results) {
        if (result.left.isCalculatedAggregationResult() == false) {
          return false;
        }
      }
      return true;
    }

    public boolean calcFromCacheData() throws IOException {
      calcFromBatch(preCachedData);
      if ((preCachedData != null && preCachedData.getMaxTimestamp() >= curEndTime) || isEndCalc()) {
        return true;
      }
      return false;
    }

    public void calcFromBatch(BatchData batchData) throws IOException {
      if (batchData == null
          || !batchData.hasCurrent()
          || batchData.getMaxTimestamp() < curStartTime) {
        return;
      }
      System.out
          .println("batch 间隔[" + batchData.currentTime() + "-" + batchData.getMaxTimestamp() + "]");
      // timeout
      if (batchData.currentTime() >= curEndTime) {
        return;
      }

      for (Pair<AggregateResult, Integer> result : results) {
        //cacl is compile
        if (result.left.isCalculatedAggregationResult()) {
          continue;
        }
        //lazy reset batch data for next calculation
        batchData.resetBatchData();

        while (batchData.hasCurrent() && batchData.currentTime() < curStartTime) {
          batchData.next();
        }
        if (batchData.hasCurrent()) {
          result.left.updateResultFromPageData(batchData, curEndTime);
        }
      }
      if (batchData.getMaxTimestamp() >= curEndTime) {
        preCachedData = batchData;
      }
    }

    public void calcFromStatistics(Statistics pageStatistics)
        throws QueryProcessException {
      for (Pair<AggregateResult, Integer> result : results) {
        //cacl is compile
        if (result.left.isCalculatedAggregationResult()) {
          continue;
        }
        result.left.updateResultFromStatistics(pageStatistics);
      }
    }

    public void initAggregateResults() {
      for (Pair<AggregateResult, Integer> result : results) {
        result.left.reset();
      }
    }
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

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context, timeFilter);
      // update filter by TTL
      timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

      pathAggregationsMap.putIfAbsent(path,
          new Aggregations(path, dataTypes.get(i), context, queryDataSource, timeFilter));

      AggregateResult aggrResult = AggregateResultFactory
          .getAggrResultByName(groupByPlan.getDeduplicatedAggregations().get(i),
              dataTypes.get(i));
      pathAggregationsMap.get(path).addAggregateResult(aggrResult, i);
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
    timeRange = new TimeRange(curStartTime, curEndTime - 1);

    AggregateResult[] fields = new AggregateResult[paths.size()];
    for (Entry<Path, Aggregations> pathAggregations : pathAggregationsMap.entrySet()) {
      pathAggregations.getValue().initAggregateResults();
      try {
        Aggregations aggregations = aggregateOneSeries(pathAggregations);
        for (int i = 0; i < aggregations.getResults().size(); i++) {
          fields[aggregations.getResults().get(i).right] = aggregations.getResults().get(i).left;
        }
      } catch (QueryProcessException e) {
        throw new IOException(e);
      }
    }

    for (AggregateResult res : fields) {
      if (res == null) {
        record.addField(new Field(null));
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  private Aggregations aggregateOneSeries(Entry<Path, Aggregations> pathAggregations)
      throws IOException, QueryProcessException {
    Aggregations aggregations = pathAggregations.getValue();
    if (aggregations.calcFromCacheData()) {
      return aggregations;
    }

    IAggregateReader reader = aggregations.getReader();
    //read overlapped data firstly
    if (readAndCalcOverlappedPage(aggregations, reader)) {
      return aggregations;
    }

    //read chunk finally
    while (reader.hasNextChunk()) {
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        return aggregations;
      }
      if (reader.canUseCurrentChunkStatistics() && timeRange.contains(
          new TimeRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime()))) {
        aggregations.calcFromStatistics(chunkStatistics);
        reader.skipCurrentChunk();
        continue;
      }
      if (readAndCalcOverlappedPage(aggregations, reader)) {
        return aggregations;
      }
    }
    return aggregations;
  }

  private boolean readAndCalcOverlappedPage(Aggregations aggregations, IAggregateReader reader)
      throws IOException, QueryProcessException {
    while (reader.hasNextPage()) {
      Statistics pageStatistics = reader.currentPageStatistics();
      if (pageStatistics.getStartTime() >= curEndTime) {
        return true;
      }
      if (reader.canUseCurrentPageStatistics() && timeRange.contains(
          new TimeRange(pageStatistics.getStartTime(), pageStatistics.getEndTime()))) {
        aggregations.calcFromStatistics(pageStatistics);
        reader.skipCurrentPage();
        if (aggregations.isEndCalc()) {
          return true;
        }
        continue;
      }
      BatchData batchData = reader.nextPage();
      if (batchData == null || !batchData.hasCurrent()) {
        continue;
      }
      // stop calc
      if (batchData.currentTime() >= curEndTime) {
        aggregations.preCachedData = batchData;
        return true;
      }

      aggregations.calcFromBatch(batchData);
      if (aggregations.isEndCalc() || batchData.currentTime() >= curEndTime) {
        return true;
      }
    }
    return false;
  }
}