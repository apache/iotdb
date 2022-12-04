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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.groupby.AlignedGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.SlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.LocalAlignedGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.LocalGroupByExecutor;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupByWithoutValueFilterDataSet extends GroupByTimeEngineDataSet {

  private static final Logger logger =
      LoggerFactory.getLogger(GroupByWithoutValueFilterDataSet.class);

  protected Map<PartialPath, GroupByExecutor> pathExecutors = new HashMap<>();
  protected Map<AlignedPath, AlignedGroupByExecutor> alignedPathExecutors = new HashMap<>();

  /**
   * non-aligned path -> result index for each aggregation
   *
   * <p>e.g.,
   *
   * <p>deduplicated paths: s1, s2, s1; deduplicated aggregations: count, count, sum
   *
   * <p>s1 -> 0, 2; s2 -> 1
   */
  protected Map<PartialPath, List<Integer>> pathToAggrIndexesMap = new HashMap<>();

  /**
   * aligned path -> result indexes for each aggregation
   *
   * <p>e.g.,
   *
   * <p>deduplicated paths: d1.s1, d1.s2, d1.s1; deduplicated aggregations: count, count, sum
   *
   * <p>d1[s1, s2] -> [[0, 2], [1]]
   */
  protected Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap = new HashMap<>();

  public GroupByWithoutValueFilterDataSet() {}

  /** constructor. */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan) {
    super(context, groupByTimePlan);
  }

  /** init reader and aggregate function. This method should be called once after initializing */
  public void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {}

  @Override
  protected AggregateResult[] getNextAggregateResult() throws IOException {
    curAggregateResults = new AggregateResult[paths.size()];
    for (SlidingWindowGroupByExecutor slidingWindowGroupByExecutor :
        slidingWindowGroupByExecutors) {
      slidingWindowGroupByExecutor.setTimeRange(
          curAggrTimeRange.getMin(), curAggrTimeRange.getMax());
    }
    try {
      while (!isEndCal()) {
        // get pre-aggregate results of non-aligned series
        for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
          MeasurementPath path = (MeasurementPath) entry.getKey();
          List<Integer> indexes = entry.getValue();
          GroupByExecutor groupByExecutor = pathExecutors.get(path);
          List<AggregateResult> aggregations =
              groupByExecutor.calcResult(
                  curPreAggrTimeRange.getMin(), curPreAggrTimeRange.getMax());
          for (int i = 0; i < aggregations.size(); i++) {
            int resultIndex = indexes.get(i);
            slidingWindowGroupByExecutors[resultIndex].update(aggregations.get(i).clone());
          }
        }
        // get pre-aggregate results of aligned series
        for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
            alignedPathToAggrIndexesMap.entrySet()) {
          AlignedPath path = entry.getKey();
          List<List<Integer>> indexesList = entry.getValue();
          AlignedGroupByExecutor groupByExecutor = alignedPathExecutors.get(path);
          List<List<AggregateResult>> aggregationsList =
              groupByExecutor.calcAlignedResult(
                  curPreAggrTimeRange.getMin(), curPreAggrTimeRange.getMax());
          for (int i = 0; i < path.getMeasurementList().size(); i++) {
            List<AggregateResult> aggregations = aggregationsList.get(i);
            List<Integer> indexes = indexesList.get(i);
            for (int j = 0; j < aggregations.size(); j++) {
              int resultIndex = indexes.get(j);
              slidingWindowGroupByExecutors[resultIndex].update(aggregations.get(j).clone());
            }
          }
        }
        updatePreAggrInterval();
      }
      for (int i = 0; i < curAggregateResults.length; i++) {
        curAggregateResults[i] = slidingWindowGroupByExecutors[i].getAggregateResult();
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByWithoutValueFilterDataSet execute has error", e);
      throw new IOException(e.getMessage(), e);
    }
    return curAggregateResults;
  }

  protected GroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> allSensors,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    return new LocalGroupByExecutor(path, allSensors, context, timeFilter, fileFilter, ascending);
  }

  protected AlignedGroupByExecutor getAlignedGroupByExecutor(
      PartialPath path,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    return new LocalAlignedGroupByExecutor(path, context, timeFilter, fileFilter, ascending);
  }
}
