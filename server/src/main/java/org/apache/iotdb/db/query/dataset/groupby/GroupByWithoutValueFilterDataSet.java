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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

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
      throws StorageEngineException, QueryProcessException {
    IExpression expression = groupByTimePlan.getExpression();

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    if (timeFilter == null) {
      throw new QueryProcessException("TimeFilter cannot be null in GroupBy query.");
    }

    List<VirtualStorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));

    // init resultIndexes, group aligned series
    pathToAggrIndexesMap = MetaUtils.groupAggregationsBySeries(paths);
    alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    try {
      // init GroupByExecutor for non-aligned series
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        MeasurementPath path = (MeasurementPath) entry.getKey();
        List<Integer> indexes = entry.getValue();
        if (!pathExecutors.containsKey(path)) {
          pathExecutors.put(
              path,
              getGroupByExecutor(
                  path,
                  groupByTimePlan.getAllMeasurementsInDevice(path.getDevice()),
                  context,
                  timeFilter.copy(),
                  null,
                  ascending));
        }
        for (int index : indexes) {
          AggregateResult aggrResult =
              AggregateResultFactory.getAggrResultByName(
                  groupByTimePlan.getDeduplicatedAggregations().get(index),
                  path.getSeriesType(),
                  ascending);
          pathExecutors.get(path).addAggregateResult(aggrResult);
        }
      }
      // init GroupByExecutor for aligned series
      for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
          alignedPathToAggrIndexesMap.entrySet()) {
        AlignedPath path = entry.getKey();
        List<List<Integer>> indexesList = entry.getValue();
        if (!alignedPathExecutors.containsKey(path)) {
          alignedPathExecutors.put(
              path, getAlignedGroupByExecutor(path, context, timeFilter.copy(), null, ascending));
        }
        for (int i = 0; i < path.getMeasurementList().size(); i++) {
          List<AggregateResult> aggrResultList = new ArrayList<>();
          for (int index : indexesList.get(i)) {
            AggregateResult aggrResult =
                AggregateResultFactory.getAggrResultByName(
                    groupByTimePlan.getDeduplicatedAggregations().get(index),
                    path.getSchemaList().get(i).getType(),
                    ascending);
            aggrResultList.add(aggrResult);
          }
          alignedPathExecutors.get(path).addAggregateResult(aggrResultList);
        }
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() " + "in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }

    curAggregateResults = getNextAggregateResult();
    for (AggregateResult res : curAggregateResults) {
      if (res == null) {
        record.addField(null);
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  private AggregateResult[] getNextAggregateResult() throws IOException {
    curAggregateResults = new AggregateResult[paths.size()];
    try {
      // get aggregate results of non-aligned series
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        MeasurementPath path = (MeasurementPath) entry.getKey();
        List<Integer> indexes = entry.getValue();
        GroupByExecutor groupByExecutor = pathExecutors.get(path);
        List<AggregateResult> aggregations = groupByExecutor.calcResult(curStartTime, curEndTime);
        for (int i = 0; i < aggregations.size(); i++) {
          int resultIndex = indexes.get(i);
          curAggregateResults[resultIndex] = aggregations.get(i);
        }
      }
      // get aggregate results of aligned series
      for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
          alignedPathToAggrIndexesMap.entrySet()) {
        AlignedPath path = entry.getKey();
        List<List<Integer>> indexesList = entry.getValue();
        AlignedGroupByExecutor groupByExecutor = alignedPathExecutors.get(path);
        List<List<AggregateResult>> aggregationsList =
            groupByExecutor.calcAlignedResult(curStartTime, curEndTime);
        for (int i = 0; i < path.getMeasurementList().size(); i++) {
          List<AggregateResult> aggregations = aggregationsList.get(i);
          List<Integer> indexes = indexesList.get(i);
          for (int j = 0; j < aggregations.size(); j++) {
            int resultIndex = indexes.get(j);
            curAggregateResults[resultIndex] = aggregations.get(j);
          }
        }
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
