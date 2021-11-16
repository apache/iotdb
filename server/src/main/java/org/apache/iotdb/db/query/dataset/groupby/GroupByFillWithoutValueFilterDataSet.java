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
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByFillWithoutValueFilterDataSet extends GroupByFillEngineDataSet {

  private static final Logger logger =
      LoggerFactory.getLogger(GroupByFillWithoutValueFilterDataSet.class);

  // path executors
  private final Map<PartialPath, GroupByExecutor> pathExecutors = new HashMap<>();
  private Map<PartialPath, GroupByExecutor> extraPreviousExecutors = null;
  private Map<PartialPath, GroupByExecutor> extraNextExecutors = null;

  public GroupByFillWithoutValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan) {
    super(context, groupByTimeFillPlan);
  }

  public void init(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException, StorageEngineException {
    initPathExecutors(context, groupByTimeFillPlan);

    initExtraExecutors(context, groupByTimeFillPlan);
    if (extraPreviousExecutors != null) {
      initExtraArrays(extraPreviousValues, extraPreviousTimes, true, extraPreviousExecutors);
    }
    if (extraNextExecutors != null) {
      initExtraArrays(extraNextValues, extraNextTimes, false, extraNextExecutors);
    }

    initCachedTimesAndValues();
  }

  private void initPathExecutors(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException, StorageEngineException {
    IExpression expression = groupByTimeFillPlan.getExpression();
    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    if (timeFilter == null) {
      throw new QueryProcessException("TimeFilter cannot be null in GroupBy query.");
    }
    getGroupByExecutors(pathExecutors, context, groupByTimeFillPlan, timeFilter, ascending);
  }

  protected GroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    return new LocalGroupByExecutor(
        path, allSensors, dataType, context, timeFilter, null, ascending);
  }

  private void getGroupByExecutors(
      Map<PartialPath, GroupByExecutor> extraExecutors,
      QueryContext context,
      GroupByTimeFillPlan groupByTimeFillPlan,
      Filter timeFilter,
      boolean isAscending)
      throws StorageEngineException, QueryProcessException {
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    try {
      // init resultIndexes, group result indexes by path
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        if (!extraExecutors.containsKey(path)) {
          // init GroupByExecutor
          extraExecutors.put(
              path,
              getGroupByExecutor(
                  path,
                  groupByTimeFillPlan.getAllMeasurementsInDevice(path.getDevice()),
                  dataTypes.get(i),
                  context,
                  timeFilter.copy(),
                  isAscending));
        }
        AggregateResult aggregateResult =
            AggregateResultFactory.getAggrResultByName(
                groupByTimeFillPlan.getDeduplicatedAggregations().get(i),
                dataTypes.get(i),
                ascending);
        extraExecutors.get(path).addAggregateResult(aggregateResult);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  /* Init extra path executors to query data outside the original group by query */
  private void initExtraExecutors(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws StorageEngineException, QueryProcessException {
    long minQueryStartTime = Long.MAX_VALUE;
    long maxQueryEndTime = Long.MIN_VALUE;
    this.fillTypes = groupByTimeFillPlan.getFillType();
    for (Map.Entry<TSDataType, IFill> IFillEntry : fillTypes.entrySet()) {
      IFill fill = IFillEntry.getValue();
      if (fill instanceof PreviousFill) {
        fill.convertRange(startTime, endTime);
        minQueryStartTime = Math.min(minQueryStartTime, fill.getQueryStartTime());
      } else if (fill instanceof LinearFill) {
        fill.convertRange(startTime, endTime);
        minQueryStartTime = Math.min(minQueryStartTime, fill.getQueryStartTime());
        maxQueryEndTime = Math.max(maxQueryEndTime, fill.getQueryEndTime());
      }
    }

    if (minQueryStartTime < Long.MAX_VALUE) {
      extraPreviousExecutors = new HashMap<>();

      long queryRange = minQueryStartTime - startTime;
      long extraStartTime, intervalNum;
      if (isSlidingStepByMonth) {
        intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep * MS_TO_MONTH));
        extraStartTime = calcIntervalByMonth(startTime, intervalNum * slidingStep);
        while (extraStartTime < minQueryStartTime) {
          intervalNum += 1;
          extraStartTime = calcIntervalByMonth(startTime, intervalNum * slidingStep);
        }
      } else {
        intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
        extraStartTime = slidingStep * intervalNum + startTime;
      }

      Filter timeFilter = new GroupByFilter(interval, slidingStep, extraStartTime, startTime);
      getGroupByExecutors(extraPreviousExecutors, context, groupByTimeFillPlan, timeFilter, false);
    }

    if (maxQueryEndTime > Long.MIN_VALUE) {
      extraNextExecutors = new HashMap<>();
      Pair<Long, Long> lastTimeRange = getLastTimeRange();
      lastTimeRange = getNextTimeRange(lastTimeRange.left, true, false);
      Filter timeFilter =
          new GroupByFilter(interval, slidingStep, lastTimeRange.left, maxQueryEndTime);
      getGroupByExecutors(extraNextExecutors, context, groupByTimeFillPlan, timeFilter, true);
    }
  }

  private void initExtraArrays(
      Object[] extraValues,
      long[] extraTimes,
      boolean isExtraPrevious,
      Map<PartialPath, GroupByExecutor> extraExecutors)
      throws QueryProcessException {
    for (int pathId = 0; pathId < deduplicatedPaths.size(); pathId++) {
      GroupByExecutor executor = extraExecutors.get(deduplicatedPaths.get(pathId));
      List<Integer> Indexes = resultIndexes.get(deduplicatedPaths.get(pathId));

      Pair<Long, Long> extraTimeRange;
      if (isExtraPrevious) {
        extraTimeRange = getFirstTimeRange();
      } else {
        extraTimeRange = getLastTimeRange();
      }

      extraTimeRange = getNextTimeRange(extraTimeRange.left, !isExtraPrevious, false);
      try {
        while (pathHasExtra(pathId, isExtraPrevious, extraTimeRange.left)) {
          List<AggregateResult> aggregations =
              executor.calcResult(extraTimeRange.left, extraTimeRange.right);
          if (!resultIsNull(aggregations)) {
            // we check extra time range in single path together,
            // thus the extra result will be cached together
            for (int i = 0; i < aggregations.size(); i++) {
              if (extraValues[Indexes.get(i)] == null) {
                extraValues[Indexes.get(i)] = aggregations.get(i).getResult();
                extraTimes[Indexes.get(i)] = extraTimeRange.left;
              }
            }
          }

          extraTimeRange = getNextTimeRange(extraTimeRange.left, !isExtraPrevious, false);
        }
      } catch (IOException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
  }

  @Override
  protected void pathGetNext(int pathId) throws IOException {
    GroupByExecutor executor = pathExecutors.get(deduplicatedPaths.get(pathId));
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));

    cacheSlideNext(resultIndex);

    List<AggregateResult> aggregateResults;
    try {
      // get second not null aggregate results
      aggregateResults = executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
      hasCachedQueryInterval[pathId] = false;
      while (resultIsNull(aggregateResults) && pathHasNext(pathId)) {
        aggregateResults = executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
        hasCachedQueryInterval[pathId] = false;
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByFillWithoutValueFilterDataSet execute has error: ", e);
      throw new IOException(e.getMessage(), e);
    }

    cacheFillNext(pathId, aggregateResults, resultIndex);

    hasCachedQueryInterval[pathId] = false;
  }
}
