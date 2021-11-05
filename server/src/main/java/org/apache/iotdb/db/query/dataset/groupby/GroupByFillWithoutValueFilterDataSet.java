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
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.executor.fill.ValueFill;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class GroupByFillWithoutValueFilterDataSet extends GroupByWithoutValueFilterDataSet {

  private Map<TSDataType, IFill> fillTypes;
  private final List<PartialPath> deduplicatedPaths;
  private final List<String> aggregations;
  private Map<PartialPath, GroupByExecutor> extraPreviousExecutors = null;
  private Map<PartialPath, GroupByExecutor> extraNextExecutors = null;

  // the extra previous means first not null value before startTime
  // used to fill result before the first not null data
  private Object[] extraPreviousValues;
  private long[] extraPreviousTimes;

  // the previous value for each time series, which means
  // first not null value GEQ curStartTime in order asc
  // second not null value GEQ curStartTime in order desc
  private Object[] previousValues;
  private long[] previousTimes;

  // the extra next means first not null value after endTime
  // used to fill result after the last not null data
  private Object[] extraNextValues;
  private long[] extraNextTimes;

  // the next value for each time series, which means
  // first not null value LEQ curStartTime in order desc
  // second not null value LEQ curStartTime in order asc
  private Object[] nextValues;
  private long[] nextTimes;

  // the result datatype for each time series
  private TSDataType[] resultDataType;

  // the next query time range of each path
  private long[] queryStartTimes;
  private long[] queryEndTimes;
  private long[] queryIntervalTimes;
  private boolean[] hasCachedQueryInterval;

  public GroupByFillWithoutValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException, StorageEngineException {
    super(context, groupByTimeFillPlan);
    this.aggregations = groupByTimeFillPlan.getDeduplicatedAggregations();

    this.deduplicatedPaths = new ArrayList<>();
    for (Path path : paths) {
      PartialPath partialPath = (PartialPath) path;
      if (!deduplicatedPaths.contains(partialPath)) {
        deduplicatedPaths.add(partialPath);
      }
    }

    initArrays();
    initExtraExecutors(context, groupByTimeFillPlan);
    if (extraPreviousExecutors != null) {
      initExtraArrays(extraPreviousValues, extraPreviousTimes, true, extraPreviousExecutors);
    }
    if (extraNextExecutors != null) {
      initExtraArrays(extraNextValues, extraNextTimes, false, extraNextExecutors);
    }
    initCachedTimesAndValues();
  }

  private void initArrays() {
    extraPreviousValues = new Object[aggregations.size()];
    extraPreviousTimes = new long[aggregations.size()];
    previousValues = new Object[aggregations.size()];
    previousTimes = new long[aggregations.size()];
    extraNextValues = new Object[aggregations.size()];
    extraNextTimes = new long[aggregations.size()];
    nextValues = new Object[aggregations.size()];
    nextTimes = new long[aggregations.size()];
    Arrays.fill(extraPreviousValues, null);
    Arrays.fill(extraPreviousTimes, Long.MIN_VALUE);
    Arrays.fill(previousValues, null);
    Arrays.fill(previousTimes, Long.MIN_VALUE);
    Arrays.fill(extraNextValues, null);
    Arrays.fill(extraNextTimes, Long.MAX_VALUE);
    Arrays.fill(nextValues, null);
    Arrays.fill(nextTimes, Long.MAX_VALUE);

    queryStartTimes = new long[paths.size()];
    queryEndTimes = new long[paths.size()];
    queryIntervalTimes = new long[paths.size()];
    hasCachedQueryInterval = new boolean[paths.size()];
    resultDataType = new TSDataType[aggregations.size()];
    Arrays.fill(queryStartTimes, curStartTime);
    Arrays.fill(queryEndTimes, curEndTime);
    Arrays.fill(queryIntervalTimes, intervalTimes);
    Arrays.fill(hasCachedQueryInterval, true);
    for (PartialPath deduplicatedPath : deduplicatedPaths) {
      List<Integer> indexes = resultIndexes.get(deduplicatedPath);
      for (int index : indexes) {
        switch (aggregations.get(index)) {
          case "avg":
          case "sum":
            resultDataType[index] = TSDataType.DOUBLE;
            break;
          case "count":
          case "max_time":
          case "min_time":
            resultDataType[index] = TSDataType.INT64;
            break;
          case "first_value":
          case "last_value":
          case "max_value":
          case "min_value":
            resultDataType[index] = dataTypes.get(index);
            break;
        }
      }
    }
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
                  null,
                  isAscending));
        }
        AggregateResult aggrResult =
            AggregateResultFactory.getAggrResultByName(
                groupByTimeFillPlan.getDeduplicatedAggregations().get(i),
                dataTypes.get(i),
                ascending);
        extraExecutors.get(path).addAggregateResult(aggrResult);
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
        extraStartTime = calcIntervalByMonth(intervalNum * slidingStep);
        while (extraStartTime < minQueryStartTime) {
          intervalNum += 1;
          extraStartTime = calcIntervalByMonth(intervalNum * slidingStep);
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
      lastTimeRange = getNextTimeRange(lastTimeRange.left, true, intervalTimes + 1, false);
      Filter timeFilter =
          new GroupByFilter(interval, slidingStep, lastTimeRange.left, maxQueryEndTime);
      getGroupByExecutors(extraNextExecutors, context, groupByTimeFillPlan, timeFilter, true);
    }
  }

  /* check if specified path has next extra range */
  private boolean pathHasExtra(int pathId, boolean isExtraPrevious, long extraStartTime) {
    List<Integer> Indexes = resultIndexes.get(deduplicatedPaths.get(pathId));
    for (int resultIndex : Indexes) {
      if (isExtraPrevious && extraPreviousValues[resultIndex] != null) {
        continue;
      } else if (!isExtraPrevious && extraNextValues[resultIndex] != null) {
        continue;
      }

      IFill fill = fillTypes.get(resultDataType[resultIndex]);
      if (fill == null) {
        continue;
      }
      if (fill instanceof PreviousFill && isExtraPrevious) {
        if (fill.getQueryStartTime() <= extraStartTime) {
          return true;
        }
      } else if (fill instanceof LinearFill) {
        if (isExtraPrevious) {
          if (fill.getQueryStartTime() <= extraStartTime) {
            return true;
          }
        } else {
          if (extraStartTime < fill.getQueryEndTime()) {
            return true;
          }
        }
      }
    }

    return false;
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

      long intervalNums = intervalTimes + (isExtraPrevious ? -1 : 1);
      Pair<Long, Long> extraTimeRange;
      if (isExtraPrevious) {
        extraTimeRange = getFirstTimeRange();
      } else {
        extraTimeRange = getLastTimeRange();
      }

      extraTimeRange = getNextTimeRange(extraTimeRange.left, !isExtraPrevious, intervalNums, false);
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

          intervalNums += isExtraPrevious ? -1 : 1;
          extraTimeRange =
              getNextTimeRange(extraTimeRange.left, !isExtraPrevious, intervalNums, false);
        }
      } catch (IOException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
  }

  private boolean pathHasNext(int pathId) {
    // has cached
    if (hasCachedQueryInterval[pathId]) {
      return true;
    }

    // for group by natural months addition
    queryIntervalTimes[pathId] += ascending ? 1 : -1;

    // find the next aggregation interval
    Pair<Long, Long> nextTimeRange =
        getNextTimeRange(queryStartTimes[pathId], ascending, queryIntervalTimes[pathId], true);
    if (nextTimeRange == null) {
      return false;
    }
    queryStartTimes[pathId] = nextTimeRange.left;
    queryEndTimes[pathId] = nextTimeRange.right;

    hasCachedQueryInterval[pathId] = true;
    return true;
  }

  /* If result is null or CountAggrResult is 0, then result is NULL */
  private boolean resultIsNull(List<AggregateResult> aggregateResults) {
    AggregateResult result = aggregateResults.get(0);
    if (result.getResult() == null) {
      return true;
    } else {
      return result instanceof CountAggrResult && (long) result.getResult() == 0;
    }
  }

  private void pathGetNext(int pathId) throws IOException {
    GroupByExecutor executor = pathExecutors.get(deduplicatedPaths.get(pathId));
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));

    // Slide value and time
    pathSlideNext(pathId);

    List<AggregateResult> aggregations;
    try {
      // get second not null aggregate results
      aggregations = executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
      hasCachedQueryInterval[pathId] = false;
      while (resultIsNull(aggregations) && pathHasNext(pathId)) {
        aggregations = executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
        hasCachedQueryInterval[pathId] = false;
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByFillWithoutValueFilterDataSet execute has error: ", e);
      throw new IOException(e.getMessage(), e);
    }

    if (resultIsNull(aggregations)) {
      pathSlide(pathId);
    } else {
      for (int i = 0; i < aggregations.size(); i++) {
        int Index = resultIndex.get(i);
        if (ascending) {
          nextValues[Index] = aggregations.get(i).getResult();
          nextTimes[Index] = queryStartTimes[pathId];
        } else {
          previousValues[Index] = aggregations.get(i).getResult();
          previousTimes[Index] = queryStartTimes[pathId];
        }
      }
    }

    hasCachedQueryInterval[pathId] = false;
  }

  private void pathSlideNext(int pathId) {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));
    if (ascending) {
      for (int resultId : resultIndex) {
        previousValues[resultId] = nextValues[resultId];
        previousTimes[resultId] = nextTimes[resultId];
        nextValues[resultId] = null;
        nextTimes[resultId] = Long.MAX_VALUE;
      }
    } else {
      for (int resultId : resultIndex) {
        nextValues[resultId] = previousValues[resultId];
        nextTimes[resultId] = previousTimes[resultId];
        previousValues[resultId] = null;
        previousTimes[resultId] = Long.MIN_VALUE;
      }
    }
  }

  private void pathSlideExtra(int pathId) {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));
    if (ascending) {
      for (int Index : resultIndex) {
        nextValues[Index] = extraNextValues[Index];
        nextTimes[Index] = extraNextTimes[Index];
      }
    } else {
      for (int Index : resultIndex) {
        previousValues[Index] = extraPreviousValues[Index];
        previousTimes[Index] = extraPreviousTimes[Index];
      }
    }
  }

  private void pathSlide(int pathId) throws IOException {
    if (pathHasNext(pathId)) {
      pathGetNext(pathId);
    } else {
      pathSlideExtra(pathId);
    }
  }

  /* Cache the previous and next query data before group by fill query */
  private void initCachedTimesAndValues() throws QueryProcessException {
    for (int pathId = 0; pathId < deduplicatedPaths.size(); pathId++) {
      try {
        pathSlide(pathId);
        pathSlide(pathId);
      } catch (IOException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
  }

  private void fillRecord(
      int resultId, RowRecord record, Pair<Long, Object> beforePair, Pair<Long, Object> afterPair)
      throws IOException {
    // Don't fill count aggregation
    if (Objects.equals(aggregations.get(resultId), "count")) {
      record.addField((long) 0, TSDataType.INT64);
      return;
    }

    IFill fill = fillTypes.get(resultDataType[resultId]);
    if (fill == null) {
      record.addField(null);
      return;
    }

    if (fill instanceof PreviousFill) {
      if (beforePair.right != null
          && (fill.getBeforeRange() == -1
              || fill.insideBeforeRange(beforePair.left, record.getTimestamp()))
          && ((!((PreviousFill) fill).isUntilLast())
              || (afterPair.right != null && afterPair.left < endTime))) {
        record.addField(beforePair.right, resultDataType[resultId]);
      } else {
        record.addField(null);
      }
    } else if (fill instanceof LinearFill) {
      LinearFill linearFill = new LinearFill();
      if (beforePair.right != null
          && afterPair.right != null
          && (fill.getBeforeRange() == -1
              || fill.insideBeforeRange(beforePair.left, record.getTimestamp()))
          && (fill.getAfterRange() == -1
              || fill.insideAfterRange(afterPair.left, record.getTimestamp()))) {
        try {
          TimeValuePair filledPair =
              linearFill.averageWithTimeAndDataType(
                  new TimeValuePair(
                      beforePair.left,
                      TsPrimitiveType.getByType(resultDataType[resultId], beforePair.right)),
                  new TimeValuePair(
                      afterPair.left,
                      TsPrimitiveType.getByType(resultDataType[resultId], afterPair.right)),
                  curStartTime,
                  resultDataType[resultId]);
          record.addField(filledPair.getValue().getValue(), resultDataType[resultId]);
        } catch (UnSupportedFillTypeException e) {
          record.addField(null);
          throw new IOException(e);
        }
      } else {
        record.addField(null);
      }
    } else if (fill instanceof ValueFill) {
      try {
        TimeValuePair filledPair = fill.getFillResult();
        record.addField(filledPair.getValue().getValue(), resultDataType[resultId]);
      } catch (QueryProcessException | StorageEngineException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() "
              + "in GroupByFillWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record = new RowRecord(curStartTime);

    boolean[] pathNeedSlide = new boolean[previousTimes.length];
    Arrays.fill(pathNeedSlide, false);
    for (int resultId = 0; resultId < previousTimes.length; resultId++) {
      if (previousTimes[resultId] == curStartTime) {
        record.addField(previousValues[resultId], resultDataType[resultId]);
        if (!ascending) {
          pathNeedSlide[resultId] = true;
        }
      } else if (nextTimes[resultId] == curStartTime) {
        record.addField(nextValues[resultId], resultDataType[resultId]);
        if (ascending) {
          pathNeedSlide[resultId] = true;
        }
      } else if (previousTimes[resultId] < curStartTime && curStartTime < nextTimes[resultId]) {
        fillRecord(
            resultId,
            record,
            new Pair<>(previousTimes[resultId], previousValues[resultId]),
            new Pair<>(nextTimes[resultId], nextValues[resultId]));
      } else if (curStartTime < previousTimes[resultId]) {
        fillRecord(
            resultId,
            record,
            new Pair<>(extraPreviousTimes[resultId], extraPreviousValues[resultId]),
            new Pair<>(previousTimes[resultId], previousValues[resultId]));
      } else if (nextTimes[resultId] < curStartTime) {
        fillRecord(
            resultId,
            record,
            new Pair<>(nextTimes[resultId], nextValues[resultId]),
            new Pair<>(extraNextTimes[resultId], extraNextValues[resultId]));
      }
    }

    // Slide paths
    // the aggregation results of one path are either all null or all not null,
    // thus slide all results together
    for (int pathId = 0; pathId < deduplicatedPaths.size(); pathId++) {
      List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));
      if (pathNeedSlide[resultIndex.get(0)]) {
        pathSlide(pathId);
      }
    }

    if (!leftCRightO) {
      record.setTimestamp(curEndTime - 1);
    }
    return record;
  }
}
