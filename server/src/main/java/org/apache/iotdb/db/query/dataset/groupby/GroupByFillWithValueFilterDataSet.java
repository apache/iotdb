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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GroupByFillWithValueFilterDataSet extends GroupByFillEngineDataSet {

  // the next query time range of each path
  private List<TimeGenerator> timestampGenerators;
  private List<TimeGenerator> extraPreviousGenerators;
  private List<TimeGenerator> extraNextGenerators;

  // data reader lists
  private List<IReaderByTimestamp> allDataReaderList;
  private List<IReaderByTimestamp> extraPreviousDataReaderList;
  private List<IReaderByTimestamp> extraNextDataReaderList;

  // cached timestamp for next group by partition
  private long lastTimestamp;
  private List<LinkedList<Long>> cachedTimestamps;

  private final int timeStampFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();

  /** constructor. */
  public GroupByFillWithValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimeFillPlan);

    initPathGenerators(context, groupByTimeFillPlan);

    initExtraGenerators(context, groupByTimeFillPlan);
    if (extraPreviousGenerators != null) {
      initExtraArrays(extraPreviousValues, extraPreviousTimes, true, extraPreviousGenerators);
    }
    if (extraNextGenerators != null) {
      initExtraArrays(extraNextValues, extraNextTimes, false, extraNextGenerators);
    }

    initCachedTimesAndValues();
  }

  private void initPathGenerators(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException, StorageEngineException {
    this.timestampGenerators = new ArrayList<>();
    this.cachedTimestamps = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      timestampGenerators.add(getTimeGenerator(context, groupByTimeFillPlan));
      cachedTimestamps.add(new LinkedList<>());
    }

    this.allDataReaderList = new ArrayList<>();
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    try {
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        allDataReaderList.add(
            getReaderByTime(path, groupByTimeFillPlan, dataTypes.get(i), context));
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  private TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  private IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan queryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        ascending);
  }

  private void replaceGroupByFilter(IExpression expression, Filter timeFilter)
      throws QueryProcessException, IllegalPathException {
    if (expression instanceof SingleSeriesExpression) {
      ((SingleSeriesExpression) expression)
          .setSeriesPath(
              new PartialPath(((SingleSeriesExpression) expression).getSeriesPath().getFullPath()));
      if (((SingleSeriesExpression) expression).getFilter() instanceof GroupByFilter) {
        ((SingleSeriesExpression) expression).setFilter(timeFilter);
      } else if (((SingleSeriesExpression) expression).getFilter() instanceof BinaryFilter) {
        if (((BinaryFilter) ((SingleSeriesExpression) expression).getFilter()).getLeft()
            instanceof GroupByFilter) {
          ((BinaryFilter) ((SingleSeriesExpression) expression).getFilter()).setLeft(timeFilter);
        } else if (((BinaryFilter) ((SingleSeriesExpression) expression).getFilter()).getRight()
            instanceof GroupByFilter) {
          ((BinaryFilter) ((SingleSeriesExpression) expression).getFilter()).setRight(timeFilter);
        }
      } else {
        throw new QueryProcessException("unknown filter type, can't replace group by filter");
      }
    } else if (expression instanceof BinaryExpression) {
      replaceGroupByFilter(((BinaryExpression) expression).getLeft(), timeFilter);
      replaceGroupByFilter(((BinaryExpression) expression).getRight(), timeFilter);
    } else {
      throw new QueryProcessException("unknown expression type, can't replace group by filter");
    }
  }

  // get new expression that can query extra range
  private IExpression getNewExpression(GroupByTimeFillPlan groupByTimeFillPlan, Filter timeFilter)
      throws QueryProcessException {
    IExpression newExpression = groupByTimeFillPlan.getExpression().clone();
    try {
      replaceGroupByFilter(newExpression, timeFilter);
    } catch (IllegalPathException ignore) {
      // ignored
    }
    return newExpression;
  }

  /* Init extra path executors to query data outside the original group by query */
  private void initExtraGenerators(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws StorageEngineException, QueryProcessException {
    long minQueryStartTime = Long.MAX_VALUE;
    long maxQueryEndTime = Long.MIN_VALUE;
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
      extraPreviousGenerators = new ArrayList<>();

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
      IExpression newExpression = getNewExpression(groupByTimeFillPlan, timeFilter);
      groupByTimeFillPlan.setExpression(newExpression);
      for (int i = 0; i < deduplicatedPaths.size(); i++) {
        extraPreviousGenerators.add(getTimeGenerator(context, groupByTimeFillPlan));
      }
    }

    if (maxQueryEndTime > Long.MIN_VALUE) {
      extraNextGenerators = new ArrayList<>();
      Pair<Long, Long> lastTimeRange = getLastTimeRange();
      lastTimeRange = getNextTimeRange(lastTimeRange.left, true, false);

      Filter timeFilter =
          new GroupByFilter(interval, slidingStep, lastTimeRange.left, maxQueryEndTime);
      IExpression newExpression = getNewExpression(groupByTimeFillPlan, timeFilter);
      groupByTimeFillPlan.setExpression(newExpression);
      for (int i = 0; i < deduplicatedPaths.size(); i++) {
        extraNextGenerators.add(getTimeGenerator(context, groupByTimeFillPlan));
      }
    }

    extraPreviousDataReaderList = new ArrayList<>();
    extraNextDataReaderList = new ArrayList<>();
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    try {
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        extraPreviousDataReaderList.add(
            getReaderByTime(path, groupByTimeFillPlan, dataTypes.get(i), context));
        extraNextDataReaderList.add(
            getReaderByTime(path, groupByTimeFillPlan, dataTypes.get(i), context));
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  private void initExtraArrays(
      Object[] extraValues,
      long[] extraTimes,
      boolean isExtraPrevious,
      List<TimeGenerator> extraGenerators)
      throws QueryProcessException {
    for (int pathId = 0; pathId < deduplicatedPaths.size(); pathId++) {
      List<Integer> Indexes = resultIndexes.get(deduplicatedPaths.get(pathId));

      Pair<Long, Long> extraTimeRange;
      if (isExtraPrevious) {
        extraTimeRange = getFirstTimeRange();
      } else {
        extraTimeRange = getLastTimeRange();
      }

      LinkedList<Long> cachedTimestamp = new LinkedList<>();
      extraTimeRange = getNextTimeRange(extraTimeRange.left, !isExtraPrevious, false);
      try {
        while (pathHasExtra(pathId, isExtraPrevious, extraTimeRange.left)) {
          List<AggregateResult> aggregateResults =
              calcResult(
                  pathId,
                  extraTimeRange,
                  extraGenerators.get(pathId),
                  cachedTimestamp,
                  isExtraPrevious ? extraPreviousDataReaderList : extraNextDataReaderList,
                  !isExtraPrevious);
          if (!resultIsNull(aggregateResults)) {
            // we check extra time range in single path together,
            // thus the extra result will be cached together
            for (int i = 0; i < aggregateResults.size(); i++) {
              if (extraValues[Indexes.get(i)] == null) {
                extraValues[Indexes.get(i)] = aggregateResults.get(i).getResult();
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

  private int constructTimeArrayForOneCal(
      long[] timestampArray,
      int timeArrayLength,
      Pair<Long, Long> timeRange,
      boolean isAscending,
      TimeGenerator timeGenerator,
      LinkedList<Long> cachedTimestamp)
      throws IOException {

    for (int cnt = 1;
        cnt < timeStampFetchSize - 1 && (!cachedTimestamp.isEmpty() || timeGenerator.hasNext());
        cnt++) {
      if (!cachedTimestamp.isEmpty()) {
        lastTimestamp = cachedTimestamp.remove();
      } else {
        lastTimestamp = timeGenerator.next();
      }
      if (isAscending && lastTimestamp < timeRange.right) {
        timestampArray[timeArrayLength++] = lastTimestamp;
      } else if (!isAscending && lastTimestamp >= timeRange.left) {
        timestampArray[timeArrayLength++] = lastTimestamp;
      } else {
        // may lastTimestamp get from cache
        if (!cachedTimestamp.isEmpty() && lastTimestamp <= cachedTimestamp.peek()) {
          cachedTimestamp.addFirst(lastTimestamp);
        } else {
          cachedTimestamp.add(lastTimestamp);
        }
        break;
      }
    }
    return timeArrayLength;
  }

  private List<AggregateResult> calcResult(
      int pathId,
      Pair<Long, Long> timeRange,
      TimeGenerator timeGenerator,
      LinkedList<Long> cachedTimestamp,
      List<IReaderByTimestamp> dataReaderList,
      boolean isAscending)
      throws IOException {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));
    List<AggregateResult> aggregateResults = new ArrayList<>();

    for (int index : resultIndex) {
      aggregateResults.add(
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(index), dataTypes.get(index), ascending));
    }

    long[] timestampArray = new long[timeStampFetchSize];
    int timeArrayLength = 0;

    if (!cachedTimestamp.isEmpty()) {
      long timestamp = cachedTimestamp.remove();
      if (timestamp < timeRange.right) {
        if (!isAscending && timestamp < timeRange.left) {
          cachedTimestamp.addFirst(timestamp);
          return aggregateResults;
        }
        if (timestamp >= timeRange.left) {
          timestampArray[timeArrayLength++] = timestamp;
        }
      } else {
        cachedTimestamp.addFirst(timestamp);
        return aggregateResults;
      }
    }

    while (!cachedTimestamp.isEmpty() || timeGenerator.hasNext()) {
      // construct timestamp array
      timeArrayLength =
          constructTimeArrayForOneCal(
              timestampArray,
              timeArrayLength,
              timeRange,
              isAscending,
              timeGenerator,
              cachedTimestamp);

      // cal result using timestamp array
      for (int i = 0; i < resultIndex.size(); i++) {
        aggregateResults
            .get(i)
            .updateResultUsingTimestamps(
                timestampArray, timeArrayLength, dataReaderList.get(resultIndex.get(i)));
      }

      timeArrayLength = 0;
      // judge if it's end
      if ((isAscending && lastTimestamp >= timeRange.right)
          || (!isAscending && lastTimestamp < timeRange.left)) {
        break;
      }
    }

    if (timeArrayLength > 0) {
      // cal result using timestamp array
      for (int i = 0; i < resultIndex.size(); i++) {
        aggregateResults
            .get(i)
            .updateResultUsingTimestamps(
                timestampArray, timeArrayLength, dataReaderList.get(resultIndex.get(i)));
      }
    }
    return aggregateResults;
  }

  @Override
  protected void pathGetNext(int pathId) throws IOException {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));

    cacheSlideNext(resultIndex);

    // get second not null aggregate results
    List<AggregateResult> aggregateResults;
    aggregateResults =
        calcResult(
            pathId,
            new Pair<>(queryStartTimes[pathId], queryEndTimes[pathId]),
            timestampGenerators.get(pathId),
            cachedTimestamps.get(pathId),
            allDataReaderList,
            ascending);
    hasCachedQueryInterval[pathId] = false;
    while (resultIsNull(aggregateResults) && pathHasNext(pathId)) {
      aggregateResults =
          calcResult(
              pathId,
              new Pair<>(queryStartTimes[pathId], queryEndTimes[pathId]),
              timestampGenerators.get(pathId),
              cachedTimestamps.get(pathId),
              allDataReaderList,
              ascending);
      hasCachedQueryInterval[pathId] = false;
    }

    cacheFillNext(pathId, aggregateResults, resultIndex);

    hasCachedQueryInterval[pathId] = false;
  }
}
