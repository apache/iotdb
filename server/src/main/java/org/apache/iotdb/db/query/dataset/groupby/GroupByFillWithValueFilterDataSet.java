package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class GroupByFillWithValueFilterDataSet extends GroupByWithValueFilterDataSet {

  private final Map<TSDataType, IFill> fillTypes;
  private final List<PartialPath> deduplicatedPaths;
  private final List<String> aggregations;
  private final Map<PartialPath, List<Integer>> resultIndexes = new HashMap<>();

  // the extra previous means first not null value before startTime
  // used to fill result before the first not null data
  private Object[] extraPreviousValues;
  private long[] extraPreviousTimes;

  // the previous value for each time series, which means
  // first not null value GEQ curStartTime in order asc
  // second not null value GEQ curStartTime in order desc
  private Object[] previousValues;
  private long[] previousTimes;

  // the next value for each time series, which means
  // first not null value LEQ curStartTime in order desc
  // second not null value LEQ curStartTime in order asc
  private Object[] nextValues;
  private long[] nextTimes;

  // the extra next means first not null value after endTime
  // used to fill result after the last not null data
  private Object[] extraNextValues;
  private long[] extraNextTimes;

  // the result datatype for each time series
  private TSDataType[] resultDataType;

  // the next query time range of each path
  private long[] queryStartTimes;
  private long[] queryEndTimes;
  private boolean[] hasCachedQueryInterval;

  // the next query time range of each path
  private final List<TimeGenerator> timestampGenerators;
  private List<TimeGenerator> extraPreviousGenerators;
  private List<TimeGenerator> extraNextGenerators;
  private List<IReaderByTimestamp> extraPreviousDataReaderList;
  private List<IReaderByTimestamp> extraNextDataReaderList;

  /** cached timestamp for next group by partition. */
  private long lastTimestamp;

  private final List<LinkedList<Long>> cachedTimestamps;

  /** constructor. */
  public GroupByFillWithValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimeFillPlan);
    this.aggregations = groupByTimeFillPlan.getDeduplicatedAggregations();
    this.fillTypes = groupByTimeFillPlan.getFillType();

    this.deduplicatedPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = (PartialPath) paths.get(i);
      if (!deduplicatedPaths.contains(path)) {
        deduplicatedPaths.add(path);
        resultIndexes.put(path, new ArrayList<>());
      }
      resultIndexes.get(path).add(i);
    }

    this.timestampGenerators = new ArrayList<>();
    this.cachedTimestamps = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      timestampGenerators.add(getTimeGenerator(context, groupByTimeFillPlan));
      cachedTimestamps.add(new LinkedList<>());
    }

    initArrays();
    initCachedTimesAndValues();
    initExtraGenerators(context, groupByTimeFillPlan);
    if (extraPreviousGenerators != null) {
      initExtraArrays(extraPreviousValues, extraPreviousTimes, true, extraPreviousGenerators);
    }
    if (extraNextGenerators != null) {
      initExtraArrays(extraNextValues, extraNextTimes, false, extraNextGenerators);
    }
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

    queryStartTimes = new long[deduplicatedPaths.size()];
    queryEndTimes = new long[deduplicatedPaths.size()];
    hasCachedQueryInterval = new boolean[deduplicatedPaths.size()];
    resultDataType = new TSDataType[aggregations.size()];
    Arrays.fill(queryStartTimes, curStartTime);
    Arrays.fill(queryEndTimes, curEndTime);
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
      IExpression newExpression = groupByTimeFillPlan.getExpression().clone();
      try {
        replaceGroupByFilter(newExpression, timeFilter);
      } catch (IllegalPathException ignore) {
        // ignored
      }
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
      IExpression newExpression = groupByTimeFillPlan.getExpression().clone();
      try {
        replaceGroupByFilter(newExpression, timeFilter);
      } catch (IllegalPathException ignore) {
        // ignored
      }
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
            getReaderByTime(path, groupByTimePlan, dataTypes.get(i), context, null));
        extraNextDataReaderList.add(
            getReaderByTime(path, groupByTimePlan, dataTypes.get(i), context, null));
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
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
          AggregateResult[] aggregateResults =
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
            for (int i = 0; i < aggregateResults.length; i++) {
              if (extraValues[Indexes.get(i)] == null) {
                extraValues[Indexes.get(i)] = aggregateResults[i].getResult();
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

  /* If result is null or CountAggrResult is 0, then result is NULL */
  private boolean resultIsNull(AggregateResult[] aggregateResults) {
    AggregateResult result = aggregateResults[0];
    if (result.getResult() == null) {
      return true;
    } else {
      return result instanceof CountAggrResult && (long) result.getResult() == 0;
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

  private AggregateResult[] calcResult(
      int pathId,
      Pair<Long, Long> timeRange,
      TimeGenerator timeGenerator,
      LinkedList<Long> cachedTimestamp,
      List<IReaderByTimestamp> dataReaderList,
      boolean isAscending)
      throws IOException {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));
    AggregateResult[] aggregateResults = new AggregateResult[resultIndex.size()];

    for (int i = 0; i < resultIndex.size(); i++) {
      aggregateResults[i] =
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(resultIndex.get(i)), dataTypes.get(resultIndex.get(i)), ascending);
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
        aggregateResults[i].updateResultUsingTimestamps(
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
        aggregateResults[i].updateResultUsingTimestamps(
            timestampArray, timeArrayLength, dataReaderList.get(resultIndex.get(i)));
      }
    }
    return aggregateResults;
  }

  private void pathGetNext(int pathId) throws IOException {
    List<Integer> resultIndex = resultIndexes.get(deduplicatedPaths.get(pathId));

    // Slide value and time
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

    // get second not null aggregate results
    AggregateResult[] aggregateResults;
    aggregateResults =
        calcResult(
            pathId,
            new Pair<>(queryStartTimes[pathId], queryEndTimes[pathId]),
            timestampGenerators.get(pathId),
            cachedTimestamps.get(pathId),
            allDataReaderList,
            groupByTimePlan.isAscending());
    hasCachedQueryInterval[pathId] = false;
    while (resultIsNull(aggregateResults) && pathHasNext(pathId)) {
      aggregateResults =
          calcResult(
              pathId,
              new Pair<>(queryStartTimes[pathId], queryEndTimes[pathId]),
              timestampGenerators.get(pathId),
              cachedTimestamps.get(pathId),
              allDataReaderList,
              groupByTimePlan.isAscending());
      hasCachedQueryInterval[pathId] = false;
    }

    if (resultIsNull(aggregateResults)) {
      pathSlide(pathId);
    } else {
      for (int i = 0; i < aggregateResults.length; i++) {
        int Index = resultIndex.get(i);
        if (ascending) {
          nextValues[Index] = aggregateResults[i].getResult();
          nextTimes[Index] = queryStartTimes[pathId];
        } else {
          previousValues[Index] = aggregateResults[i].getResult();
          previousTimes[Index] = queryStartTimes[pathId];
        }
      }
    }

    hasCachedQueryInterval[pathId] = false;
  }

  private boolean pathHasNext(int pathId) {
    // has cached
    if (hasCachedQueryInterval[pathId]) {
      return true;
    }

    // find the next aggregation interval
    Pair<Long, Long> nextTimeRange = getNextTimeRange(queryStartTimes[pathId], ascending, true);
    if (nextTimeRange == null) {
      return false;
    }
    queryStartTimes[pathId] = nextTimeRange.left;
    queryEndTimes[pathId] = nextTimeRange.right;

    hasCachedQueryInterval[pathId] = true;
    return true;
  }

  private void pathSlide(int pathId) throws IOException {
    if (pathHasNext(pathId)) {
      pathGetNext(pathId);
    } else {
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
              + "in GroupByFillWithValueFilterDataSet.");
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
