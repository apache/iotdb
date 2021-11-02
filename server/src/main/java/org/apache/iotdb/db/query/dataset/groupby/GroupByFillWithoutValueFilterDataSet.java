package org.apache.iotdb.db.query.dataset.groupby;

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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GroupByFillWithoutValueFilterDataSet extends GroupByWithoutValueFilterDataSet {

  private final Map<TSDataType, IFill> fillTypes;
  private final List<String> aggregations;

  // the extra previous means first not null value before startTime
  private Object[] extraPreviousValues;
  private long[] extraPreviousTimes;

  // the previous value for each time series, which means
  // first not null value GEQ curStartTime in order asc
  // second not null value GEQ curStartTime in order desc
  private Object[] previousValues;
  private long[] previousTimes;

  // the extra next means first not null value after endTime
  private Object[] extraNextValues;
  private long[] extraNextTimes;

  // the next value for each time series, which means
  // first not null value LEQ curStartTime in order desc
  // second not null value LEQ curStartTime in order asc
  private Object[] nextValues;
  private long[] nextTimes;

  // the query and result datatype for each time series
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
    this.fillTypes = groupByTimeFillPlan.getFillType();
    this.aggregations = groupByTimeFillPlan.getDeduplicatedAggregations();

    initArrays();
    // TODO: init extra arrays
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
    for (int i = 0; i < paths.size(); i++) {
      List<Integer> indexes = resultIndexes.get((PartialPath) paths.get(i));
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
            resultDataType[index] = dataTypes.get(i);
            break;
        }
      }
    }
  }

  private boolean pathHasNext(int pathId) {
    if (hasCachedQueryInterval[pathId]) {
      return true;
    }

    // for group by natural months addition
    queryIntervalTimes[pathId] += ascending ? 1 : -1;

    if (ascending) {
      if (isSlidingStepByMonth) {
        queryStartTimes[pathId] = calcIntervalByMonth(slidingStep * queryIntervalTimes[pathId]);
      } else {
        queryStartTimes[pathId] += slidingStep;
      }
      // This is an open interval , [0-100)
      if (queryStartTimes[pathId] >= endTime) {
        return false;
      }
    } else {
      if (isSlidingStepByMonth) {
        queryStartTimes[pathId] = calcIntervalByMonth(slidingStep * queryIntervalTimes[pathId]);
      } else {
        queryStartTimes[pathId] -= slidingStep;
      }
      if (queryStartTimes[pathId] < startTime) {
        return false;
      }
    }

    hasCachedQueryInterval[pathId] = true;
    if (isIntervalByMonth) {
      queryEndTimes[pathId] =
          Math.min(
              calcIntervalByMonth(queryIntervalTimes[pathId] * slidingStep + interval), endTime);
    } else {
      queryEndTimes[pathId] = Math.min(queryStartTimes[pathId] + interval, endTime);
    }
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

  private void slideCache(int pathId) {
    if (ascending) {
      previousValues[pathId] = nextValues[pathId];
      previousTimes[pathId] = nextTimes[pathId];
      nextValues[pathId] = null;
      nextTimes[pathId] = Long.MAX_VALUE;
    } else {
      nextValues[pathId] = previousValues[pathId];
      nextTimes[pathId] = previousTimes[pathId];
      previousValues[pathId] = null;
      previousTimes[pathId] = Long.MIN_VALUE;
    }
  }

  private void pathGetNext(int pathId) throws IOException {
    try {
      GroupByExecutor executor = pathExecutors.get((PartialPath) paths.get(pathId));
      List<Integer> resultIndex = resultIndexes.get((PartialPath) paths.get(pathId));

      // Slide value and time
      for (int Index : resultIndex) {
        slideCache(Index);
      }

      // get second not null aggregate results
      List<AggregateResult> aggregations =
          executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
      hasCachedQueryInterval[pathId] = false;
      while (resultIsNull(aggregations) && pathHasNext(pathId)) {
        aggregations = executor.calcResult(queryStartTimes[pathId], queryEndTimes[pathId]);
        hasCachedQueryInterval[pathId] = false;
      }

      if (resultIsNull(aggregations)) {
        if (ascending) {
          nextValues[pathId] = extraNextValues[pathId];
          nextTimes[pathId] = extraNextTimes[pathId];
        } else {
          previousValues[pathId] = extraPreviousValues[pathId];
          previousTimes[pathId] = extraPreviousTimes[pathId];
        }
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
    } catch (QueryProcessException e) {
      logger.error("GroupByFillWithoutValueFilterDataSet execute has error: ", e);
      throw new IOException(e.getMessage(), e);
    }

    hasCachedQueryInterval[pathId] = false;
  }

  private void pathSlide(int pathId) throws IOException {
    if (pathHasNext(pathId)) {
      pathGetNext(pathId);
    } else {
      if (ascending) {
        nextValues[pathId] = extraNextValues[pathId];
        nextTimes[pathId] = extraNextTimes[pathId];
      } else {
        previousValues[pathId] = extraPreviousValues[pathId];
        previousTimes[pathId] = extraPreviousTimes[pathId];
      }
    }
  }

  private void initCachedTimesAndValues() throws QueryProcessException {
    for (int pathId = 0; pathId < paths.size(); pathId++) {
      try {
        pathSlide(pathId);
        pathSlide(pathId);
      } catch (IOException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
  }

  private void fillRecord(
      int pathId, RowRecord record, Pair<Long, Object> beforePair, Pair<Long, Object> afterPair)
      throws IOException {
    // Don't fill count aggregation
    if (Objects.equals(aggregations.get(pathId), "count")) {
      record.addField((long) 0, TSDataType.INT64);
      return;
    }

    IFill fill = fillTypes.get(resultDataType[pathId]);
    if (fill instanceof PreviousFill) {
      if (beforePair.right != null
          && ((!((PreviousFill) fill).isUntilLast())
              || (afterPair.right != null && afterPair.left < endTime))) {
        record.addField(beforePair.right, resultDataType[pathId]);
      } else {
        record.addField(null);
      }
    } else if (fill instanceof LinearFill) {
      LinearFill linearFill = new LinearFill();
      if (beforePair.right != null && afterPair.right != null) {
        try {
          TimeValuePair filledPair =
              linearFill.averageWithTimeAndDataType(
                  new TimeValuePair(
                      beforePair.left,
                      TsPrimitiveType.getByType(resultDataType[pathId], beforePair.right)),
                  new TimeValuePair(
                      afterPair.left,
                      TsPrimitiveType.getByType(resultDataType[pathId], afterPair.right)),
                  curStartTime,
                  resultDataType[pathId]);
          record.addField(filledPair.getValue().getValue(), resultDataType[pathId]);
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
        record.addField(filledPair.getValue().getValue(), resultDataType[pathId]);
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
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }

    for (int pathId = 0; pathId < previousTimes.length; pathId++) {
      if (previousTimes[pathId] == curStartTime) {
        record.addField(previousValues[pathId], resultDataType[pathId]);
        if (!ascending) {
          pathSlide(pathId);
        }
      } else if (nextTimes[pathId] == curStartTime) {
        record.addField(nextValues[pathId], resultDataType[pathId]);
        if (ascending) {
          pathSlide(pathId);
        }
      } else if (previousTimes[pathId] < curStartTime && curStartTime < nextTimes[pathId]) {
        fillRecord(
            pathId,
            record,
            new Pair<>(previousTimes[pathId], previousValues[pathId]),
            new Pair<>(nextTimes[pathId], nextValues[pathId]));
      } else if (curStartTime < previousTimes[pathId]) {
        fillRecord(
            pathId,
            record,
            new Pair<>(extraPreviousTimes[pathId], extraPreviousValues[pathId]),
            new Pair<>(previousTimes[pathId], previousValues[pathId]));
      } else if (nextTimes[pathId] < curStartTime) {
        fillRecord(
            pathId,
            record,
            new Pair<>(nextTimes[pathId], nextValues[pathId]),
            new Pair<>(extraNextTimes[pathId], extraNextValues[pathId]));
      }
    }

    return record;
  }
}
