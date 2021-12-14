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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class GroupByFillEngineDataSet extends GroupByEngineDataSet {

  protected Map<TSDataType, IFill> fillTypes;
  protected IFill singleFill;
  protected final List<PartialPath> deduplicatedPaths;
  protected final List<String> aggregations;
  protected final Map<PartialPath, List<Integer>> resultIndexes = new HashMap<>();

  // the extra previous means first not null value before startTime
  // used to fill result before the first not null data
  protected Object[] extraPreviousValues;
  protected long[] extraPreviousTimes;

  // the previous value for each time series, which means
  // first not null value GEQ curStartTime in order asc
  // second not null value GEQ curStartTime in order desc
  protected Object[] previousValues;
  protected long[] previousTimes;

  // the next value for each time series, which means
  // first not null value LEQ curStartTime in order desc
  // second not null value LEQ curStartTime in order asc
  protected Object[] nextValues;
  protected long[] nextTimes;

  // the extra next means first not null value after endTime
  // used to fill result after the last not null data
  protected Object[] extraNextValues;
  protected long[] extraNextTimes;

  // the result datatype for each time series
  protected TSDataType[] resultDataType;

  // the next query time range of each path
  protected long[] queryStartTimes;
  protected long[] queryEndTimes;
  protected boolean[] hasCachedQueryInterval;

  public GroupByFillEngineDataSet(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan) {
    super(context, groupByTimeFillPlan);
    this.aggregations = groupByTimeFillPlan.getDeduplicatedAggregations();
    this.fillTypes = groupByTimeFillPlan.getFillType();
    this.singleFill = groupByTimeFillPlan.getSingleFill();

    this.deduplicatedPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = (PartialPath) paths.get(i);
      if (!deduplicatedPaths.contains(path)) {
        deduplicatedPaths.add(path);
        resultIndexes.put(path, new ArrayList<>());
      }
      resultIndexes.get(path).add(i);
    }

    initArrays();
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
  protected void initCachedTimesAndValues() throws QueryProcessException {
    for (int pathId = 0; pathId < deduplicatedPaths.size(); pathId++) {
      try {
        pathSlide(pathId);
        pathSlide(pathId);
      } catch (IOException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
  }

  /* check if specified path has next extra range */
  protected boolean pathHasExtra(int pathId, boolean isExtraPrevious, long extraStartTime) {
    List<Integer> Indexes = resultIndexes.get(deduplicatedPaths.get(pathId));
    for (int resultIndex : Indexes) {
      if (isExtraPrevious && extraPreviousValues[resultIndex] != null) {
        continue;
      } else if (!isExtraPrevious && extraNextValues[resultIndex] != null) {
        continue;
      }

      IFill fill;
      if (fillTypes != null) {
        // old type fill logic
        fill = fillTypes.get(resultDataType[resultIndex]);
      } else {
        fill = singleFill;
      }
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

  /* If result is null or CountAggrResult is 0, then result is NULL */
  protected boolean resultIsNull(List<AggregateResult> aggregateResults) {
    AggregateResult result = aggregateResults.get(0);
    if (result.getResult() == null) {
      return true;
    } else {
      return result instanceof CountAggrResult && (long) result.getResult() == 0;
    }
  }

  protected boolean pathHasNext(int pathId) {
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

  protected abstract void pathGetNext(int pathId) throws IOException;

  protected void pathSlide(int pathId) throws IOException {
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

    IFill fill;
    if (fillTypes != null) {
      // old type fill logic
      fill = fillTypes.get(resultDataType[resultId]);
    } else {
      fill = singleFill;
    }
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
        if (filledPair == null) {
          filledPair = ((ValueFill) fill).getSpecifiedFillResult(resultDataType[resultId]);
        }
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

  // Slide cached value and time
  protected void cacheSlideNext(List<Integer> cacheIndexes) {
    if (ascending) {
      for (int cacheId : cacheIndexes) {
        previousValues[cacheId] = nextValues[cacheId];
        previousTimes[cacheId] = nextTimes[cacheId];
        nextValues[cacheId] = null;
        nextTimes[cacheId] = Long.MAX_VALUE;
      }
    } else {
      for (int cacheId : cacheIndexes) {
        nextValues[cacheId] = previousValues[cacheId];
        nextTimes[cacheId] = previousTimes[cacheId];
        previousValues[cacheId] = null;
        previousTimes[cacheId] = Long.MIN_VALUE;
      }
    }
  }

  // Fill cached value and time
  protected void cacheFillNext(
      int pathId, List<AggregateResult> aggregateResults, List<Integer> cacheIndexes)
      throws IOException {
    if (resultIsNull(aggregateResults)) {
      pathSlide(pathId);
    } else {
      for (int i = 0; i < aggregateResults.size(); i++) {
        int Index = cacheIndexes.get(i);
        if (ascending) {
          nextValues[Index] = aggregateResults.get(i).getResult();
          nextTimes[Index] = queryStartTimes[pathId];
        } else {
          previousValues[Index] = aggregateResults.get(i).getResult();
          previousTimes[Index] = queryStartTimes[pathId];
        }
      }
    }
  }
}
