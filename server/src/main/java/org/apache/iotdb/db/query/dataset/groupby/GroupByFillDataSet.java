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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GroupByFillDataSet extends QueryDataSet {

  private GroupByEngineDataSet groupByEngineDataSet;
  private Map<TSDataType, IFill> fillTypes;
  // the previous value for each time series
  private Object[] previousValue;
  private long[] previousTime;
  // the next value for each time series
  private Object[] nextValue;
  private long[] nextTime;
  // last timestamp for each time series
  private long[] lastTimeArray;
  private TimeValuePair[] firstNotNullTV;
  private boolean isPeekEnded = false;
  // cached row records
  private ArrayList<RowRecord> unFilledRowRecords = new ArrayList<>();
  private LinkedList<RowRecord> cachedRowRecords = new LinkedList<>();
  // aggregation type of each column
  private List<String> aggregations;

  private static int DEFAULT_FETCH_SIZE = 5000;

  public GroupByFillDataSet(
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      GroupByEngineDataSet groupByEngineDataSet,
      Map<TSDataType, IFill> fillTypes,
      QueryContext context,
      GroupByTimeFillPlan groupByFillPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    super(new ArrayList<>(paths), dataTypes, groupByFillPlan.isAscending());
    this.groupByEngineDataSet = groupByEngineDataSet;
    this.fillTypes = fillTypes;
    this.aggregations = groupByFillPlan.getAggregations();
    this.lastTimeArray = new long[paths.size()];

    previousValue = new Object[paths.size()];
    previousTime = new long[paths.size()];
    nextValue = new Object[paths.size()];
    nextTime = new long[paths.size()];
    Arrays.fill(previousValue, null);
    Arrays.fill(previousTime, Long.MAX_VALUE);
    Arrays.fill(nextValue, null);
    Arrays.fill(nextTime, Long.MIN_VALUE);

//    List<StorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(paths);
//    try {
//      initLastTimeArray(context, groupByFillPlan);
//    } finally {
//      StorageEngine.getInstance().mergeUnLock(list);
//    }
  }

  private void initPreviousParis(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    previousValue = new Object[paths.size()];
    previousTime = new long[paths.size()];
    firstNotNullTV = new TimeValuePair[paths.size()];

    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = (PartialPath) paths.get(i);
      TSDataType dataType = dataTypes.get(i);
      IFill fill;
      if (fillTypes.containsKey(dataType)) {
        fill =
            new PreviousFill(
                dataType,
                groupByEngineDataSet.getStartTime(),
                ((PreviousFill) fillTypes.get(dataType)).getBeforeRange(),
                ((PreviousFill) fillTypes.get(dataType)).isUntilLast());
      } else {
        fill =
            new PreviousFill(
                dataType,
                groupByEngineDataSet.getStartTime(),
                IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval());
      }
      fill.configureFill(
          path,
          dataType,
          groupByEngineDataSet.getStartTime(),
          groupByFillPlan.getAllMeasurementsInDevice(path.getDevice()),
          context);

      firstNotNullTV[i] = fill.getFillResult();
      TimeValuePair timeValuePair = firstNotNullTV[i];
      previousValue[i] = null;
      previousTime[i] = Long.MAX_VALUE;
      if (ascending && timeValuePair != null && timeValuePair.getValue() != null) {
        previousValue[i] = timeValuePair.getValue().getValue();
        previousTime[i] = timeValuePair.getTimestamp();
      }
    }
  }

  private void initLastTimeArray(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws IOException, StorageEngineException, QueryProcessException {
    lastTimeArray = new long[paths.size()];
    Arrays.fill(lastTimeArray, Long.MAX_VALUE);
    List<PartialPath> seriesPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      seriesPaths.add((PartialPath) paths.get(i));
    }
    List<Pair<Boolean, TimeValuePair>> lastValueContainer =
        LastQueryExecutor.calculateLastPairForSeriesLocally(
            seriesPaths, dataTypes, context, null, groupByFillPlan.getDeviceToMeasurements());
    for (int i = 0; i < lastValueContainer.size(); i++) {
      if (Boolean.TRUE.equals(lastValueContainer.get(i).left)) {
        lastTimeArray[i] = lastValueContainer.get(i).right.getTimestamp();
      }
    }
  }

  private boolean isFieldNull(Field field) {
    return field == null || field.getDataType() == null;
  }

  private boolean fieldLessOrNull(int curId, int nextId, int pathId) {
    if (nextId == unFilledRowRecords.size()) {
      return false;
    }
    if (curId >= nextId) {
      return true;
    }
    RowRecord rowRecord = unFilledRowRecords.get(nextId);
    Field field = rowRecord.getFields().get(pathId);
    return isFieldNull(field);
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    if (cachedRowRecords.size() > 0) {
      return true;
    }

    // TODO: test desc
    if (groupByEngineDataSet.hasNextWithoutConstraint()) {
      BitSet isFilled = new BitSet(paths.size());
      isFilled.clear();
      // TODO: test exceed limit
      for (int recordCnt = 0; recordCnt < DEFAULT_FETCH_SIZE; recordCnt++) {
        if (!groupByEngineDataSet.hasNextWithoutConstraint()) {
          break;
        }
        RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();
        for (int pathId = 0; pathId < paths.size(); pathId++) {
          Field field = rowRecord.getFields().get(pathId);
          TSDataType tsDataType = dataTypes.get(pathId);
          boolean filledFlag;
          if (isFieldNull(field)) {
            IFill fill = fillTypes.get(tsDataType);
            // LinearFill and PreviousUntilLastFill needs to be filled with data at the next non-empty time
            if (fill instanceof PreviousFill) {
              filledFlag = !((PreviousFill) fill).isUntilLast();
            } else {
              filledFlag = false;
            }
          } else {
            filledFlag = true;
            lastTimeArray[pathId] = rowRecord.getTimestamp();
          }
          isFilled.set(pathId, filledFlag);
        }

        unFilledRowRecords.add(rowRecord);
        if (isFilled.cardinality() == paths.size()) {
          break;
        }
      }

      for (int pathId = 0; pathId < paths.size(); pathId++) {
        TSDataType queryDataType = dataTypes.get(pathId);
        TSDataType resultDataType = getResultDataType(pathId);
        IFill fill = fillTypes.get(queryDataType);
        if (fill instanceof PreviousFill) {
          for (RowRecord rowRecord : unFilledRowRecords) {
            Field field = rowRecord.getFields().get(pathId);
            long curTime = rowRecord.getTimestamp();
            if (isFieldNull(field)) {
              // TODO: desc fill
              if (previousValue[pathId] != null
                  && previousTime[pathId] < curTime
                  && (!((PreviousFill) fill).isUntilLast() || curTime < lastTimeArray[pathId])) {
                rowRecord
                    .getFields()
                    .set(pathId, Field.getField(previousValue[pathId], resultDataType));
              }
            } else {
              previousValue[pathId] = field.getObjectValue(resultDataType);
              previousTime[pathId] = curTime;
            }
          }
        } else if (fill instanceof LinearFill) {
          int nextId = 0;
          for (int curId = 0; curId < unFilledRowRecords.size(); curId++) {
            RowRecord rowRecord = unFilledRowRecords.get(curId);
            Field field = rowRecord.getFields().get(pathId);
            long curTime = rowRecord.getTimestamp();
            if (isFieldNull(field)) {
              while (fieldLessOrNull(curId, nextId, pathId)) {
                nextId++;
              }
              if (nextId == unFilledRowRecords.size()) {
                // TODO: Query next then calculate
              } else if (previousValue[pathId] != null && previousTime[pathId] < curTime) {
                RowRecord nextRowRecord = unFilledRowRecords.get(nextId);
                Field nextField = nextRowRecord.getFields().get(pathId);
                LinearFill linearFill = new LinearFill();
                TimeValuePair beforePair =
                    new TimeValuePair(
                        previousTime[pathId],
                        TsPrimitiveType.getByType(resultDataType, previousValue[pathId]));
                TimeValuePair afterPair =
                    new TimeValuePair(
                        nextRowRecord.getTimestamp(),
                        TsPrimitiveType.getByType(
                            resultDataType, nextField.getObjectValue(resultDataType)));
                TimeValuePair filledPair = null;
                try {
                  filledPair =
                      linearFill.averageWithTimeAndDataType(
                          beforePair, afterPair, curTime, resultDataType);
                } catch (UnSupportedFillTypeException e) {
                  // ignored
                }
                rowRecord
                    .getFields()
                    .set(pathId, Field.getField(filledPair.getValue().getValue(), resultDataType));
              } else {
                // TODO: Query previous then calculate
              }
            } else {
              previousValue[pathId] = field.getObjectValue(resultDataType);
              previousTime[pathId] = curTime;
            }
          }
        }
      }

      cachedRowRecords.addAll(unFilledRowRecords);
      unFilledRowRecords.clear();
      return true;
    } else {
      return false;
    }
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public RowRecord nextWithoutConstraint() throws IOException {
    if (cachedRowRecords.size() == 0) {
      throw new IOException(
          "need to call hasNext() before calling next() " + "in GroupByFillDataSet.");
    }

    return cachedRowRecords.removeFirst();
  }

  //  @Override
  //  public boolean hasNextWithoutConstraint() {
  //    return groupByEngineDataSet.hasNextWithoutConstraint();
  //  }
  //
  //  @Override
  //  @SuppressWarnings("squid:S3776")
  //  public RowRecord nextWithoutConstraint() throws IOException {
  //    RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();
  //
  //    for (int i = 0; i < paths.size(); i++) {
  //      Field field = rowRecord.getFields().get(i);
  //      // current group by result is null
  //      if (field == null || field.getDataType() == null) {
  //        TSDataType tsDataType = dataTypes.get(i);
  //        // for desc query peek previous time and value
  //        if (!ascending && !isPeekEnded && !canUseCacheData(rowRecord, tsDataType, i)) {
  //          fillCache(i);
  //        }
  //
  //        if (canUseCacheData(rowRecord, tsDataType, i)) {
  //          rowRecord.getFields().set(i, Field.getField(previousValue[i], tsDataType));
  //        }
  //      } else {
  //        // use now value update previous value
  //        previousValue[i] = field.getObjectValue(field.getDataType());
  //        previousTime[i] = rowRecord.getTimestamp();
  //      }
  //    }
  //    return rowRecord;
  //  }

  private TSDataType getResultDataType(int pathId) {
    switch (aggregations.get(pathId)) {
      case "avg":
      case "sum":
        return TSDataType.DOUBLE;
      case "count":
      case "max_time":
      case "min_time":
        return TSDataType.INT64;
      case "first_value":
      case "last_value":
      case "max_value":
      case "min_value":
        return dataTypes.get(pathId);
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in group by fill : %s", aggregations.get(pathId)));
    }
  }

  private void fillCache(int i) throws IOException {
    Pair<Long, Object> data = groupByEngineDataSet.peekNextNotNullValue(paths.get(i), i);
    if (data == null) {
      isPeekEnded = true;
      previousTime[i] = Long.MIN_VALUE;
      previousValue[i] = null;
      if (!firstCacheIsEmpty(i)) {
        previousValue[i] = firstNotNullTV[i].getValue().getValue();
        previousTime[i] = firstNotNullTV[i].getTimestamp();
      }
    } else {
      previousValue[i] = data.right;
      previousTime[i] = data.left;
    }
  }

  // the previous value is not null
  // and (fill type is not previous until last or now time is before last time)
  // and (previous before range is not limited or previous before range contains the previous
  // interval)
  private boolean canUseCacheData(RowRecord rowRecord, TSDataType tsDataType, int i) {
    PreviousFill previousFill = (PreviousFill) fillTypes.get(tsDataType);
    return !cacheIsEmpty(i)
        && satisfyTime(rowRecord, tsDataType, previousFill, lastTimeArray[i])
        && satisfyRange(tsDataType, previousFill)
        && isIncreasingTime(rowRecord, previousTime[i]);
  }

  private boolean isIncreasingTime(RowRecord rowRecord, long time) {
    return rowRecord.getTimestamp() >= time;
  }

  private boolean satisfyTime(
      RowRecord rowRecord, TSDataType tsDataType, PreviousFill previousFill, long lastTime) {
    return (fillTypes.containsKey(tsDataType) && !previousFill.isUntilLast())
        || rowRecord.getTimestamp() <= lastTime;
  }

  private boolean satisfyRange(TSDataType tsDataType, PreviousFill previousFill) {
    return !fillTypes.containsKey(tsDataType)
        || previousFill.getBeforeRange() < 0
        || previousFill.getBeforeRange() >= groupByEngineDataSet.interval;
  }

  private boolean cacheIsEmpty(int i) {
    return previousValue[i] == null;
  }

  private boolean firstCacheIsEmpty(int i) {
    return firstNotNullTV[i] == null || firstNotNullTV[i].getValue() == null;
  }
}
