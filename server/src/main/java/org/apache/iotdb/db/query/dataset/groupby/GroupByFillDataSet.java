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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
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
    Arrays.fill(lastTimeArray, Long.MIN_VALUE);
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
            // LinearFill and PreviousUntilLastFill and order by time desc
            // needs to be filled with data at the next non-empty time
            if (fill instanceof PreviousFill) {
              if (!this.ascending) {
                filledFlag = false;
              } else {
                filledFlag = !((PreviousFill) fill).isUntilLast();
              }
            } else {
              filledFlag = false;
            }
          } else {
            filledFlag = true;
            lastTimeArray[pathId] = Math.max(lastTimeArray[pathId], rowRecord.getTimestamp());
          }
          isFilled.set(pathId, filledFlag);
        }

        unFilledRowRecords.add(rowRecord);
        if (isFilled.cardinality() == paths.size()) {
          break;
        }
      }

      if (!this.ascending) {
        Collections.reverse(unFilledRowRecords);
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
          long firstTime = Long.MAX_VALUE;
          Object firstValue = null;
          for (int curId = 0; curId < unFilledRowRecords.size(); curId++) {
            RowRecord rowRecord = unFilledRowRecords.get(curId);
            Field field = rowRecord.getFields().get(pathId);
            long curTime = rowRecord.getTimestamp();
            if (isFieldNull(field)) {
              while (fieldLessOrNull(curId, nextId, pathId)) {
                nextId++;
              }
              LinearFill linearFill = new LinearFill();
              TimeValuePair filledPair = null;
              TimeValuePair beforePair = null;
              TimeValuePair afterPair = null;

              if (nextId == unFilledRowRecords.size()) {
                if (nextValue[pathId] != null && nextTime[pathId] > curTime) {
                  afterPair =
                      new TimeValuePair(
                          nextTime[pathId],
                          TsPrimitiveType.getByType(resultDataType, nextValue[pathId]));
                } else {
                  // TODO: Query next then calculate
                }
              } else {
                RowRecord nextRowRecord = unFilledRowRecords.get(nextId);
                Field nextField = nextRowRecord.getFields().get(pathId);
                afterPair =
                    new TimeValuePair(
                        nextRowRecord.getTimestamp(),
                        TsPrimitiveType.getByType(
                            resultDataType, nextField.getObjectValue(resultDataType)));
              }

              if (previousValue[pathId] != null && previousTime[pathId] < curTime) {
                beforePair =
                    new TimeValuePair(
                        previousTime[pathId],
                        TsPrimitiveType.getByType(resultDataType, previousValue[pathId]));
              } else {
                // TODO: Query previous then calculate
              }

              if (beforePair != null && afterPair != null) {
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
              }
            } else {
              previousValue[pathId] = field.getObjectValue(resultDataType);
              previousTime[pathId] = curTime;
              if (curTime < firstTime) {
                firstTime = curTime;
                firstValue = field.getObjectValue(resultDataType);
              }
            }
          }

          if (!this.ascending) {
            nextTime[pathId] = firstTime;
            nextValue[pathId] = firstValue;
          }
        }
      }

      if (!this.ascending) {
        Collections.reverse(unFilledRowRecords);
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
}