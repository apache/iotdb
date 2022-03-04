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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.executor.fill.ValueFill;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class GroupByFillDataSet extends GroupByEngineDataSet {

  private static final Logger logger = LoggerFactory.getLogger(GroupByFillDataSet.class);

  private QueryDataSet dataSet;

  private final Map<TSDataType, IFill> fillTypes;
  private final IFill singleFill;
  private final List<String> aggregations;

  // the result datatype for each aggregation
  private final TSDataType[] resultDataType;

  // the last value and time for each aggregation
  private long[] previousTimes;
  private Object[] previousValues;

  // the next not null and unused rowId for each aggregation
  private int[] nextIndices;
  // the next value and time for each aggregation
  private List<ElasticSerializableTVList> nextTVLists;

  private final float groupByFillCacheSizeInMB =
      IoTDBDescriptor.getInstance().getConfig().getGroupByFillCacheSizeInMB();

  public GroupByFillDataSet(QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException {
    super(context, groupByTimeFillPlan);
    this.aggregations = groupByTimeFillPlan.getDeduplicatedAggregations();
    this.fillTypes = groupByTimeFillPlan.getFillType();
    this.singleFill = groupByTimeFillPlan.getSingleFill();

    this.resultDataType = new TSDataType[aggregations.size()];
    initArrays(context);
  }

  private void initArrays(QueryContext context) throws QueryProcessException {
    for (int i = 0; i < aggregations.size(); i++) {
      resultDataType[i] = TypeInferenceUtils.getAggrDataType(aggregations.get(i), dataTypes.get(i));
    }

    previousTimes = new long[aggregations.size()];
    previousValues = new Object[aggregations.size()];
    nextIndices = new int[aggregations.size()];
    Arrays.fill(previousTimes, Long.MAX_VALUE);
    Arrays.fill(previousValues, null);
    Arrays.fill(nextIndices, 0);

    nextTVLists = new ArrayList<>(aggregations.size());
    for (int i = 0; i < aggregations.size(); i++) {
      nextTVLists.add(
          ElasticSerializableTVList.newElasticSerializableTVList(
              resultDataType[i], context.getQueryId(), groupByFillCacheSizeInMB, 2));
    }
  }

  public void setDataSet(QueryDataSet dataSet) {
    this.dataSet = dataSet;
  }

  public void initCache() throws QueryProcessException {
    BitSet cacheSet = new BitSet(aggregations.size());
    try {
      while (cacheSet.cardinality() < aggregations.size() && dataSet.hasNextWithoutConstraint()) {
        RowRecord record = dataSet.nextWithoutConstraint();
        long timestamp = record.getTimestamp();
        List<Field> fields = record.getFields();
        for (int i = 0; i < fields.size(); i++) {
          Field field = fields.get(i);
          if (field == null) {
            continue;
          }

          if (ascending && timestamp < startTime) {
            previousTimes[i] = timestamp;
            previousValues[i] = field.getObjectValue(resultDataType[i]);
          } else if (!ascending && timestamp >= endTime) {
            previousTimes[i] = timestamp;
            previousValues[i] = field.getObjectValue(resultDataType[i]);
          } else {
            nextTVLists.get(i).put(timestamp, field.getObjectValue(resultDataType[i]));
            cacheSet.set(i);
          }
        }
      }
    } catch (IOException e) {
      logger.error("there has an exception while init: ", e);
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() " + "in GroupByFillDataSet.");
    }

    hasCachedTimeInterval = false;
    RowRecord record;
    long curTimestamp;
    if (leftCRightO) {
      curTimestamp = curStartTime;
      record = new RowRecord(curStartTime);
    } else {
      curTimestamp = curEndTime - 1;
      record = new RowRecord(curEndTime - 1);
    }

    for (int i = 0; i < aggregations.size(); i++) {
      if (nextTVLists.get(i).size() == nextIndices[i]) {
        fillRecord(i, record);
        continue;
      }

      long cacheTime = nextTVLists.get(i).getTime(nextIndices[i]);
      if (cacheTime == curTimestamp) {
        record.addField(getNextCacheValue(i), resultDataType[i]);
      } else {
        fillRecord(i, record);
      }
    }

    try {
      slideCache(record.getTimestamp());
    } catch (QueryProcessException e) {
      logger.warn("group by fill has an exception while sliding: ", e);
    }

    return record;
  }

  private void fillRecord(int index, RowRecord record) throws IOException {
    IFill fill;
    if (fillTypes != null) {
      // old type fill logic
      fill = fillTypes.get(resultDataType[index]);
    } else {
      fill = singleFill;
    }
    if (fill == null) {
      record.addField(null);
      return;
    }

    Pair<Long, Object> beforePair, afterPair;
    if (ascending) {
      if (previousValues[index] != null) {
        beforePair = new Pair<>(previousTimes[index], previousValues[index]);
      } else {
        beforePair = null;
      }
      if (nextIndices[index] < nextTVLists.get(index).size()) {
        afterPair =
            new Pair<>(
                nextTVLists.get(index).getTime(nextIndices[index]), getNextCacheValue(index));
      } else {
        afterPair = null;
      }
    } else {
      if (nextIndices[index] < nextTVLists.get(index).size()) {
        beforePair =
            new Pair<>(
                nextTVLists.get(index).getTime(nextIndices[index]), getNextCacheValue(index));
      } else {
        beforePair = null;
      }
      if (previousValues[index] != null) {
        afterPair = new Pair<>(previousTimes[index], previousValues[index]);
      } else {
        afterPair = null;
      }
    }

    if (fill instanceof PreviousFill) {
      if (beforePair != null
          && (fill.getBeforeRange() == -1
              || fill.insideBeforeRange(beforePair.left, record.getTimestamp()))
          && ((!((PreviousFill) fill).isUntilLast())
              || (afterPair != null && afterPair.left < endTime))) {
        record.addField(beforePair.right, resultDataType[index]);
      } else {
        record.addField(null);
      }
    } else if (fill instanceof LinearFill) {
      LinearFill linearFill = new LinearFill();
      if (beforePair != null
          && afterPair != null
          && (fill.getBeforeRange() == -1
              || fill.insideBeforeRange(beforePair.left, record.getTimestamp()))
          && (fill.getAfterRange() == -1
              || fill.insideAfterRange(afterPair.left, record.getTimestamp()))) {
        try {
          TimeValuePair filledPair =
              linearFill.averageWithTimeAndDataType(
                  new TimeValuePair(
                      beforePair.left,
                      TsPrimitiveType.getByType(resultDataType[index], beforePair.right)),
                  new TimeValuePair(
                      afterPair.left,
                      TsPrimitiveType.getByType(resultDataType[index], afterPair.right)),
                  record.getTimestamp(),
                  resultDataType[index]);
          record.addField(filledPair.getValue().getValue(), resultDataType[index]);
        } catch (UnSupportedFillTypeException e) {
          // Don't fill and ignore unsupported type exception
          record.addField(null);
        }
      } else {
        record.addField(null);
      }
    } else if (fill instanceof ValueFill) {
      try {
        TimeValuePair filledPair = fill.getFillResult();
        if (filledPair == null) {
          filledPair = ((ValueFill) fill).getSpecifiedFillResult(resultDataType[index]);
        }
        record.addField(filledPair.getValue().getValue(), resultDataType[index]);
      } catch (NumberFormatException ne) {
        // Don't fill and ignore type convert exception
        record.addField(null);
      } catch (QueryProcessException | StorageEngineException e) {
        throw new IOException(e);
      }
    }
  }

  private Object getNextCacheValue(int index) throws IOException {
    switch (resultDataType[index]) {
      case INT32:
        return nextTVLists.get(index).getInt(nextIndices[index]);
      case INT64:
        return nextTVLists.get(index).getLong(nextIndices[index]);
      case FLOAT:
        return nextTVLists.get(index).getFloat(nextIndices[index]);
      case DOUBLE:
        return nextTVLists.get(index).getDouble(nextIndices[index]);
      case BOOLEAN:
        return nextTVLists.get(index).getBoolean(nextIndices[index]);
      case TEXT:
        return nextTVLists.get(index).getBinary(nextIndices[index]);
      default:
        throw new IOException("unknown data type!");
    }
  }

  private void slideCache(long curTimestamp) throws IOException, QueryProcessException {
    BitSet slideSet = new BitSet(aggregations.size());
    for (int i = 0; i < aggregations.size(); i++) {
      if (nextTVLists.get(i).size() == nextIndices[i]) {
        continue;
      }

      // slide cache when the current TV is used
      if (nextTVLists.get(i).getTime(nextIndices[i]) == curTimestamp) {
        previousTimes[i] = curTimestamp;
        previousValues[i] = getNextCacheValue(i);
        nextIndices[i]++;
        nextTVLists.get(i).setEvictionUpperBound(nextIndices[i]);
        slideSet.set(i);
      }
    }

    while (slideSet.cardinality() > 0 && dataSet.hasNextWithoutConstraint()) {
      RowRecord record = dataSet.nextWithoutConstraint();
      long timestamp = record.getTimestamp();
      List<Field> fields = record.getFields();
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        if (field == null) {
          continue;
        }
        nextTVLists.get(i).put(timestamp, field.getObjectValue(resultDataType[i]));
        slideSet.clear(i);
      }
    }
  }
}
