package org.apache.iotdb.db.query.dataset.groupby;

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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GroupByFillDataSet extends GroupByEngineDataSet {

  private GroupByEngineDataSet dataSet;

  private final Map<TSDataType, IFill> fillTypes;
  private final IFill singleFill;
  private final List<String> aggregations;

  // the result datatype for each time series
  private final TSDataType[] resultDataType;

  // the last value and time for each time series
  private long[] previousTimes;
  private Object[] previousValues;

  // the next value and time for each time series
  private int[] nextIds;
  private List<ElasticSerializableTVList> nextTVLists;

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
      switch (aggregations.get(i)) {
        case "avg":
        case "sum":
          resultDataType[i] = TSDataType.DOUBLE;
          break;
        case "count":
        case "max_time":
        case "min_time":
          resultDataType[i] = TSDataType.INT64;
          break;
        case "first_value":
        case "last_value":
        case "max_value":
        case "min_value":
          resultDataType[i] = dataTypes.get(i);
          break;
        default:
          throw new QueryProcessException("unknown aggregation type, please update this code!");
      }
    }

    previousTimes = new long[aggregations.size()];
    previousValues = new Object[aggregations.size()];
    nextIds = new int[aggregations.size()];
    Arrays.fill(previousTimes, Long.MAX_VALUE);
    Arrays.fill(previousValues, null);
    Arrays.fill(nextIds, 0);

    nextTVLists = new ArrayList<>();
    for (int i = 0; i < aggregations.size(); i++) {
      nextTVLists.add(
          ElasticSerializableTVList.newElasticSerializableTVList(
              resultDataType[i], context.getQueryId(), 10, fetchSize));
    }
  }

  public void setDataSet(GroupByEngineDataSet dataSet) {
    this.dataSet = dataSet;
  }

  public void initCache() throws IOException, QueryProcessException {
    BitSet cacheSet = new BitSet(aggregations.size());
    cacheSet.clear();
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
      if (nextTVLists.get(i).size() == nextIds[i]) {
        fillRecord(i, record);
        continue;
      }

      long cacheTime = nextTVLists.get(i).getTime(nextIds[i]);
      if (cacheTime == curTimestamp) {
        record.addField(getNextCacheValue(i), resultDataType[i]);
      } else {
        fillRecord(i, record);
      }
    }

    try {
      slideCache(record.getTimestamp());
    } catch (QueryProcessException e) {
      // ignored
    }

    return record;
  }

  private void fillRecord(int index, RowRecord record) throws IOException {
    // Don't fill count aggregation
    if (Objects.equals(aggregations.get(index), "count")) {
      record.addField((long) 0, TSDataType.INT64);
      return;
    }

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
      if (nextIds[index] < nextTVLists.get(index).size()) {
        afterPair =
            new Pair<>(nextTVLists.get(index).getTime(nextIds[index]), getNextCacheValue(index));
      } else {
        afterPair = null;
      }
    } else {
      if (nextIds[index] < nextTVLists.get(index).size()) {
        beforePair =
            new Pair<>(nextTVLists.get(index).getTime(nextIds[index]), getNextCacheValue(index));
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
          // Don't fill and ignore exception
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
      } catch (QueryProcessException | StorageEngineException e) {
        throw new IOException(e);
      }
    }
  }

  private Object getNextCacheValue(int index) throws IOException {
    switch (resultDataType[index]) {
      case INT32:
        return nextTVLists.get(index).getInt(nextIds[index]);
      case INT64:
        return nextTVLists.get(index).getLong(nextIds[index]);
      case FLOAT:
        return nextTVLists.get(index).getFloat(nextIds[index]);
      case DOUBLE:
        return nextTVLists.get(index).getDouble(nextIds[index]);
      case BOOLEAN:
        return nextTVLists.get(index).getBoolean(nextIds[index]);
      case TEXT:
        return nextTVLists.get(index).getString(nextIds[index]);
      default:
        throw new IOException("unknown data type!");
    }
  }

  private void slideCache(long curTimestamp) throws IOException, QueryProcessException {
    BitSet slideSet = new BitSet(aggregations.size());
    slideSet.clear();
    for (int i = 0; i < aggregations.size(); i++) {
      if (nextTVLists.get(i).size() == nextIds[i]) {
        continue;
      }

      // slide cache when the current TV is used
      if (nextTVLists.get(i).getTime(nextIds[i]) == curTimestamp) {
        previousTimes[i] = curTimestamp;
        previousValues[i] = getNextCacheValue(i);
        nextIds[i] += 1;
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
      }
    }
  }
}
