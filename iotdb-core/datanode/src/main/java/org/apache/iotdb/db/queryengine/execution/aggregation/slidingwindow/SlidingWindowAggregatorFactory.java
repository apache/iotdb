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

package org.apache.iotdb.db.queryengine.execution.aggregation.slidingwindow;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class SlidingWindowAggregatorFactory {

  private SlidingWindowAggregatorFactory() {}

  /** comparators used for MonotonicQueueSlidingWindowAggregator. */
  private static final Map<TSDataType, Comparator<Column>> maxComparators =
      new EnumMap<>(TSDataType.class);

  private static final Map<TSDataType, Comparator<Column>> minComparators =
      new EnumMap<>(TSDataType.class);
  private static final Map<TSDataType, Comparator<Column>> extremeComparators =
      new EnumMap<>(TSDataType.class);

  private static final Map<TSDataType, Comparator<Column>> maxByComparators =
      new EnumMap<>(TSDataType.class);

  private static final Map<TSDataType, Comparator<Column>> minByComparators =
      new EnumMap<>(TSDataType.class);

  static {
    // return a value greater than 0 if o1 is numerically greater than o2
    maxComparators.put(TSDataType.INT32, Comparator.comparingInt(o -> o.getInt(0)));
    maxComparators.put(TSDataType.INT64, Comparator.comparingLong(o -> o.getLong(0)));
    maxComparators.put(TSDataType.FLOAT, Comparator.comparing(o -> o.getFloat(0)));
    maxComparators.put(TSDataType.DOUBLE, Comparator.comparingDouble(o -> o.getDouble(0)));

    // return a value greater than 0 if o1 is numerically less than o2
    minComparators.put(TSDataType.INT32, (o1, o2) -> Integer.compare(o2.getInt(0), o1.getInt(0)));
    minComparators.put(TSDataType.INT64, (o1, o2) -> Long.compare(o2.getLong(0), o1.getLong(0)));
    minComparators.put(TSDataType.FLOAT, (o1, o2) -> Float.compare(o2.getFloat(0), o1.getFloat(0)));
    minComparators.put(
        TSDataType.DOUBLE, (o1, o2) -> Double.compare(o2.getDouble(0), o1.getDouble(0)));

    // return a value greater than 0 if abs(o1) is numerically greater than abs(o2)
    // if abs(o1) == abs(o2), return a value greater than 0 if o1 is numerically greater than o2
    extremeComparators.put(
        TSDataType.INT32,
        (o1, o2) -> {
          int value1 = o1.getInt(0);
          int value2 = o2.getInt(0);
          if (Math.abs(value1) > Math.abs(value2)
              || (Math.abs(value1) == Math.abs(value2) && value1 > value2)) {
            return 1;
          } else if (value1 == value2) {
            return 0;
          }
          return -1;
        });
    extremeComparators.put(
        TSDataType.INT64,
        (o1, o2) -> {
          long value1 = o1.getLong(0);
          long value2 = o2.getLong(0);
          if (Math.abs(value1) > Math.abs(value2)
              || (Math.abs(value1) == Math.abs(value2) && value1 > value2)) {
            return 1;
          } else if (value1 == value2) {
            return 0;
          }
          return -1;
        });
    extremeComparators.put(
        TSDataType.FLOAT,
        (o1, o2) -> {
          float value1 = o1.getFloat(0);
          float value2 = o2.getFloat(0);
          if (Math.abs(value1) > Math.abs(value2)
              || (Math.abs(value1) == Math.abs(value2) && value1 > value2)) {
            return 1;
          } else if (value1 == value2) {
            return 0;
          }
          return -1;
        });
    extremeComparators.put(
        TSDataType.DOUBLE,
        (o1, o2) -> {
          double value1 = o1.getDouble(0);
          double value2 = o2.getDouble(0);
          if (Math.abs(value1) > Math.abs(value2)
              || (Math.abs(value1) == Math.abs(value2) && value1 > value2)) {
            return 1;
          } else if (value1 == value2) {
            return 0;
          }
          return -1;
        });
    // Intermediate Value of maxBy is a byte array: | y | xNull | x |
    maxByComparators.put(
        TSDataType.INT32,
        Comparator.comparingInt(
            o -> BytesUtils.bytesToInt(o.getBinary(0).getValues(), Long.BYTES)));
    maxByComparators.put(
        TSDataType.INT64,
        Comparator.comparingLong(
            o ->
                BytesUtils.bytesToLongFromOffset(
                    o.getBinary(0).getValues(), Long.BYTES, Long.BYTES)));
    maxByComparators.put(
        TSDataType.FLOAT,
        Comparator.comparing(o -> BytesUtils.bytesToFloat(o.getBinary(0).getValues(), Long.BYTES)));
    maxByComparators.put(
        TSDataType.DOUBLE,
        Comparator.comparingDouble(
            o -> BytesUtils.bytesToDouble(o.getBinary(0).getValues(), Long.BYTES)));

    // return a value greater than 0 if o1 is numerically less than o2
    minByComparators.put(
        TSDataType.INT32,
        (o1, o2) ->
            Integer.compare(
                BytesUtils.bytesToInt(o2.getBinary(0).getValues(), Long.BYTES),
                BytesUtils.bytesToInt(o1.getBinary(0).getValues(), Long.BYTES)));
    minByComparators.put(
        TSDataType.INT64,
        (o1, o2) ->
            Long.compare(
                BytesUtils.bytesToLongFromOffset(
                    o2.getBinary(0).getValues(), Long.BYTES, Long.BYTES),
                BytesUtils.bytesToLongFromOffset(
                    o1.getBinary(0).getValues(), Long.BYTES, Long.BYTES)));
    minByComparators.put(
        TSDataType.FLOAT,
        (o1, o2) ->
            Float.compare(
                BytesUtils.bytesToFloat(o2.getBinary(0).getValues(), Long.BYTES),
                BytesUtils.bytesToFloat(o1.getBinary(0).getValues(), Long.BYTES)));
    minByComparators.put(
        TSDataType.DOUBLE,
        (o1, o2) ->
            Double.compare(
                BytesUtils.bytesToDouble(o2.getBinary(0).getValues(), Long.BYTES),
                BytesUtils.bytesToDouble(o1.getBinary(0).getValues(), Long.BYTES)));
  }

  public static SlidingWindowAggregator createSlidingWindowAggregator(
      String functionName,
      TAggregationType aggregationType,
      List<TSDataType> dataTypes,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending,
      List<InputLocation[]> inputLocationList,
      AggregationStep step) {
    Accumulator accumulator =
        AccumulatorFactory.createAccumulator(
            functionName,
            aggregationType,
            dataTypes,
            inputExpressions,
            inputAttributes,
            ascending,
            step.isInputRaw());
    switch (aggregationType) {
      case SUM:
      case AVG:
      case COUNT:
      case COUNT_TIME:
      case STDDEV:
      case STDDEV_POP:
      case STDDEV_SAMP:
      case VARIANCE:
      case VAR_POP:
      case VAR_SAMP:
      case UDAF: // Currently UDAF belongs to SmoothQueueSlidingWindowAggregator
        return new SmoothQueueSlidingWindowAggregator(accumulator, inputLocationList, step);
      case MAX_VALUE:
        return new MonotonicQueueSlidingWindowAggregator(
            accumulator, inputLocationList, step, maxComparators.get(dataTypes.get(0)));
      case MIN_VALUE:
        return new MonotonicQueueSlidingWindowAggregator(
            accumulator, inputLocationList, step, minComparators.get(dataTypes.get(0)));
      case EXTREME:
        return new MonotonicQueueSlidingWindowAggregator(
            accumulator, inputLocationList, step, extremeComparators.get(dataTypes.get(0)));
      case MIN_TIME:
      case FIRST_VALUE:
        return !ascending
            ? new EmptyQueueSlidingWindowAggregator(accumulator, inputLocationList, step)
            : new NormalQueueSlidingWindowAggregator(accumulator, inputLocationList, step);
      case MAX_TIME:
      case LAST_VALUE:
        return !ascending
            ? new NormalQueueSlidingWindowAggregator(accumulator, inputLocationList, step)
            : new EmptyQueueSlidingWindowAggregator(accumulator, inputLocationList, step);
      case MAX_BY:
        return new MonotonicQueueSlidingWindowAggregator(
            accumulator, inputLocationList, step, maxByComparators.get(dataTypes.get(1)));
      case MIN_BY:
        return new MonotonicQueueSlidingWindowAggregator(
            accumulator, inputLocationList, step, minByComparators.get(dataTypes.get(1)));
      case COUNT_IF:
        throw new SemanticException("COUNT_IF with slidingWindow is not supported now");
      case TIME_DURATION:
        throw new SemanticException("TIME_DURATION with slidingWindow is not supported now");
      case MODE:
        throw new SemanticException("MODE with slidingWindow is not supported now");
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + aggregationType);
    }
  }
}
