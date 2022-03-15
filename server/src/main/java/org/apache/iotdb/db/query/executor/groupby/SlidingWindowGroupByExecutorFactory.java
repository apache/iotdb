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

package org.apache.iotdb.db.query.executor.groupby;

import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.executor.groupby.impl.EmptyQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.MonotonicQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.NormalQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.SmoothQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Comparator;

public class SlidingWindowGroupByExecutorFactory {

  /** comparators used for MonotonicQueueSlidingWindowGroupByExecutor */

  // return a value greater than 0 if o1 is numerically greater than o2
  private static final Comparator<AggregateResult>[] maxComparators =
      new Comparator[] {
        Comparator.comparingInt(AggregateResult::getIntValue),
        Comparator.comparingLong(AggregateResult::getLongValue),
        Comparator.comparing(AggregateResult::getFloatValue),
        Comparator.comparingDouble(AggregateResult::getDoubleValue)
      };

  // return a value greater than 0 if o1 is numerically less than o2
  private static final Comparator<AggregateResult>[] minComparators =
      new Comparator[] {
        maxComparators[0].reversed(),
        maxComparators[1].reversed(),
        maxComparators[2].reversed(),
        maxComparators[3].reversed()
      };

  // return a value greater than 0 if abs(o1) is numerically greater than abs(o2)
  // if abs(o1) == abs(o2), return a value greater than 0 if o1 is numerically greater than o2
  private static final Comparator<AggregateResult>[] extremeComparators =
      new Comparator[] {
        Comparator.comparingInt(AggregateResult::getIntAbsValue)
            .thenComparingInt(AggregateResult::getIntValue),
        Comparator.comparingLong(AggregateResult::getLongAbsValue)
            .thenComparingLong(AggregateResult::getLongValue),
        Comparator.comparing(AggregateResult::getFloatAbsValue)
            .thenComparing(AggregateResult::getFloatValue),
        Comparator.comparingDouble(AggregateResult::getDoubleAbsValue)
            .thenComparingDouble(AggregateResult::getDoubleValue)
      };

  public static SlidingWindowGroupByExecutor getSlidingWindowGroupByExecutor(
      String aggrFuncName, TSDataType dataType, boolean ascending) {
    if (aggrFuncName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.SUM:
      case SQLConstant.AVG:
      case SQLConstant.COUNT:
        return new SmoothQueueSlidingWindowGroupByExecutor(dataType, aggrFuncName, ascending);
      case SQLConstant.MAX_VALUE:
        return new MonotonicQueueSlidingWindowGroupByExecutor(
            dataType, aggrFuncName, ascending, maxComparators[dataType.ordinal() - 1]);
      case SQLConstant.MIN_VALUE:
        return new MonotonicQueueSlidingWindowGroupByExecutor(
            dataType, aggrFuncName, ascending, minComparators[dataType.ordinal() - 1]);
      case SQLConstant.EXTREME:
        return new MonotonicQueueSlidingWindowGroupByExecutor(
            dataType, aggrFuncName, ascending, extremeComparators[dataType.ordinal() - 1]);
      case SQLConstant.MIN_TIME:
      case SQLConstant.FIRST_VALUE:
        return !ascending
            ? new EmptyQueueSlidingWindowGroupByExecutor(dataType, aggrFuncName, ascending)
            : new NormalQueueSlidingWindowGroupByExecutor(dataType, aggrFuncName, ascending);
      case SQLConstant.MAX_TIME:
      case SQLConstant.LAST_VALUE:
        return !ascending
            ? new NormalQueueSlidingWindowGroupByExecutor(dataType, aggrFuncName, ascending)
            : new EmptyQueueSlidingWindowGroupByExecutor(dataType, aggrFuncName, ascending);
      default:
        throw new IllegalArgumentException("Invalid Aggregation Type: " + aggrFuncName);
    }
  }
}
