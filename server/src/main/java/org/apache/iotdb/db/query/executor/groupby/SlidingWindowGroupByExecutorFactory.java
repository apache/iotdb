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
import org.apache.iotdb.db.query.executor.groupby.impl.EmptyQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.MonotonicQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.NormalQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.db.query.executor.groupby.impl.SmoothQueueSlidingWindowGroupByExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SlidingWindowGroupByExecutorFactory {

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
            dataType, aggrFuncName, ascending, (o1, o2) -> ((Comparable<Object>) o1).compareTo(o2));
      case SQLConstant.MIN_VALUE:
        return new MonotonicQueueSlidingWindowGroupByExecutor(
            dataType, aggrFuncName, ascending, (o1, o2) -> ((Comparable<Object>) o2).compareTo(o1));
      case SQLConstant.EXTREME:
        return new MonotonicQueueSlidingWindowGroupByExecutor(
            dataType,
            aggrFuncName,
            ascending,
            (o1, o2) -> {
              TSDataType resultDataType = getResultDataType(dataType, aggrFuncName);
              Comparable<Object> extVal = (Comparable<Object>) o1;
              Comparable<Object> absExtVal = (Comparable<Object>) getAbsValue(o1, resultDataType);
              Comparable<Object> candidateResult = (Comparable<Object>) o2;
              Comparable<Object> absCandidateResult =
                  (Comparable<Object>) getAbsValue(o2, resultDataType);
              if (absExtVal.compareTo(absCandidateResult) == 0) {
                return extVal.compareTo(candidateResult);
              } else {
                return absExtVal.compareTo(absCandidateResult);
              }
            });
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

  private static Object getAbsValue(Object v, TSDataType resultDataType) {
    switch (resultDataType) {
      case DOUBLE:
        return Math.abs((Double) v);
      case FLOAT:
        return Math.abs((Float) v);
      case INT32:
        return Math.abs((Integer) v);
      case INT64:
        return Math.abs((Long) v);
      default:
        throw new UnSupportedDataTypeException(java.lang.String.valueOf(resultDataType));
    }
  }

  private static TSDataType getResultDataType(TSDataType dataType, String aggrFuncName) {
    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
      case SQLConstant.MAX_TIME:
      case SQLConstant.COUNT:
        return TSDataType.INT64;
      case SQLConstant.FIRST_VALUE:
      case SQLConstant.LAST_VALUE:
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      case SQLConstant.EXTREME:
        return dataType;
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }
}
