/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccumulatorFactory {

  // TODO: Are we going to create different seriesScanOperator based on order by sequence?
  public static Accumulator createAccumulator(
      TAggregationType aggregationType,
      TSDataType tsDataType,
      List<Expression> inputExpressions,
      Map<String, String> inputAttributes,
      boolean ascending) {
    switch (aggregationType) {
      case COUNT:
        return new CountAccumulator();
      case AVG:
        return new AvgAccumulator(tsDataType);
      case SUM:
        return new SumAccumulator(tsDataType);
      case EXTREME:
        return new ExtremeAccumulator(tsDataType);
      case MAX_TIME:
        return ascending ? new MaxTimeAccumulator() : new MaxTimeDescAccumulator();
      case MIN_TIME:
        return ascending ? new MinTimeAccumulator() : new MinTimeDescAccumulator();
      case MAX_VALUE:
        return new MaxValueAccumulator(tsDataType);
      case MIN_VALUE:
        return new MinValueAccumulator(tsDataType);
      case LAST_VALUE:
        return ascending
            ? new LastValueAccumulator(tsDataType)
            : new LastValueDescAccumulator(tsDataType);
      case FIRST_VALUE:
        return ascending
            ? new FirstValueAccumulator(tsDataType)
            : new FirstValueDescAccumulator(tsDataType);
      case COUNT_IF:
        return new CountIfAccumulator(inputExpressions, inputAttributes);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  // Old method to createAccumulator, please don't use it any more
  // keep here for compatibility of old Test before introduce of count_if
  @TestOnly
  @Deprecated
  public static Accumulator createAccumulator(
      TAggregationType aggregationType, TSDataType tsDataType, boolean ascending) {
    switch (aggregationType) {
      case COUNT:
        return new CountAccumulator();
      case AVG:
        return new AvgAccumulator(tsDataType);
      case SUM:
        return new SumAccumulator(tsDataType);
      case EXTREME:
        return new ExtremeAccumulator(tsDataType);
      case MAX_TIME:
        return ascending ? new MaxTimeAccumulator() : new MaxTimeDescAccumulator();
      case MIN_TIME:
        return ascending ? new MinTimeAccumulator() : new MinTimeDescAccumulator();
      case MAX_VALUE:
        return new MaxValueAccumulator(tsDataType);
      case MIN_VALUE:
        return new MinValueAccumulator(tsDataType);
      case LAST_VALUE:
        return ascending
            ? new LastValueAccumulator(tsDataType)
            : new LastValueDescAccumulator(tsDataType);
      case FIRST_VALUE:
        return ascending
            ? new FirstValueAccumulator(tsDataType)
            : new FirstValueDescAccumulator(tsDataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }

  public static List<Accumulator> createAccumulators(
      List<TAggregationType> aggregationTypes, TSDataType tsDataType, boolean ascending) {
    List<Accumulator> accumulators = new ArrayList<>();
    for (TAggregationType aggregationType : aggregationTypes) {
      accumulators.add(createAccumulator(aggregationType, tsDataType, ascending));
    }
    return accumulators;
  }
}
