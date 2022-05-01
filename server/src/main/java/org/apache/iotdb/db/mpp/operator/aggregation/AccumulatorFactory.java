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

package org.apache.iotdb.db.mpp.operator.aggregation;

import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class AccumulatorFactory {

  // TODO: Are we going to create different seriesScanOperator based on order by sequence?
  public static Accumulator createAccumulator(
      AggregationType aggregationType, TSDataType tsDataType) {
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
        return new MaxTimeAccumulator();
      case MIN_TIME:
        return new MinTimeAccumulator();
      case MAX_VALUE:
        return new MaxValueAccumulator(tsDataType);
      case MIN_VALUE:
        return new MinValueAccumulator(tsDataType);
      case LAST_VALUE:
        return new LastValueAccumulator(tsDataType);
      case FIRST_VALUE:
        return new FirstValueAccumulator(tsDataType);
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggregationType);
    }
  }
}
