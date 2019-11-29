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

package org.apache.iotdb.db.query.factory;

import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.FirstValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.LastValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MinTimeAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.SumAggrFunc;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Easy factory pattern to build AggregateFunction.
 */
public class AggreFuncFactory {

  private AggreFuncFactory() {
  }

  /**
   * construct AggregateFunction using factory pattern.
   *
   * @param aggrFuncName function name.
   * @param dataType data type.
   */
  public static AggregateFunction getAggrFuncByName(String aggrFuncName, TSDataType dataType)
      throws PathException {
    if (aggrFuncName == null) {
      throw new PathException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case StatisticConstant.MIN_TIME:
        return new MinTimeAggrFunc();
      case StatisticConstant.MAX_TIME:
        return new MaxTimeAggrFunc();
      case StatisticConstant.MIN_VALUE:
        return new MinValueAggrFunc(dataType);
      case StatisticConstant.MAX_VALUE:
        return new MaxValueAggrFunc(dataType);
      case StatisticConstant.COUNT:
        return new CountAggrFunc();
      case StatisticConstant.AVG:
        return new AvgAggrFunc(dataType);
      case StatisticConstant.FIRST_VALUE:
        return new FirstValueAggrFunc(dataType);
      case StatisticConstant.SUM:
        return new SumAggrFunc(dataType);
      case StatisticConstant.LAST_VALUE:
        return new LastValueAggrFunc(dataType);
      default:
        throw new PathException(
            "aggregate does not support " + aggrFuncName + " function.");
    }
  }
}
