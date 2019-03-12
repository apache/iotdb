/**
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

package org.apache.iotdb.db.query.aggregation;

import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.FirstAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MeanAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MinTimeAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.SumAggrFunc;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Easy factory pattern to build AggregateFunction.
 */
public class AggreFuncFactory {

  /**
   * construct AggregateFunction using factory pattern.
   * @param aggrFuncName function name.
   * @param dataType data type.
   * @return
   * @throws ProcessorException
   */
  public static AggregateFunction getAggrFuncByName(String aggrFuncName, TSDataType dataType)
      throws ProcessorException {
    if (aggrFuncName == null) {
      throw new ProcessorException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case AggregationConstant.MIN_TIME:
        return new MinTimeAggrFunc();
      case AggregationConstant.MAX_TIME:
        return new MaxTimeAggrFunc();
      case AggregationConstant.MIN_VALUE:
        return new MinValueAggrFunc(dataType);
      case AggregationConstant.MAX_VALUE:
        return new MaxValueAggrFunc(dataType);
      case AggregationConstant.COUNT:
        return new CountAggrFunc();
      case AggregationConstant.MEAN:
        return new MeanAggrFunc(dataType);
      case AggregationConstant.FIRST:
        return new FirstAggrFunc(dataType);
      case AggregationConstant.SUM:
        return new SumAggrFunc(dataType);
      case AggregationConstant.LAST:
        return new LastAggrFunc(dataType);
      default:
        throw new ProcessorException(
            "aggregate does not support " + aggrFuncName + " function.");
    }
  }
}
