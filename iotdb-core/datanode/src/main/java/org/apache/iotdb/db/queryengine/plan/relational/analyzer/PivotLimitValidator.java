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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.math.BigInteger;
import java.util.List;

/** Checks the final output width before constructing any value combinations or result symbols. */
public final class PivotLimitValidator {
  private PivotLimitValidator() {}

  public static BigInteger validate(
      int groupCount, int aggregateCount, List<Integer> valueCounts, int limit) {
    BigInteger combinations = BigInteger.ONE;
    for (int valueCount : valueCounts) {
      combinations = combinations.multiply(BigInteger.valueOf(valueCount));
    }
    BigInteger total =
        BigInteger.valueOf(groupCount)
            .add(combinations.multiply(BigInteger.valueOf(aggregateCount)));
    if (total.compareTo(BigInteger.valueOf(limit)) > 0) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_PIVOT_OUTPUT_COLUMN_COUNT_ARG_EXCEEDS_PIVOT_LIMIT_ARG_644AC9B8,
              total,
              limit));
    }
    return total;
  }
}
