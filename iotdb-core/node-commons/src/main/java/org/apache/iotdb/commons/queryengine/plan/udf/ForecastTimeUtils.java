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

package org.apache.iotdb.commons.queryengine.plan.udf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.QueryMessages;

import java.math.BigInteger;

public final class ForecastTimeUtils {

  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  private ForecastTimeUtils() {
    // Utility class.
  }

  public static long calculateForecastInterval(
      long inputStartTime, long inputEndTime, int inputSize, long outputInterval) {
    if (outputInterval > 0) {
      return outputInterval;
    }
    if (inputSize <= 1) {
      throw new SemanticException(
          QueryMessages
              .EXCEPTION_FORECAST_OUTPUT_INTERVAL_MUST_BE_POSITIVE_WHEN_INPUT_HAS_ONE_ROW_C9DBE14B);
    }

    BigInteger interval =
        BigInteger.valueOf(inputEndTime)
            .subtract(BigInteger.valueOf(inputStartTime))
            .divide(BigInteger.valueOf(inputSize - 1L));
    if (interval.compareTo(BIG_LONG_MAX) > 0) {
      throw new SemanticException(
          QueryMessages.EXCEPTION_FORECAST_OUTPUT_TIME_IS_OUT_OF_RANGE_0F114E4E);
    }
    return interval.longValue();
  }

  public static long calculateForecastStartTime(
      long inputEndTime, long outputStartTime, long interval) {
    if (outputStartTime != Long.MIN_VALUE) {
      return outputStartTime;
    }
    try {
      return Math.addExact(inputEndTime, interval);
    } catch (ArithmeticException e) {
      throw new SemanticException(
          QueryMessages.EXCEPTION_FORECAST_OUTPUT_TIME_IS_OUT_OF_RANGE_0F114E4E);
    }
  }

  public static long calculateForecastOutputTime(long outputStartTime, long interval, int index) {
    try {
      return Math.addExact(outputStartTime, Math.multiplyExact(interval, (long) index));
    } catch (ArithmeticException e) {
      throw new SemanticException(
          QueryMessages.EXCEPTION_FORECAST_OUTPUT_TIME_IS_OUT_OF_RANGE_0F114E4E);
    }
  }
}
