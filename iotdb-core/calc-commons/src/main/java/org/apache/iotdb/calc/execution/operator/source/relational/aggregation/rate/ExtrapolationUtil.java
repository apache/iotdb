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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public final class ExtrapolationUtil {

  private static final BigInteger ELEVEN = BigInteger.valueOf(11);

  private ExtrapolationUtil() {}

  public static double extrapolate(
      int sampleCount,
      long firstTime,
      double firstValue,
      long lastTime,
      long windowStart,
      long windowEnd,
      double increase,
      boolean applyCounterZeroProtection) {
    validateBoundaries(sampleCount, firstTime, lastTime, windowStart, windowEnd);

    BigInteger sampledTicks = timestampDiff(lastTime, firstTime);
    BigInteger durationToStartTicks = timestampDiff(firstTime, windowStart);
    BigInteger durationToEndTicks = timestampDiff(windowEnd, lastTime);
    double sampledInterval = ticksToSeconds(sampledTicks);
    double durationToStart = ticksToSeconds(durationToStartTicks);
    double durationToEnd = ticksToSeconds(durationToEndTicks);

    if (!Double.isFinite(increase)) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_PRODUCED_A_NON_FINITE_INTERMEDIATE_RESULT_D46B30CD);
    }
    if (applyCounterZeroProtection && increase == 0.0) {
      return 0.0;
    }

    double averageInterval = sampledInterval / (sampleCount - 1);

    if (isExtrapolationThresholdReached(durationToStartTicks, sampledTicks, sampleCount)) {
      durationToStart = averageInterval / 2.0;
    }

    if (applyCounterZeroProtection && increase > 0.0) {
      double durationToZero = sampledInterval * (firstValue / increase);
      if (Double.isFinite(durationToZero) && durationToZero < durationToStart) {
        durationToStart = durationToZero;
      }
    }

    if (isExtrapolationThresholdReached(durationToEndTicks, sampledTicks, sampleCount)) {
      durationToEnd = averageInterval / 2.0;
    }

    double factor = (sampledInterval + durationToStart + durationToEnd) / sampledInterval;
    double result = increase * factor;
    if (!Double.isFinite(factor) || !Double.isFinite(result)) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_PRODUCED_A_NON_FINITE_EXTRAPOLATION_RESULT_6482CF1D);
    }
    return result;
  }

  public static double timestampDiffToSeconds(long later, long earlier) {
    return ticksToSeconds(timestampDiff(later, earlier));
  }

  static boolean isExtrapolationThresholdReached(
      BigInteger gapTicks, BigInteger sampledTicks, int sampleCount) {
    return gapTicks
            .multiply(BigInteger.valueOf(sampleCount - 1L))
            .multiply(BigInteger.TEN)
            .compareTo(sampledTicks.multiply(ELEVEN))
        >= 0;
  }

  private static BigInteger timestampDiff(long later, long earlier) {
    return BigInteger.valueOf(later).subtract(BigInteger.valueOf(earlier));
  }

  private static double ticksToSeconds(BigInteger ticks) {
    double ticksPerSecond = TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.SECONDS);
    double seconds = ticks.doubleValue() / ticksPerSecond;
    if (!Double.isFinite(seconds)) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_PRODUCED_A_NON_FINITE_TIME_INTERVAL_B26C4162);
    }
    return seconds;
  }

  private static void validateBoundaries(
      int sampleCount, long firstTime, long lastTime, long windowStart, long windowEnd) {
    if (sampleCount < 2) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_REQUIRES_AT_LEAST_TWO_VALID_SAMPLES_1A89901C);
    }
    if (firstTime >= lastTime) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_THE_FIRST_SAMPLE_TIME_MUST_BE_LESS_THAN_THE_LAST_SAMPLE_TIME_FC4D3517);
    }
    if (windowStart >= windowEnd) {
      throw new SemanticException(
          CalcMessages.EXCEPTION_THE_WINDOW_START_MUST_BE_LESS_THAN_THE_WINDOW_END_B1A38C98);
    }
    if (firstTime < windowStart || lastTime >= windowEnd) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_REQUIRES_WINDOW_START_FIRST_TIME_LAST_TIME_WINDOW_END_60DE93C8);
    }
  }
}
