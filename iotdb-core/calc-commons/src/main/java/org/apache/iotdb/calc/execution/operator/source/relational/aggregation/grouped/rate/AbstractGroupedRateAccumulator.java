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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.rate;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.ExtrapolationUtil;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.RateFunctionType;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.enums.TSDataType;

abstract class AbstractGroupedRateAccumulator implements GroupedAccumulator {

  protected final TSDataType valueDataType;
  private final BooleanBigArray windowInitialized = new BooleanBigArray();
  private final LongBigArray windowStarts = new LongBigArray();
  private final LongBigArray windowEnds = new LongBigArray();

  protected AbstractGroupedRateAccumulator(TSDataType valueDataType) {
    this.valueDataType = valueDataType;
  }

  protected final void ensureWindowCapacity(long groupCount) {
    windowInitialized.ensureCapacity(groupCount);
    windowStarts.ensureCapacity(groupCount);
    windowEnds.ensureCapacity(groupCount);
  }

  protected final void initializeOrValidateWindow(
      int groupId, long candidateWindowStart, long candidateWindowEnd) {
    if (!windowInitialized.get(groupId)) {
      windowInitialized.set(groupId, true);
      windowStarts.set(groupId, candidateWindowStart);
      windowEnds.set(groupId, candidateWindowEnd);
      return;
    }
    long expectedStart = windowStarts.get(groupId);
    long expectedEnd = windowEnds.get(groupId);
    if (expectedStart != candidateWindowStart || expectedEnd != candidateWindowEnd) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_AGGREGATE_FUNCTION_ARG_REQUIRES_CONSISTENT_WINDOW_BOUNDARIES_IN_THE_SAME_AGGREGATION_GROUP_EXPECTED_ARG_ARG_BUT_GOT_ARG_ARG_38631886,
              RateFunctionType.RATE.getFunctionName(),
              expectedStart,
              expectedEnd,
              candidateWindowStart,
              candidateWindowEnd));
    }
  }

  protected final long windowStart(int groupId) {
    return windowStarts.get(groupId);
  }

  protected final long windowEnd(int groupId) {
    return windowEnds.get(groupId);
  }

  protected final long windowsSizeOf() {
    return windowInitialized.sizeOf() + windowStarts.sizeOf() + windowEnds.sizeOf();
  }

  protected final void resetWindows() {
    windowInitialized.reset();
    windowStarts.reset();
    windowEnds.reset();
  }

  protected final double calculateRate(
      int sampleCount,
      long firstTime,
      double firstValue,
      long lastTime,
      double correctedIncrease,
      long windowStart,
      long windowEnd) {
    double extrapolated =
        ExtrapolationUtil.extrapolate(
            sampleCount,
            firstTime,
            firstValue,
            lastTime,
            windowStart,
            windowEnd,
            correctedIncrease,
            true);
    return validateFinite(
        extrapolated / ExtrapolationUtil.timestampDiffToSeconds(windowEnd, windowStart));
  }

  protected final SemanticException duplicateTimestamp(long time) {
    return new SemanticException(
        String.format(
            CalcMessages
                .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_DUPLICATE_TIME_COL_VALUES_IN_THE_SAME_AGGREGATION_GROUP_ARG_087A91BC,
            RateFunctionType.RATE.getFunctionName(),
            time));
  }

  protected final SemanticException orderedInputViolation(long time, long previousTime) {
    return new SemanticException(
        String.format(
            CalcMessages
                .EXCEPTION_AGGREGATE_FUNCTION_ARG_EXPECTED_TIME_COL_IN_STRICTLY_ASCENDING_ORDER_BUT_GOT_ARG_AFTER_ARG_9289E0F9,
            RateFunctionType.RATE.getFunctionName(),
            time,
            previousTime));
  }

  protected final UnsupportedOperationException unsupportedIntermediate() {
    return new UnsupportedOperationException(
        String.format(
            CalcMessages
                .EXCEPTION_ORDERED_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_INTERMEDIATE_STATE_6B4B2B1B,
            RateFunctionType.RATE.getFunctionName()));
  }

  protected final double validateFinite(double value) {
    if (!Double.isFinite(value)) {
      throw new SemanticException(
          CalcMessages
              .EXCEPTION_RATE_FAMILY_AGGREGATE_FUNCTION_PRODUCED_A_NON_FINITE_INTERMEDIATE_RESULT_D46B30CD);
    }
    return value;
  }

  @Override
  public final void prepareFinal() {}
}
