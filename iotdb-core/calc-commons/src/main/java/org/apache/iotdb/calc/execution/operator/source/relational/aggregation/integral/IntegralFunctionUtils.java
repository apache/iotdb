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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.integral;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate.TimeValueBuffer;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import static org.apache.iotdb.calc.utils.constant.SqlConstant.INTEGRAL;

/**
 * Helpers shared by the non-grouped and the grouped implementations of integral(), so that value
 * reading, validation messages and the trapezoidal accumulation stay identical in both paths.
 */
public final class IntegralFunctionUtils {

  private IntegralFunctionUtils() {}

  public static double readValue(TSDataType valueDataType, Column column, int position) {
    final double value =
        switch (valueDataType) {
          case INT32 -> column.getInt(position);
          case INT64 -> column.getLong(position);
          case FLOAT -> column.getFloat(position);
          case DOUBLE -> column.getDouble(position);
          default ->
              throw new SemanticException(
                  String.format(
                      CalcMessages
                          .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_VALUE_TYPE_ARG_9DD7388D,
                      INTEGRAL,
                      valueDataType));
        };
    if (!Double.isFinite(value)) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_NON_FINITE_VALUE_COL_ARG_AC2AAC62,
              INTEGRAL,
              value));
    }
    return value;
  }

  /** Sorts the samples by time and sums the trapezoidal area of adjacent distinct timestamps. */
  public static double calculateArea(TimeValueBuffer samples) {
    // fewer than two effective points have no time span, so the area is 0
    if (samples == null || samples.size() < 2) {
      return 0.0;
    }
    samples.sort();
    // "start" is the last point whose value can no longer change, "end" is the newest point. The
    // area of the segment between them is added only once a larger timestamp proves that "end"
    // will not be overwritten by a later sample of the same timestamp.
    long startTime = samples.getTime(0);
    double startValue = samples.getValue(0);
    long endTime = startTime;
    double endValue = startValue;
    int effectiveSampleCount = 1;
    double area = 0.0;
    for (int index = 1; index < samples.size(); index++) {
      long time = samples.getTime(index);
      double value = samples.getValue(index);
      if (time == endTime) {
        // the later point overwrites the earlier one when timestamps are duplicated
        endValue = value;
        continue;
      }
      if (effectiveSampleCount >= 2) {
        area += (startValue + endValue) / 2.0 * (endTime - startTime);
      }
      startTime = endTime;
      startValue = endValue;
      endTime = time;
      endValue = value;
      effectiveSampleCount++;
    }
    if (effectiveSampleCount < 2) {
      return 0.0;
    }
    // the newest point is final because there is no later sample to overwrite it
    return area + (startValue + endValue) / 2.0 * (endTime - startTime);
  }

  public static SemanticException invalidTimeUnit(String text) {
    return new SemanticException(
        String.format(
            CalcMessages
                .EXCEPTION_THE_TIME_UNIT_ARGUMENT_OF_AGGREGATE_FUNCTION_ARG_IS_INVALID_89D17438,
            INTEGRAL,
            text));
  }

  public static SemanticException orderedInputViolation(long time, long previousTime) {
    return new SemanticException(
        String.format(
            CalcMessages
                .EXCEPTION_AGGREGATE_FUNCTION_ARG_EXPECTED_TIME_COL_IN_STRICTLY_ASCENDING_ORDER_BUT_GOT_ARG_AFTER_ARG_9289E0F9,
            INTEGRAL,
            time,
            previousTime));
  }

  public static UnsupportedOperationException unsupportedIntermediate() {
    return new UnsupportedOperationException(
        String.format(
            CalcMessages
                .EXCEPTION_ORDERED_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_INTERMEDIATE_STATE_6B4B2B1B,
            INTEGRAL));
  }
}
