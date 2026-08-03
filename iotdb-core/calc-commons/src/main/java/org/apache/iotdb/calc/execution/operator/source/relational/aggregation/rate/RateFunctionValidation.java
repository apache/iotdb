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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public final class RateFunctionValidation {

  private RateFunctionValidation() {}

  public static void validateArgumentCount(Column[] arguments, RateFunctionType functionType) {
    int expected = functionType.isWindowed() ? 4 : 2;
    if (arguments.length != expected) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_AGGREGATE_FUNCTION_ARG_REQUIRES_ARG_ARGUMENTS_BUT_GOT_ARG_D6249DD2,
              functionType.getFunctionName(),
              expected,
              arguments.length));
    }
  }

  public static double readValue(
      Column column, int position, TSDataType valueDataType, RateFunctionType functionType) {
    double value;
    switch (valueDataType) {
      case INT32:
        value = column.getInt(position);
        break;
      case INT64:
        value = column.getLong(position);
        break;
      case FLOAT:
        value = column.getFloat(position);
        break;
      case DOUBLE:
        value = column.getDouble(position);
        break;
      default:
        throw new SemanticException(
            String.format(
                CalcMessages
                    .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_VALUE_TYPE_ARG_9DD7388D,
                functionType.getFunctionName(),
                valueDataType));
    }

    if (!Double.isFinite(value)) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_NON_FINITE_VALUE_COL_ARG_AC2AAC62,
              functionType.getFunctionName(),
              value));
    }
    if (functionType.isCounter() && value < 0.0) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_THE_VALUE_COL_ARGUMENT_OF_AGGREGATE_FUNCTION_ARG_MUST_BE_A_NON_NEGATIVE_NUMBER_BUT_GOT_ARG_4D5B7D74,
              functionType.getFunctionName(),
              value));
    }
    return value;
  }

  public static long readRequiredTime(
      Column column, int position, RateFunctionType functionType, int argumentPosition) {
    if (column.isNull(position)) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_THE_ARGUMENT_ARG_OF_AGGREGATE_FUNCTION_ARG_MUST_NOT_BE_NULL_WHEN_VALUE_COL_IS_NOT_NULL_7F087E99,
              argumentPosition,
              functionType.getFunctionName()));
    }
    return column.getLong(position);
  }

  public static void validateWindow(
      RateFunctionType functionType, long time, long windowStart, long windowEnd) {
    if (windowStart >= windowEnd) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_THE_WINDOW_START_ARGUMENT_OF_AGGREGATE_FUNCTION_ARG_MUST_BE_LESS_THAN_WINDOW_END_17D2A79A,
              functionType.getFunctionName()));
    }
    if (time < windowStart || time >= windowEnd) {
      throw new SemanticException(
          String.format(
              CalcMessages
                  .EXCEPTION_THE_SAMPLE_TIME_OF_AGGREGATE_FUNCTION_ARG_MUST_SATISFY_WINDOW_START_TIME_COL_WINDOW_END_35014D15,
              functionType.getFunctionName()));
    }
  }
}
