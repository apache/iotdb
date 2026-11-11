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

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public abstract class AbstractIntegralTableAccumulator implements TableAccumulator {

  // Default time unit of integral() when the second argument is omitted
  public static final String DEFAULT_INTEGRAL_TIME_UNIT = "1s";

  protected static final int VALUE_ARGUMENT_INDEX = 0;
  protected static final int TIME_UNIT_ARGUMENT_INDEX = 1;
  protected static final int TIME_ARGUMENT_INDEX = 2;

  protected final TSDataType valueDataType;

  // The time unit converted to ticks of the current timestamp precision, 0 means uninitialized
  protected long timeUnit;

  protected AbstractIntegralTableAccumulator(TSDataType valueDataType) {
    this.valueDataType = valueDataType;
  }

  protected final double readValue(Column column, int position) {
    return IntegralFunctionUtils.readValue(valueDataType, column, position);
  }

  /**
   * The time unit has already been converted to ticks of the current timestamp precision in the
   * analyzer, so only the first value has to be read.
   */
  protected final void initializeOrValidateTimeUnit(Column timeUnitColumn) {
    // an empty block carries no value to read, and there is nothing to integrate either
    if (timeUnit != 0 || timeUnitColumn.getPositionCount() == 0) {
      return;
    }
    initializeOrValidateTimeUnit(timeUnitColumn.getLong(0));
  }

  protected final void initializeOrValidateTimeUnit(long unitTicks) {
    if (unitTicks <= 0) {
      throw invalidTimeUnit(String.valueOf(unitTicks));
    }
    if (timeUnit != 0 && timeUnit != unitTicks) {
      throw invalidTimeUnit(String.valueOf(unitTicks));
    }
    timeUnit = unitTicks;
  }

  protected final void resetTimeUnit() {
    timeUnit = 0;
  }

  protected static SemanticException invalidTimeUnit(String text) {
    return IntegralFunctionUtils.invalidTimeUnit(text);
  }

  protected static SemanticException orderedInputViolation(long time, long previousTime) {
    return IntegralFunctionUtils.orderedInputViolation(time, previousTime);
  }

  protected static UnsupportedOperationException unsupportedIntermediate() {
    return IntegralFunctionUtils.unsupportedIntermediate();
  }
}
