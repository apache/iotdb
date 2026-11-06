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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.integral;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.integral.IntegralFunctionUtils;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

abstract class AbstractGroupedIntegralAccumulator implements GroupedAccumulator {

  protected static final int VALUE_ARGUMENT_INDEX = 0;
  protected static final int TIME_UNIT_ARGUMENT_INDEX = 1;
  protected static final int TIME_ARGUMENT_INDEX = 2;

  protected final TSDataType valueDataType;

  // The time unit converted to ticks of the current timestamp precision, 0 means uninitialized
  protected long timeUnit;

  protected AbstractGroupedIntegralAccumulator(TSDataType valueDataType) {
    this.valueDataType = valueDataType;
  }

  protected final double readValue(Column column, int position) {
    return IntegralFunctionUtils.readValue(valueDataType, column, position);
  }

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

  protected final double normalizeArea(double area) {
    // timeUnit stays 0 when no input has ever been added
    return timeUnit == 0 ? 0.0 : area / timeUnit;
  }

  protected final SemanticException invalidTimeUnit(String text) {
    return IntegralFunctionUtils.invalidTimeUnit(text);
  }

  protected final SemanticException orderedInputViolation(long time, long previousTime) {
    return IntegralFunctionUtils.orderedInputViolation(time, previousTime);
  }

  protected final UnsupportedOperationException unsupportedIntermediate() {
    return IntegralFunctionUtils.unsupportedIntermediate();
  }

  @Override
  public final void prepareFinal() {}
}
