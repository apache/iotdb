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
package org.apache.iotdb.db.mpp.execution.operator.process.fill.linear;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;

import java.util.Optional;

public class FloatLinearFill extends LinearFill {

  // previous value
  private float previousValue;
  // next non-null value whose time is closest to the current TsBlock's endTime
  private float nextValue;

  private float nextValueInCurrentColumn;

  @Override
  void fillValue(Column column, int index, Object array) {
    ((float[]) array)[index] = column.getFloat(index);
  }

  @Override
  void fillValue(Object array, int index) {
    ((float[]) array)[index] = getFilledValue();
  }

  @Override
  Object createValueArray(int size) {
    return new float[size];
  }

  @Override
  Column createNullValueColumn() {
    return FloatColumnBuilder.NULL_VALUE_BLOCK;
  }

  @Override
  Column createFilledValueColumn() {
    float filledValue = getFilledValue();
    return new FloatColumn(1, Optional.empty(), new float[] {filledValue});
  }

  @Override
  Column createFilledValueColumn(Object array, boolean[] isNull, boolean hasNullValue, int size) {
    if (hasNullValue) {
      return new FloatColumn(size, Optional.of(isNull), (float[]) array);
    } else {
      return new FloatColumn(size, Optional.empty(), (float[]) array);
    }
  }

  @Override
  void updatePreviousValue(Column column, int index) {
    previousValue = column.getFloat(index);
  }

  @Override
  void updateNextValue(Column nextValueColumn, int index) {
    this.nextValue = nextValueColumn.getFloat(index);
  }

  @Override
  void updateNextValueInCurrentColumn(Column nextValueColumn, int index) {
    this.nextValueInCurrentColumn = nextValueColumn.getFloat(index);
  }

  @Override
  void updateNextValueInCurrentColumn() {
    this.nextValueInCurrentColumn = this.nextValue;
  }

  private float getFilledValue() {
    return (previousValue + nextValueInCurrentColumn) / 2.0f;
  }
}
