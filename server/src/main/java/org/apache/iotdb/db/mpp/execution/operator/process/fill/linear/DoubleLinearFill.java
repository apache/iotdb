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
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;

import java.util.Optional;

public class DoubleLinearFill extends LinearFill {

  // previous value
  private double previousValue;
  // next non-null value whose time is closest to the current TsBlock's endTime
  private double nextValue;

  private double nextValueInCurrentColumn;

  @Override
  void fillValue(Column column, int index, Object array) {
    ((double[]) array)[index] = column.getDouble(index);
  }

  @Override
  void fillValue(Object array, int index) {
    ((double[]) array)[index] = getFilledValue();
  }

  @Override
  Object createValueArray(int size) {
    return new double[size];
  }

  @Override
  Column createNullValueColumn() {
    return DoubleColumnBuilder.NULL_VALUE_BLOCK;
  }

  @Override
  Column createFilledValueColumn() {
    double filledValue = getFilledValue();
    return new DoubleColumn(1, Optional.empty(), new double[] {filledValue});
  }

  @Override
  Column createFilledValueColumn(Object array, boolean[] isNull, boolean hasNullValue, int size) {
    if (hasNullValue) {
      return new DoubleColumn(size, Optional.of(isNull), (double[]) array);
    } else {
      return new DoubleColumn(size, Optional.empty(), (double[]) array);
    }
  }

  @Override
  void updatePreviousValue(Column column, int index) {
    previousValue = column.getDouble(index);
  }

  @Override
  void updateNextValue(Column nextValueColumn, int index) {
    this.nextValue = nextValueColumn.getDouble(index);
  }

  @Override
  void updateNextValueInCurrentColumn(Column nextValueColumn, int index) {
    this.nextValueInCurrentColumn = nextValueColumn.getDouble(index);
  }

  @Override
  void updateNextValueInCurrentColumn() {
    this.nextValueInCurrentColumn = this.nextValue;
  }

  private double getFilledValue() {
    return (previousValue + nextValueInCurrentColumn) / 2.0;
  }
}
