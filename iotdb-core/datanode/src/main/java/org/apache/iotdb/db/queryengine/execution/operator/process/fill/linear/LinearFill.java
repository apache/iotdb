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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill.linear;

import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The result of Linear Fill functions at timestamp "T" is calculated by performing a linear fitting
 * method on two time series values, one is at the closest timestamp before T, and the other is at
 * the closest timestamp after T. Linear Fill function calculation only supports numeric types
 * including long, int, double and float.
 */
public abstract class LinearFill implements ILinearFill {

  // whether previous value is null
  protected boolean previousIsNull = true;

  // next row index which is not null
  private long nextRowIndex = -1;
  // next row index in current column which is not null
  private long nextRowIndexInCurrentColumn = -1;

  // previous time coresponding to previous not null value
  protected long previousTime = -1;

  private long nextTime = -1;

  protected long nextTimeInCurrentColumn = -1;

  @Override
  public Column fill(Column timeColumn, Column valueColumn, long startRowIndex) {
    int size = valueColumn.getPositionCount();
    if (size == 0) {
      return valueColumn;
    }
    // if this valueColumn doesn't have any null value, record the last value, and then return
    // itself.
    if (!valueColumn.mayHaveNull()) {
      int lastNonNullIndex = -1;
      if (!timeColumn.mayHaveNull()) {
        lastNonNullIndex = size - 1;
      } else {
        for (int i = size - 1; i >= 0; i--) {
          if (!timeColumn.isNull(i)) {
            lastNonNullIndex = i;
            break;
          }
        }
      }
      if (lastNonNullIndex != -1) {
        previousIsNull = false;
        // update the value using last non-null value
        previousTime = timeColumn.getLong(lastNonNullIndex);
        updatePreviousValue(valueColumn, lastNonNullIndex);
      }

      return valueColumn;
    }

    // if its values are all null
    if ((valueColumn instanceof RunLengthEncodedColumn)) {
      return doWithAllNulls(startRowIndex, size, timeColumn, valueColumn);
    } else {
      Object array = createValueArray(size);
      boolean[] isNull = new boolean[size];
      // have null value
      boolean hasNullValue = false;

      for (int i = 0; i < size; i++) {
        // current value is null, we need to fill it
        if (valueColumn.isNull(i)) {
          hasNullValue =
              fill(startRowIndex, i, isNull, timeColumn, valueColumn, array) || hasNullValue;
        } else { // current is not null
          // fill value using its own value
          fillValue(valueColumn, i, array);
          if (!timeColumn.isNull(i)) {
            // update previous value
            previousTime = timeColumn.getLong(i);
            updatePreviousValue(valueColumn, i);
            previousIsNull = false;
          }
        }
      }
      return createFilledValueColumn(array, isNull, hasNullValue, size);
    }
  }

  private Column doWithAllNulls(
      long startRowIndex, int size, Column timeColumn, Column valueColumn) {
    // previous value is null or next value is null, we just return NULL_VALUE_BLOCK
    if (previousIsNull || nextRowIndex < startRowIndex) {
      return new RunLengthEncodedColumn(createNullValueColumn(), size);
    } else {
      prepareForNextValueInCurrentColumn(startRowIndex + size - 1, size, timeColumn, valueColumn);
      double[] factors = new double[size];
      boolean[] valueIsNull = new boolean[size];
      boolean hasNull = false;
      for (int i = 0; i < size; i++) {
        if (timeColumn.isNull(i)) {
          valueIsNull[i] = true;
          hasNull = true;
        } else {
          factors[i] = getFactor(timeColumn.getLong(i));
        }
      }
      return createFilledValueColumn(
          factors, hasNull ? Optional.empty() : Optional.of(valueIsNull));
    }
  }

  private boolean fill(
      long startRowIndex,
      int i,
      boolean[] isNull,
      Column timeColumn,
      Column valueColumn,
      Object array) {
    long currentRowIndex = startRowIndex + i;
    prepareForNextValueInCurrentColumn(currentRowIndex, i + 1, timeColumn, valueColumn);
    // we don't fill it, if previous value or next value or time column is null
    if (previousIsNull || timeColumn.isNull(i) || nextIsNull(currentRowIndex)) {
      isNull[i] = true;
      return true;
    } else {
      // fill value using previous and next value
      // factor is (x - x0) / (x1 - x0)
      double factor = getFactor(timeColumn.getLong(i));
      fillValue(array, i, factor);
      return false;
    }
  }

  private double getFactor(long currentTime) {
    return nextTimeInCurrentColumn - previousTime == 0
        ? 0.0
        : ((double) (currentTime - previousTime)) / (nextTimeInCurrentColumn - previousTime);
  }

  /**
   * Whether need prepare for next.
   *
   * @param rowIndex end time of current valueColumn that need to be filled
   * @param valueColumn valueColumn that need to be filled
   * @return true if valueColumn can't be filled using current information, and we need to get next
   *     TsBlock and then call prepareForNext. false if valueColumn can be filled using current
   *     information, and we can directly call fill() function
   */
  @Override
  public boolean needPrepareForNext(
      long rowIndex, Column valueColumn, int lastRowIndexForNonNullHelperColumn) {
    return nextRowIndex < rowIndex
        && lastRowIndexForNonNullHelperColumn >= 0
        && valueColumn.isNull(lastRowIndexForNonNullHelperColumn);
  }

  @Override
  public boolean prepareForNext(
      long startRowIndex, long endRowIndex, Column nextTimeColumn, Column nextValueColumn) {
    checkArgument(
        nextTimeColumn.getPositionCount() > 0 && endRowIndex < startRowIndex,
        "nextColumn's time should be greater than current time");
    if (endRowIndex <= nextRowIndex) {
      return true;
    }

    for (int i = 0, size = nextValueColumn.getPositionCount(); i < size; i++) {
      if (!nextTimeColumn.isNull(i) && !nextValueColumn.isNull(i)) {
        updateNextValue(nextValueColumn, i);
        this.nextTime = nextTimeColumn.getLong(i);
        this.nextRowIndex = startRowIndex + i;
        return true;
      }
    }
    return false;
  }

  private boolean nextIsNull(long rowIndex) {
    return nextRowIndexInCurrentColumn <= rowIndex;
  }

  private void prepareForNextValueInCurrentColumn(
      long currentRowIndex, int startIndex, Column timeColumn, Column valueColumn) {
    if (currentRowIndex <= nextRowIndexInCurrentColumn) {
      return;
    }
    for (int i = startIndex; i < valueColumn.getPositionCount(); i++) {
      if (!timeColumn.isNull(i) && !valueColumn.isNull(i)) {
        this.nextRowIndexInCurrentColumn = currentRowIndex + (i - startIndex + 1);
        this.nextTimeInCurrentColumn = timeColumn.getLong(i);
        updateNextValueInCurrentColumn(valueColumn, i);
        return;
      }
    }

    // current column's value is not enough for filling, we should use value of next Column
    this.nextRowIndexInCurrentColumn = this.nextRowIndex;
    this.nextTimeInCurrentColumn = this.nextTime;
    updateNextValueInCurrentColumn();
  }

  @Override
  public void reset() {
    previousIsNull = true;
    nextRowIndex = -1;
    nextRowIndexInCurrentColumn = -1;
    previousTime = -1;
    nextTime = -1;
    nextTimeInCurrentColumn = -1;
  }

  abstract void fillValue(Column column, int index, Object array);

  abstract void fillValue(Object array, int index, double factor);

  abstract Object createValueArray(int size);

  abstract Column createNullValueColumn();

  abstract Column createFilledValueColumn(double[] factors, Optional<boolean[]> valueIsNull);

  abstract Column createFilledValueColumn(
      Object array, boolean[] isNull, boolean hasNullValue, int size);

  abstract void updatePreviousValue(Column column, int index);

  abstract void updateNextValue(Column nextValueColumn, int index);

  abstract void updateNextValueInCurrentColumn(Column nextValueColumn, int index);

  /** update nextValueInCurrentColumn using value of next Column. */
  abstract void updateNextValueInCurrentColumn();
}
