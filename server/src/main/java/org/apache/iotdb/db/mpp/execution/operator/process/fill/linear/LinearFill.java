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
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The result of Linear Fill functions at timestamp "T" is calculated by performing a linear fitting
 * method on two time series values, one is at the closest timestamp before T, and the other is at
 * the closest timestamp after T. Linear Fill function calculation only supports numeric types
 * including int, double and float.
 */
public abstract class LinearFill {

  // whether previous value is null
  protected boolean previousIsNull = true;
  // time of next value
  protected long nextTime = Long.MIN_VALUE;

  protected long nextTimeInCurrentColumn;

  /**
   * Before we call this method, we need to make sure the nextValue has been prepared or noMoreNext
   * has been set to true
   *
   * @param timeColumn TimeColumn of valueColumn
   * @param valueColumn valueColumn that need to be filled
   * @return Value Column that has been filled
   */
  public Column fill(TimeColumn timeColumn, Column valueColumn) {
    int size = valueColumn.getPositionCount();
    // if this valueColumn is empty, just return itself;
    if (size == 0) {
      return valueColumn;
    }
    // if this valueColumn doesn't have any null value, record the last value, and then return
    // itself.
    if (!valueColumn.mayHaveNull()) {
      previousIsNull = false;
      // update the value using last non-null value
      updatePreviousValue(valueColumn, valueColumn.getPositionCount() - 1);
      return valueColumn;
    }

    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      // previous value is null or next value is null, we just return NULL_VALUE_BLOCK
      if (previousIsNull || nextTime < timeColumn.getStartTime()) {
        return new RunLengthEncodedColumn(createNullValueColumn(), size);
      } else {
        prepareForNextValueInCurrentColumn(
            timeColumn.getEndTime(), timeColumn.getPositionCount() - 1, timeColumn, valueColumn);
        return new RunLengthEncodedColumn(createFilledValueColumn(), size);
      }
    } else {
      Object array = createValueArray(size);
      boolean[] isNull = new boolean[size];
      // have null value
      boolean hasNullValue = false;

      for (int i = 0; i < size; i++) {
        // current value is null, we need to fill it
        if (valueColumn.isNull(i)) {
          long currentTime = timeColumn.getLong(i);
          prepareForNextValueInCurrentColumn(currentTime, i + 1, timeColumn, valueColumn);
          // we don't fill it, if either previous value or next value is null
          if (previousIsNull || nextIsNull(currentTime)) {
            isNull[i] = true;
            hasNullValue = true;
          } else {
            // fill value using previous and next value
            fillValue(array, i);
          }
        } else { // current is not null
          // fill value using its own value
          fillValue(valueColumn, i, array);
          // update previous value
          updatePreviousValue(valueColumn, i);
          previousIsNull = false;
        }
      }
      return createFilledValueColumn(array, isNull, hasNullValue, size);
    }
  }

  /**
   * @param time end time of current valueColumn that need to be filled
   * @param valueColumn valueColumn that need to be filled
   * @return true if valueColumn can't be filled using current information, and we need to get next
   *     TsBlock and then call prepareForNext. false if valueColumn can be filled using current
   *     information, and we can directly call fill() function
   */
  public boolean needPrepareForNext(long time, Column valueColumn) {
    return time > nextTime && valueColumn.isNull(valueColumn.getPositionCount() - 1);
  }

  /**
   * @param time end time of current valueColumn that need to be filled
   * @param nextTimeColumn TimeColumn of next TsBlock
   * @param nextValueColumn Value Column of next TsBlock
   * @return true if we get enough information to fill current column, and we can stop getting next
   *     TsBlock and calling prepareForNext. false if we still don't get enough information to fill
   *     current column, and still need to keep getting next TsBlock and then call prepareForNext
   */
  public boolean prepareForNext(long time, TimeColumn nextTimeColumn, Column nextValueColumn) {
    checkArgument(
        nextTimeColumn.getPositionCount() > 0 && nextTimeColumn.getLong(0) > time,
        "nextColumn's time should be greater than current time");
    if (time <= nextTime) {
      return true;
    }

    for (int i = 0; i < nextValueColumn.getPositionCount(); i++) {
      if (!nextValueColumn.isNull(i)) {
        updateNextValue(nextValueColumn, i);
        this.nextTime = nextTimeColumn.getLong(i);
        return true;
      }
    }
    return false;
  }

  private boolean nextIsNull(long time) {
    return nextTimeInCurrentColumn <= time;
  }

  private void prepareForNextValueInCurrentColumn(
      long time, int startIndex, TimeColumn timeColumn, Column valueColumn) {
    if (time <= nextTimeInCurrentColumn) {
      return;
    }
    for (int i = startIndex; i < valueColumn.getPositionCount(); i++) {
      if (!valueColumn.isNull(i)) {
        this.nextTimeInCurrentColumn = timeColumn.getLong(i);
        updateNextValueInCurrentColumn(valueColumn, i);
        return;
      }
    }

    // current column's value is not enough for filling, we should use value of next Column
    this.nextTimeInCurrentColumn = this.nextTime;
    updateNextValueInCurrentColumn();
  }

  abstract void fillValue(Column column, int index, Object array);

  abstract void fillValue(Object array, int index);

  abstract Object createValueArray(int size);

  abstract Column createNullValueColumn();

  abstract Column createFilledValueColumn();

  abstract Column createFilledValueColumn(
      Object array, boolean[] isNull, boolean hasNullValue, int size);

  abstract void updatePreviousValue(Column column, int index);

  abstract void updateNextValue(Column nextValueColumn, int index);

  abstract void updateNextValueInCurrentColumn(Column nextValueColumn, int index);

  /** update nextValueInCurrentColumn using value of next Column */
  abstract void updateNextValueInCurrentColumn();
}
