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
package org.apache.iotdb.db.mpp.execution.operator.process.fill.previousuntillast;

import org.apache.iotdb.db.mpp.execution.operator.process.fill.IPreviousUntilLastFill;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Optional;

public class BooleanPreviousUntilLastFill implements IPreviousUntilLastFill {

  /** previous value */
  private boolean value;
  /** whether previous value is null */
  private boolean previousIsNull = true;

  private int index = 0;
  /** Determine whether it is null from the first tsblock, */
  private boolean previousTsBlockIsNull = false;

  private int lastHasValueTsBlockIndex = 0;

  @Override
  public Column fill(Column valueColumn, int currentIndex) {
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
      value = valueColumn.getBoolean(size - 1);
      return valueColumn;
    }
    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      // If the current tsblock is after the last tsblock with value , it will not be filled
      if (currentIndex > lastHasValueTsBlockIndex) {
        return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, size);
      } else {
        // If the current tsblock is before the last tsblock with value then it will be filled
        if (previousIsNull) {
          return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, size);
        } else {
          return new RunLengthEncodedColumn(
              new BooleanColumn(1, Optional.empty(), new boolean[] {value}), size);
        }
      }
    } else {
      int index = valueColumn.getPositionCount();

      // Get the index of the last row with a value for the current column
      if (currentIndex >= lastHasValueTsBlockIndex) {
        for (int count = index - 1; count >= 0; count--) {
          if (!valueColumn.isNull(count)) {
            index = count;
            break;
          }
        }
      }

      boolean[] array = new boolean[size];
      boolean[] isNull = new boolean[size];
      // have null value
      boolean hasNullValue = false;
      for (int i = 0; i < size; i++) {
        if (i <= index) {
          if (valueColumn.isNull(i)) {
            if (previousIsNull) {
              isNull[i] = true;
              hasNullValue = true;
            } else {
              array[i] = value;
            }
          } else {
            array[i] = valueColumn.getBoolean(i);
            value = array[i];
            previousIsNull = false;
          }
        } else {
          isNull[i] = true;
          hasNullValue = true;
        }
      }
      if (hasNullValue) {
        return new BooleanColumn(size, Optional.of(isNull), array);
      } else {
        return new BooleanColumn(size, Optional.empty(), array);
      }
    }
  }

  @Override
  public boolean needPrepareForNext(Column valueColumn, int currentIndex) {
    if (valueColumn instanceof RunLengthEncodedColumn) {
      if (previousTsBlockIsNull) {
        return false;
      }
      if (currentIndex < lastHasValueTsBlockIndex) {
        return false;
      }
      // It is the first tsblock, you don't need to know whether the next tsblock has a value, you
      // can fill it directly
      if (index == 0) {
        previousTsBlockIsNull = true;
        index++;
        return false;
      }
      return true;
    } else {
      // Prevent the first tsblock from being non-empty, but the second tsblock is all null and the
      // index is still 0
      if (index == 0) {
        index++;
      }
      previousTsBlockIsNull = false;
      this.lastHasValueTsBlockIndex = currentIndex;
      return valueColumn.isNull(valueColumn.getPositionCount() - 1);
    }
  }

  @Override
  public void updateLastHasValueTsBlockIndex(int lastHasValueTsBlockIndex) {
    this.lastHasValueTsBlockIndex = lastHasValueTsBlockIndex;
  }

  @Override
  public boolean allValueIsNull(Column valueColumn) {
    return valueColumn instanceof RunLengthEncodedColumn;
  }
}
