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
package org.apache.iotdb.db.mpp.execution.operator.process.fill.previous;

import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Optional;

public class BooleanPreviousFill implements IFill {

  // previous value
  private boolean value;
  // whether previous value is null
  private boolean previousIsNull = true;
  // PREVIOUS is false
  private boolean isPreviousUntilLast = false;

  public BooleanPreviousFill(boolean isPreviousUntilLast) {
    this.isPreviousUntilLast = isPreviousUntilLast;
  }

  @Override
  public Column fill(Column valueColumn) {
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
      if (previousIsNull) {
        return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, size);
      } else {
        return new RunLengthEncodedColumn(
            new BooleanColumn(1, Optional.empty(), new boolean[] {value}), size);
      }
    } else {
      int index = valueColumn.getPositionCount();
      if (isPreviousUntilLast) {
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
}
