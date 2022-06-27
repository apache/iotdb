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
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Optional;

public class IntPreviousFill implements IFill {

  // previous value
  private int value;
  // whether previous value is null
  private boolean previousIsNull = true;

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
      value = valueColumn.getInt(size - 1);
      return valueColumn;
    }
    // if its values are all null
    if (valueColumn instanceof RunLengthEncodedColumn) {
      if (previousIsNull) {
        return new RunLengthEncodedColumn(IntColumnBuilder.NULL_VALUE_BLOCK, size);
      } else {
        return new RunLengthEncodedColumn(
            new IntColumn(1, Optional.empty(), new int[] {value}), size);
      }
    } else {
      int[] array = new int[size];
      boolean[] isNull = new boolean[size];
      // have null value
      boolean hasNullValue = false;
      for (int i = 0; i < size; i++) {
        if (valueColumn.isNull(i)) {
          if (previousIsNull) {
            isNull[i] = true;
            hasNullValue = true;
          } else {
            array[i] = value;
          }
        } else {
          array[i] = valueColumn.getInt(i);
          value = array[i];
          previousIsNull = false;
        }
      }
      if (hasNullValue) {
        return new IntColumn(size, Optional.of(isNull), array);
      } else {
        return new IntColumn(size, Optional.empty(), array);
      }
    }
  }
}
