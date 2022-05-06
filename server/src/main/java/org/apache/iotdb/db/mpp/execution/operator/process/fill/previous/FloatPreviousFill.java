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
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Optional;

public class FloatPreviousFill implements IFill {

  // previous value
  private float value;
  // whether previous value is null
  private boolean previousIsNull;
  // index of the column which is need to be filled
  private final int columnIndex;

  public FloatPreviousFill(float value, int columnIndex) {
    this.value = value;
    this.columnIndex = columnIndex;
  }

  @Override
  public Column fill(TsBlock tsBlock) {
    Column column = tsBlock.getColumn(columnIndex);
    int size = column.getPositionCount();
    // if this column doesn't have any null value, or it's empty, just return itself;
    if (!column.mayHaveNull() || size == 0) {
      if (size != 0) {
        previousIsNull = false;
        // update the value using last non-null value
        value = column.getFloat(size - 1);
      }
      return column;
    }
    // if its values are all null
    if (column instanceof RunLengthEncodedColumn) {
      if (previousIsNull) {
        return new RunLengthEncodedColumn(FloatColumnBuilder.NULL_VALUE_BLOCK, size);
      } else {
        // update the value using last non-null value
        value = column.getFloat(size - 1);
        return new RunLengthEncodedColumn(
            new FloatColumn(1, Optional.empty(), new float[] {value}), size);
      }
    } else {
      float[] array = new float[size];
      boolean[] isNull = new boolean[size];
      // have no null value
      boolean nonNullValue = true;
      for (int i = 0; i < size; i++) {
        if (column.isNull(i)) {
          if (previousIsNull) {
            isNull[i] = true;
            nonNullValue = false;
          } else {
            array[i] = value;
          }
        } else {
          array[i] = column.getFloat(i);
          value = array[i];
          previousIsNull = false;
        }
      }
      if (nonNullValue) {
        return new FloatColumn(size, Optional.empty(), array);
      } else {
        return new FloatColumn(size, Optional.of(isNull), array);
      }
    }
  }
}
