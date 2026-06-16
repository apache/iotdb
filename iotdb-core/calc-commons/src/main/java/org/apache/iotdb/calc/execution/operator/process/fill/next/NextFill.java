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

package org.apache.iotdb.calc.execution.operator.process.fill.next;

import org.apache.iotdb.calc.execution.operator.process.fill.IFillFilter;
import org.apache.iotdb.calc.execution.operator.process.fill.ILinearFill;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

/**
 * NEXT fill copies the nearest non-null value that appears later in the current input order. The
 * optional filter is only used for TIME_BOUND checks.
 */
public abstract class NextFill implements ILinearFill {

  private final IFillFilter filter;

  private long nextRowIndex = -1;
  private long nextTime = -1;
  private boolean nextIsNull = true;

  private long nextTimeInCurrentColumn = -1;
  private boolean nextInCurrentColumnIsNull = true;

  protected NextFill(IFillFilter filter) {
    this.filter = filter;
  }

  @Override
  public Column fill(Column timeColumn, Column valueColumn, long startRowIndex) {
    int size = valueColumn.getPositionCount();
    if (size == 0 || !valueColumn.mayHaveNull()) {
      return valueColumn;
    }

    prepareNextValueInCurrentColumn(startRowIndex, size);
    if (valueColumn instanceof RunLengthEncodedColumn && filter == null) {
      return nextInCurrentColumnIsNull
          ? new RunLengthEncodedColumn(createNullValueColumn(), size)
          : createRunLengthEncodedFilledValueColumn(size);
    }

    Object array = createValueArray(size);
    boolean[] isNull = new boolean[size];
    boolean hasNullValue = false;
    for (int i = size - 1; i >= 0; i--) {
      if (valueColumn.isNull(i)) {
        if (nextInCurrentColumnIsNull || cannotFillByTimeBound(timeColumn, i)) {
          isNull[i] = true;
          hasNullValue = true;
        } else {
          fillNextValueInCurrentColumn(array, i);
        }
      } else {
        fillValue(valueColumn, i, array);
        if (canBeNextCandidate(timeColumn, i)) {
          nextInCurrentColumnIsNull = false;
          nextTimeInCurrentColumn = filter == null ? -1 : timeColumn.getLong(i);
          updateNextValueInCurrentColumn(valueColumn, i);
        }
      }
    }
    return createFilledValueColumn(array, isNull, hasNullValue, size);
  }

  @Override
  public boolean needPrepareForNext(
      long rowIndex, Column valueColumn, int lastRowIndexForNonNullHelperColumn) {
    if (valueColumn.getPositionCount() == 0 || !valueColumn.mayHaveNull()) {
      return false;
    }
    if (filter != null && lastRowIndexForNonNullHelperColumn < 0) {
      return false;
    }
    if (!nextIsNull && nextRowIndex > rowIndex) {
      return false;
    }

    int lastIndex =
        filter == null ? valueColumn.getPositionCount() - 1 : lastRowIndexForNonNullHelperColumn;
    if (lastIndex < 0) {
      return false;
    }

    boolean hasTrailingNull = false;
    for (int i = lastIndex; i >= 0; i--) {
      if (valueColumn.isNull(i)) {
        hasTrailingNull = true;
      } else {
        return hasTrailingNull;
      }
    }
    return hasTrailingNull;
  }

  @Override
  public boolean prepareForNext(
      long startRowIndex, long endRowIndex, Column nextTimeColumn, Column nextValueColumn) {
    if (!nextIsNull && endRowIndex < nextRowIndex) {
      return true;
    }

    for (int i = 0, size = nextValueColumn.getPositionCount(); i < size; i++) {
      if (!nextValueColumn.isNull(i) && canBeNextCandidate(nextTimeColumn, i)) {
        updateNextValue(nextValueColumn, i);
        nextTime = filter == null ? -1 : nextTimeColumn.getLong(i);
        nextRowIndex = startRowIndex + i;
        nextIsNull = false;
        return true;
      }
    }
    return false;
  }

  @Override
  public void reset() {
    nextRowIndex = -1;
    nextTime = -1;
    nextIsNull = true;
    nextTimeInCurrentColumn = -1;
    nextInCurrentColumnIsNull = true;
  }

  private void prepareNextValueInCurrentColumn(long startRowIndex, int size) {
    if (!nextIsNull && nextRowIndex >= startRowIndex + size) {
      nextInCurrentColumnIsNull = false;
      nextTimeInCurrentColumn = nextTime;
      updateNextValueInCurrentColumn();
    } else {
      nextInCurrentColumnIsNull = true;
      nextTimeInCurrentColumn = -1;
    }
  }

  private boolean canBeNextCandidate(Column timeColumn, int index) {
    return filter == null || !timeColumn.isNull(index);
  }

  private boolean cannotFillByTimeBound(Column timeColumn, int index) {
    return filter != null
        && (timeColumn.isNull(index)
            || !filter.needFill(timeColumn.getLong(index), nextTimeInCurrentColumn));
  }

  abstract void fillValue(Column column, int index, Object array);

  abstract void fillNextValueInCurrentColumn(Object array, int index);

  abstract Object createValueArray(int size);

  abstract Column createNullValueColumn();

  abstract Column createRunLengthEncodedFilledValueColumn(int size);

  abstract Column createFilledValueColumn(
      Object array, boolean[] isNull, boolean hasNullValue, int size);

  abstract void updateNextValue(Column nextValueColumn, int index);

  abstract void updateNextValueInCurrentColumn(Column nextValueColumn, int index);

  /** update nextValueInCurrentColumn using value of next Column. */
  abstract void updateNextValueInCurrentColumn();
}
