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

package org.apache.tsfile.read.common.block;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;

import java.util.Arrays;

public class TsBlockUtil {

  private TsBlockUtil() {
    // forbidding instantiation
  }

  /** Skip lines at the beginning of the tsBlock that are not in the time range. */
  public static TsBlock skipPointsOutOfTimeRange(
      TsBlock tsBlock, TimeRange targetTimeRange, boolean ascending) {
    int firstIndex = getFirstConditionIndex(tsBlock, targetTimeRange, ascending);
    return tsBlock.subTsBlock(firstIndex);
  }

  // If ascending, find the index of first greater than or equal to targetTime
  // else, find the index of first less than or equal to targetTime
  public static int getFirstConditionIndex(
      TsBlock tsBlock, TimeRange targetTimeRange, boolean ascending) {
    Column timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? targetTimeRange.getMin() : targetTimeRange.getMax();
    int left = 0;
    int right = timeColumn.getPositionCount() - 1;
    int mid;

    while (left < right) {
      mid = (left + right) >> 1;
      if (timeColumn.getLong(mid) < targetTime) {
        if (ascending) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else if (timeColumn.getLong(mid) > targetTime) {
        if (ascending) {
          right = mid;
        } else {
          left = mid + 1;
        }
      } else if (timeColumn.getLong(mid) == targetTime) {
        return mid;
      }
    }
    return left;
  }

  public static TsBlock applyFilterAndLimitOffsetToTsBlock(
      TsBlock unFilteredBlock,
      TsBlockBuilder builder,
      Filter pushDownFilter,
      PaginationController paginationController) {
    boolean[] selection = new boolean[unFilteredBlock.getPositionCount()];
    Arrays.fill(selection, true);
    boolean[] keepCurrentRow = pushDownFilter.satisfyTsBlock(selection, unFilteredBlock);

    // construct time column
    int readEndIndex =
        buildTimeColumnWithPagination(
            unFilteredBlock, builder, keepCurrentRow, paginationController);

    // construct value columns
    for (int i = 0; i < builder.getValueColumnBuilders().length; i++) {
      for (int rowIndex = 0; rowIndex < readEndIndex; rowIndex++) {
        if (keepCurrentRow[rowIndex]) {
          if (unFilteredBlock.getValueColumns()[i].isNull(rowIndex)) {
            builder.getColumnBuilder(i).appendNull();
          } else {
            builder
                .getColumnBuilder(i)
                .writeObject(unFilteredBlock.getValueColumns()[i].getObject(rowIndex));
          }
        }
      }
    }
    return builder.build();
  }

  private static int buildTimeColumnWithPagination(
      TsBlock unFilteredBlock,
      TsBlockBuilder builder,
      boolean[] keepCurrentRow,
      PaginationController paginationController) {
    int readEndIndex = unFilteredBlock.getPositionCount();
    for (int rowIndex = 0; rowIndex < readEndIndex; rowIndex++) {
      if (keepCurrentRow[rowIndex]) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
          keepCurrentRow[rowIndex] = false;
        } else if (paginationController.hasCurLimit()) {
          builder.getTimeColumnBuilder().writeLong(unFilteredBlock.getTimeByIndex(rowIndex));
          builder.declarePosition();
          paginationController.consumeLimit();
        } else {
          readEndIndex = rowIndex;
          break;
        }
      }
    }
    return readEndIndex;
  }
}
