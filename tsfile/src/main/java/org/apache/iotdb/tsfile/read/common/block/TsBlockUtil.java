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

package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

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
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    long targetTime = ascending ? targetTimeRange.getMin() : targetTimeRange.getMax();
    int left = 0, right = timeColumn.getPositionCount() - 1, mid;

    while (left < right) {
      mid = (left + right) >> 1;
      if (timeColumn.getLongWithoutCheck(mid) < targetTime) {
        if (ascending) {
          left = mid + 1;
        } else {
          right = mid;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) > targetTime) {
        if (ascending) {
          right = mid;
        } else {
          left = mid + 1;
        }
      } else if (timeColumn.getLongWithoutCheck(mid) == targetTime) {
        return mid;
      }
    }
    return left;
  }

  public static void appendTsBlockToBuilder(TsBlock tsBlock, TsBlockBuilder builder) {
    int size = tsBlock.getPositionCount();
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    for (int i = 0; i < size; i++) {
      timeColumnBuilder.writeLong(timeColumn.getLong(i));
      builder.declarePosition();
    }
    for (int columnIndex = 0, columnSize = tsBlock.getValueColumnCount();
        columnIndex < columnSize;
        columnIndex++) {
      ColumnBuilder columnBuilder = builder.getColumnBuilder(columnIndex);
      Column column = tsBlock.getColumn(columnIndex);
      if (column.mayHaveNull()) {
        for (int i = 0; i < size; i++) {
          if (column.isNull(i)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(column, i);
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          columnBuilder.write(column, i);
        }
      }
    }
  }
}
