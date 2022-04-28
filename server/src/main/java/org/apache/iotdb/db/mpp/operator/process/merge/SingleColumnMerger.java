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
package org.apache.iotdb.db.mpp.operator.process.merge;

import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

/** only has one input column */
public class SingleColumnMerger implements ColumnMerger {

  private static final TimeComparator ASC_TIME_COMPARATOR = new AscTimeComparator();

  private static final TimeComparator DESC_TIME_COMPARATOR = new DescTimeComparator();

  private final InputLocation location;

  private final TimeComparator comparator;

  public SingleColumnMerger(InputLocation location, OrderBy orderBy) {
    this.location = location;
    if (orderBy == OrderBy.TIMESTAMP_ASC) {
      comparator = ASC_TIME_COMPARATOR;
    } else {
      comparator = DESC_TIME_COMPARATOR;
    }
  }

  @Override
  public void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      TimeColumnBuilder timeBuilder,
      long currentEndTime,
      ColumnBuilder columnBuilder) {
    int tsBlockIndex = location.getTsBlockIndex();
    int columnIndex = location.getValueColumnIndex();

    int rowCount = timeBuilder.getPositionCount();
    int index = inputIndex[tsBlockIndex];
    // input column is empty or current time of input column is already larger than currentEndTime
    // just appendNull rowCount null
    if (empty(tsBlockIndex, inputTsBlocks, inputIndex)
        || !comparator.satisfy(inputTsBlocks[tsBlockIndex].getTimeByIndex(index), currentEndTime)) {
      columnBuilder.appendNull(rowCount);
    } else {
      // read from input column and write it into columnBuilder
      TimeColumn timeColumn = inputTsBlocks[tsBlockIndex].getTimeColumn();
      Column valueColumn = inputTsBlocks[tsBlockIndex].getColumn(columnIndex);
      for (int i = 0; i < rowCount; i++) {
        // current index reaches the size of input column or current time of input column is already
        // larger than currentEndTime, use null column to fill the remaining
        if (timeColumn.getPositionCount() == index
            || !comparator.satisfy(
                inputTsBlocks[tsBlockIndex].getTimeByIndex(index), currentEndTime)) {
          columnBuilder.appendNull(rowCount - i);
          break;
        }
        // current time of input column is equal to result row's time
        if (timeColumn.getLong(index) == timeBuilder.getTime(i)) {
          // if input column's value at index is null, append a null value
          if (valueColumn.isNull(index)) {
            columnBuilder.appendNull();
          } else {
            // if input column's value at index is not null, append the value
            columnBuilder.write(valueColumn, index);
          }
          // increase the index
          index++;
        } else {
          // otherwise, append a null
          columnBuilder.appendNull();
        }
      }
      // update the index after merging
      updatedInputIndex[tsBlockIndex] = index;
    }
  }

  private interface TimeComparator {

    /** @return true if time is satisfied with endTime, otherwise false */
    boolean satisfy(long time, long endTime);
  }

  private static class AscTimeComparator implements TimeComparator {

    /** @return if order by time asc, return true if time <= endTime, otherwise false */
    @Override
    public boolean satisfy(long time, long endTime) {
      return time <= endTime;
    }
  }

  private static class DescTimeComparator implements TimeComparator {

    /** @return if order by time desc, return true if time >= endTime, otherwise false */
    @Override
    public boolean satisfy(long time, long endTime) {
      return time >= endTime;
    }
  }
}
