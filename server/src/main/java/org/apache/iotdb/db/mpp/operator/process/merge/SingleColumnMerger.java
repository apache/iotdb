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

import org.apache.iotdb.db.mpp.sql.planner.plan.InputLocation;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

/** only has one input column */
public class SingleColumnMerger implements ColumnMerger {

  private final InputLocation location;

  public SingleColumnMerger(InputLocation location) {
    this.location = location;
  }

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
        || inputTsBlocks[tsBlockIndex].getTimeByIndex(index) > currentEndTime) {
      columnBuilder.appendNull(rowCount);
    } else {
      // read from input column and write it into columnBuilder
      TimeColumn timeColumn = inputTsBlocks[tsBlockIndex].getTimeColumn();
      Column valueColumn = inputTsBlocks[tsBlockIndex].getColumn(columnIndex);
      for (int i = 0; i < rowCount; i++) {
        // current index is less than size of input column and current time of input column is equal
        // to result row's time and input column's value at index is not null
        if (timeColumn.getPositionCount() > index
            && timeColumn.getLong(index) == timeBuilder.getTime(i)
            && !valueColumn.isNull(index)) {
          columnBuilder.write(valueColumn, index++);
        } else {
          // otherwise, append a null
          columnBuilder.appendNull();
        }
      }
      // update the index after merging
      updatedInputIndex[tsBlockIndex] = index;
    }
  }
}
