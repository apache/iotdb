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
package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;

/** has more than one input column, but these columns' time is overlapped */
public class MultiColumnMerger implements ColumnMerger {

  private final List<InputLocation> inputLocations;

  public MultiColumnMerger(List<InputLocation> inputLocations) {
    this.inputLocations = inputLocations;
  }

  @Override
  public void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      TimeColumnBuilder timeBuilder,
      long currentEndTime,
      ColumnBuilder columnBuilder) {
    int rowCount = timeBuilder.getPositionCount();

    // init startIndex for each input locations
    for (InputLocation inputLocation : inputLocations) {
      int tsBlockIndex = inputLocation.getTsBlockIndex();
      updatedInputIndex[tsBlockIndex] = inputIndex[tsBlockIndex];
    }

    for (int i = 0; i < rowCount; i++) {
      // record whether current row already has value to be appended
      boolean appendValue = false;
      // we don't use MinHeap here to choose the right column, because inputLocations.size() won't
      // be very large.
      // Assuming inputLocations.size() will be less than 5, performance of for-loop may be better
      // than PriorityQueue.
      for (InputLocation location : inputLocations) {
        int tsBlockIndex = location.getTsBlockIndex();
        int columnIndex = location.getValueColumnIndex();
        int index = updatedInputIndex[tsBlockIndex];

        // current location's input column is not empty
        if (!ColumnMerger.empty(tsBlockIndex, inputTsBlocks, updatedInputIndex)) {
          TimeColumn timeColumn = inputTsBlocks[tsBlockIndex].getTimeColumn();
          Column valueColumn = inputTsBlocks[tsBlockIndex].getColumn(columnIndex);
          // time of current location's input column is equal to current row's time
          if (timeColumn.getLong(index) == timeBuilder.getTime(i)) {
            // value of current location's input column is not null
            if (!valueColumn.isNull(index)) {
              columnBuilder.write(valueColumn, index);
              appendValue = true;
            }
            // increase the index
            index++;
            // update the index after merging
            updatedInputIndex[tsBlockIndex] = index;
            // we can safely set appendValue to true and then break the loop, because these input
            // columns' time is not overlapped
            if (appendValue) {
              break;
            }
          }
        }
      }
      // all input columns are null at current row, so just append a null
      if (!appendValue) {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  public void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      long currentTime,
      ColumnBuilder columnBuilder) {

    // init startIndex for each input locations
    for (InputLocation inputLocation : inputLocations) {
      int tsBlockIndex = inputLocation.getTsBlockIndex();
      updatedInputIndex[tsBlockIndex] = inputIndex[tsBlockIndex];
    }

    // record whether current row already has value to be appended
    boolean appendValue = false;
    // we don't use MinHeap here to choose the right column, because inputLocations.size() won't
    // be very large.
    // Assuming inputLocations.size() will be less than 5, performance of for-loop may be better
    // than PriorityQueue.
    for (InputLocation location : inputLocations) {
      int tsBlockIndex = location.getTsBlockIndex();
      int columnIndex = location.getValueColumnIndex();
      int index = updatedInputIndex[tsBlockIndex];

      // current location's input column is not empty
      if (!ColumnMerger.empty(tsBlockIndex, inputTsBlocks, updatedInputIndex)) {
        TimeColumn timeColumn = inputTsBlocks[tsBlockIndex].getTimeColumn();
        Column valueColumn = inputTsBlocks[tsBlockIndex].getColumn(columnIndex);
        // time of current location's input column is equal to current row's time
        if (timeColumn.getLong(index) == currentTime) {
          // value of current location's input column is not null
          // here we only append value if there is no value appended before and current value is
          // null
          // TODO That means we choose first value as the final value if there exist timestamp
          // belonging to more than one DataRegion, we need to choose which one is latest
          if (!appendValue && !valueColumn.isNull(index)) {
            columnBuilder.write(valueColumn, index);
            appendValue = true;
          }
          // increase the index
          index++;
          // update the index after merging
          updatedInputIndex[tsBlockIndex] = index;
          // we can never safely set appendValue to true and then break the loop, because these
          // input
          // columns' time may be overlapped, we should increase each column's index whose time is
          // equal to currentTime
          // if (appendValue) {
          //    break;
          //  }
        }
      }
    }
    // all input columns are null at current row, so just append a null
    if (!appendValue) {
      columnBuilder.appendNull();
    }
  }
}
