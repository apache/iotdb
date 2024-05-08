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
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;

/** has more than one input column, but these columns' time is not overlapped */
public class NonOverlappedMultiColumnMerger implements ColumnMerger {

  private final List<InputLocation> inputLocations;

  private final TimeComparator comparator;

  // index for inputLocations indicating current iterating TsBlock's InputLocation
  private int index;

  /**
   * these columns' time should never be overlapped
   *
   * @param inputLocations The time order in TsBlock represented by inputLocations should be
   *     incremented by timestamp if it is order by time asc, otherwise decreased by timestamp if it
   *     is order by time desc
   */
  public NonOverlappedMultiColumnMerger(
      List<InputLocation> inputLocations, TimeComparator comparator) {
    this.inputLocations = inputLocations;
    this.comparator = comparator;
    this.index = 0;
  }

  @Override
  public void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      TimeColumnBuilder timeBuilder,
      long currentEndTime,
      ColumnBuilder columnBuilder) {
    // move to next InputLocation if current InputLocation's column has been consumed up
    moveToNextIfNecessary(inputTsBlocks);
    // merge current column
    SingleColumnMerger.mergeOneColumn(
        inputTsBlocks,
        inputIndex,
        updatedInputIndex,
        timeBuilder,
        currentEndTime,
        columnBuilder,
        inputLocations.get(index),
        comparator);
  }

  @Override
  public void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      long currentTime,
      ColumnBuilder columnBuilder) {
    // move to next InputLocation if current InputLocation's column has been consumed up
    moveToNextIfNecessary(inputTsBlocks);
    // merge current column
    SingleColumnMerger.mergeOneColumn(
        inputTsBlocks,
        inputIndex,
        updatedInputIndex,
        currentTime,
        columnBuilder,
        inputLocations.get(index));
  }

  private void moveToNextIfNecessary(TsBlock[] inputTsBlocks) {
    // if it is already at the last index, don't need to move
    if (index == inputLocations.size() - 1) {
      return;
    }
    // if inputTsBlocks[tsBlockIndex] is null, means current index's TsBlock has been consumed up
    while (index < inputLocations.size() - 1
        && inputTsBlocks[inputLocations.get(index).getTsBlockIndex()] == null) {
      index++;
    }
  }
}
