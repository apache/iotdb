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

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

/** used to merge columns belonging to same series into one column */
public interface ColumnMerger {

  /**
   * @param tsBlockIndex index
   * @param inputTsBlocks input TsBlock array
   * @param inputIndex current index for each input TsBlock and size of it is equal to inputTsBlocks
   * @return true if TsBlock at tsBlockIndex is null or its current read index is larger than its
   *     size
   */
  static boolean empty(int tsBlockIndex, TsBlock[] inputTsBlocks, int[] inputIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == inputIndex[tsBlockIndex];
  }

  /**
   * merge columns belonging to same series into one column, merge until each input column's time is
   * larger than currentEndTime
   *
   * @param inputTsBlocks all source TsBlocks, some of which will contain source column
   * @param inputIndex start index for each source TsBlock and size of it is equal to inputTsBlocks,
   *     we should only read from this array and not update it because others will use the start
   *     index value in inputIndex array
   * @param updatedInputIndex current index for each source TsBlock after merging
   * @param timeBuilder result time column, which is already generated and used to indicate each
   *     row's timestamp
   * @param currentEndTime merge until each input column's time is larger than currentEndTime
   * @param columnBuilder used to write merged value into
   */
  void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      TimeColumnBuilder timeBuilder,
      long currentEndTime,
      ColumnBuilder columnBuilder);

  /**
   * merge columns belonging to same series into one column, merge just one row whose time is equal
   * to currentTime
   *
   * @param inputTsBlocks all source TsBlocks, some of which will contain source column
   * @param inputIndex start index for each source TsBlock and size of it is equal to inputTsBlocks,
   *     we should only read from this array and not update it because others will use the start
   *     index value in inputIndex array
   * @param updatedInputIndex current index for each source TsBlock after merging
   * @param currentTime merge just one row whose time is equal to currentTime
   * @param columnBuilder used to write merged value into
   */
  void mergeColumn(
      TsBlock[] inputTsBlocks,
      int[] inputIndex,
      int[] updatedInputIndex,
      long currentTime,
      ColumnBuilder columnBuilder);
}
