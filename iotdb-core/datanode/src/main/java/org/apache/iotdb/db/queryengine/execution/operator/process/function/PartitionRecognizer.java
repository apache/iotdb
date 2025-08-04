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

package org.apache.iotdb.db.queryengine.execution.operator.process.function;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;

public class PartitionRecognizer {

  private final Comparator<SortKey> partitionComparator;
  private final List<Integer> requiredChannels;
  private final List<Integer> passThroughChannels;
  private final List<Type> inputDataTypes;

  private boolean noMoreData = false;

  private SortKey currentPartitionKey;
  private TsBlock currentTsBlock = null;
  private int currentIndex = 0;
  private PartitionState currentState = PartitionState.INIT_STATE;

  public PartitionRecognizer(
      List<Integer> partitionChannels,
      List<Integer> requiredChannels,
      List<Integer> passThroughChannels,
      List<TSDataType> inputDataTypes) {
    this.currentPartitionKey = null;
    if (partitionChannels.isEmpty()) {
      // always return 0
      this.partitionComparator = (o1, o2) -> 0;
    } else {
      this.partitionComparator =
          getComparatorForTable(
              partitionChannels.stream().map(i -> ASC_NULLS_FIRST).collect(Collectors.toList()),
              partitionChannels,
              partitionChannels.stream().map(inputDataTypes::get).collect(Collectors.toList()));
    }
    this.requiredChannels = requiredChannels;
    this.passThroughChannels = passThroughChannels;
    this.inputDataTypes = UDFDataTypeTransformer.transformToUDFDataTypeList(inputDataTypes);
  }

  // TsBlock is sorted by partition columns already
  public void addTsBlock(TsBlock tsBlock) {
    if (noMoreData) {
      throw new IllegalArgumentException(
          "The partition handler is finished, cannot add more data.");
    }
    currentTsBlock = tsBlock;
  }

  /** Marks the handler as finished. */
  public void noMoreData() {
    noMoreData = true;
  }

  public PartitionState nextState() {
    updateState();
    return currentState;
  }

  private void updateState() {
    switch (currentState.getStateType()) {
      case INIT:
        currentState = handleInitState();
        break;
      case ITERATING:
      case NEW_PARTITION:
        currentState = handleIteratingOrNewPartitionState();
        break;
      case NEED_MORE_DATA:
        currentState = handleNeedMoreDataState();
        break;
      case FINISHED:
        // do nothing
        return;
    }
    if (PartitionState.NEED_MORE_DATA_STATE.equals(currentState)) {
      currentIndex = 0;
      currentTsBlock = null;
    }
  }

  private PartitionState handleInitState() {
    if (noMoreData) {
      return PartitionState.FINISHED_STATE;
    } else if (currentTsBlock == null || currentTsBlock.isEmpty()) {
      return PartitionState.INIT_STATE;
    }
    // init the partition Key as the first row
    currentPartitionKey = new SortKey(currentTsBlock, currentIndex);
    int endPartitionIndex = findNextDifferentRowIndex();
    Slice slice = getSlice(currentIndex, endPartitionIndex);
    currentIndex = endPartitionIndex;
    return PartitionState.newPartitionState(slice);
  }

  private PartitionState handleNeedMoreDataState() {
    if (noMoreData) {
      return PartitionState.FINISHED_STATE;
    } else if (currentTsBlock == null || currentTsBlock.isEmpty()) {
      return PartitionState.NEED_MORE_DATA_STATE;
    }
    int endPartitionIndex = findNextDifferentRowIndex();
    if (endPartitionIndex != 0) {
      Slice slice = getSlice(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.iteratingState(slice);
    } else {
      endPartitionIndex = findNextDifferentRowIndex();
      Slice slice = getSlice(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.newPartitionState(slice);
    }
  }

  private PartitionState handleIteratingOrNewPartitionState() {
    if (currentIndex >= currentTsBlock.getPositionCount()) {
      return PartitionState.NEED_MORE_DATA_STATE;
    } else {
      int endPartitionIndex = findNextDifferentRowIndex();
      Slice slice = getSlice(currentIndex, endPartitionIndex);
      currentIndex = endPartitionIndex;
      return PartitionState.newPartitionState(slice);
    }
  }

  /**
   * Find next row index whose partition values are different from the current partition values. If
   * all rows have the same partition values, return the position count of the current TsBlock.
   */
  private int findNextDifferentRowIndex() {
    int totalRows = currentTsBlock.getPositionCount();

    // check if all rows have the same partition values
    SortKey compareKey = new SortKey(currentTsBlock, totalRows - 1);
    if (partitionComparator.compare(currentPartitionKey, compareKey) == 0) {
      return totalRows;
    }

    // check the first row
    compareKey.rowIndex = currentIndex;
    if (partitionComparator.compare(currentPartitionKey, compareKey) != 0) {
      currentPartitionKey = compareKey;
      return currentIndex;
    }

    // binary search to find the next different partition values
    int low = currentIndex;
    int high = totalRows - 1;
    int firstDiff = totalRows;
    while (low <= high) {
      compareKey.rowIndex = low + (high - low) / 2;
      int cmp = partitionComparator.compare(currentPartitionKey, compareKey);
      if (cmp == 0) {
        low = compareKey.rowIndex + 1;
      } else {
        // try to find earlier different row
        firstDiff = compareKey.rowIndex;
        high = compareKey.rowIndex - 1;
      }
    }
    compareKey.rowIndex = firstDiff;
    currentPartitionKey = compareKey;
    return firstDiff;
  }

  private Slice getSlice(int startPartitionIndex, int endPartitionIndex) {
    return new Slice(
        startPartitionIndex,
        endPartitionIndex,
        currentTsBlock.getValueColumns(),
        requiredChannels,
        passThroughChannels,
        inputDataTypes);
  }
}
