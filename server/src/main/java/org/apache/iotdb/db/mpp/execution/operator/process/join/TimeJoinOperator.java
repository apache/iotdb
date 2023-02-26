/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.operator.process.join;

import org.apache.iotdb.db.mpp.execution.operator.AbstractOperator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.successfulAsList;

@Deprecated
public class TimeJoinOperator extends AbstractOperator {

  private final List<Operator> children;

  private final int inputOperatorsCount;

  /** TsBlock from child operator. Only one cache now. */
  private final TsBlock[] inputTsBlocks;

  /** start index for each input TsBlocks and size of it is equal to inputTsBlocks */
  private final int[] inputIndex;

  /** used to record current index for input TsBlocks after merging */
  private final int[] shadowInputIndex;

  /**
   * Represent whether there are more tsBlocks from ith child operator. If all elements in
   * noMoreTsBlocks[] are true and inputTsBlocks[] are consumed completely, this operator is
   * finished.
   */
  private final boolean[] noMoreTsBlocks;

  private final TimeSelector timeSelector;

  private final int outputColumnCount;

  /**
   * this field indicates each data type for output columns(not including time column) of
   * TimeJoinOperator its size should be equal to outputColumnCount
   */
  private final List<TSDataType> dataTypes;

  private final List<ColumnMerger> mergers;

  private final TsBlockBuilder tsBlockBuilder;

  private boolean finished;

  private final TimeComparator comparator;

  public TimeJoinOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      Ordering mergeOrder,
      List<TSDataType> dataTypes,
      List<ColumnMerger> mergers,
      TimeComparator comparator) {
    checkArgument(
        children != null && !children.isEmpty(),
        "child size of TimeJoinOperator should be larger than 0");
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[this.inputOperatorsCount];
    this.inputIndex = new int[this.inputOperatorsCount];
    this.shadowInputIndex = new int[this.inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.timeSelector = new TimeSelector(this.inputOperatorsCount << 1, Ordering.ASC == mergeOrder);
    this.outputColumnCount = dataTypes.size();
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.mergers = mergers;
    this.comparator = comparator;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
  }

  @Override
  public TsBlock next() {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    tsBlockBuilder.reset();
    // end time for returned TsBlock this time, it's the min/max end time among all the children
    // TsBlocks order by asc/desc
    long currentEndTime = 0;
    boolean init = false;

    // get TsBlock for each input, put their time stamp into TimeSelector and then use the min Time
    // among all the input TsBlock as the current output TsBlock's endTime.
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        if (children.get(i).hasNextWithTimer()) {
          inputIndex[i] = 0;
          inputTsBlocks[i] = children.get(i).nextWithTimer();
          if (!empty(i)) {
            int rowSize = inputTsBlocks[i].getPositionCount();
            for (int row = 0; row < rowSize; row++) {
              timeSelector.add(inputTsBlocks[i].getTimeByIndex(row));
            }
          } else {
            // child operator has next but return an empty TsBlock which means that it may not
            // finish calculation in given time slice.
            // In such case, TimeJoinOperator can't go on calculating, so we just return null.
            // We can also use the while loop here to continuously call the hasNext() and next()
            // methods of the child operator until its hasNext() returns false or the next() gets
            // the data that is not empty, but this will cause the execution time of the while loop
            // to be uncontrollable and may exceed all allocated time slice
            return null;
          }
        } else { // no more tsBlock
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
      // update the currentEndTime if the TsBlock is not empty
      if (!empty(i)) {
        currentEndTime =
            init
                ? comparator.getCurrentEndTime(currentEndTime, inputTsBlocks[i].getEndTime())
                : inputTsBlocks[i].getEndTime();
        init = true;
      }
    }

    if (timeSelector.isEmpty()) {
      // return empty TsBlock
      TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(0, dataTypes);
      return tsBlockBuilder.build();
    }

    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    while (!timeSelector.isEmpty()
        && comparator.satisfyCurEndTime(timeSelector.first(), currentEndTime)) {
      timeBuilder.writeLong(timeSelector.pollFirst());
      tsBlockBuilder.declarePosition();
    }

    for (int i = 0; i < outputColumnCount; i++) {
      ColumnMerger merger = mergers.get(i);
      merger.mergeColumn(
          inputTsBlocks,
          inputIndex,
          shadowInputIndex,
          timeBuilder,
          currentEndTime,
          tsBlockBuilder.getColumnBuilder(i));
    }

    // update inputIndex using shadowInputIndex
    System.arraycopy(shadowInputIndex, 0, inputIndex, 0, inputOperatorsCount);

    resultTsBlock = tsBlockBuilder.build();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    if (retainedTsBlock != null) {
      return true;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!empty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (children.get(i).hasNextWithTimer()) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    if (retainedTsBlock != null) {
      return false;
    }

    finished = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      // has more tsBlock output from children[i] or has cached tsBlock in inputTsBlocks[i]
      if (!noMoreTsBlocks[i] || !empty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemory());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }

    maxPeekMemory += calculateMaxReturnSize();
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0, minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock
    return currentRetainedSize - minChildReturnSize;
  }

  /**
   * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  private boolean empty(int columnIndex) {
    return inputTsBlocks[columnIndex] == null
        || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
  }
}
