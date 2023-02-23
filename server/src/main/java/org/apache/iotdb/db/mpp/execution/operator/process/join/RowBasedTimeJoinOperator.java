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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.AbstractConsumeAllOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.successfulAsList;

public class RowBasedTimeJoinOperator extends AbstractConsumeAllOperator {

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

  public RowBasedTimeJoinOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      Ordering mergeOrder,
      List<TSDataType> dataTypes,
      List<ColumnMerger> mergers,
      TimeComparator comparator) {
    super(operatorContext, children);
    checkArgument(!children.isEmpty(), "child size of TimeJoinOperator should be larger than 0");
    this.inputIndex = new int[this.inputOperatorsCount];
    this.shadowInputIndex = new int[this.inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.timeSelector = new TimeSelector(this.inputOperatorsCount << 1, Ordering.ASC == mergeOrder);
    this.outputColumnCount = dataTypes.size();
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.mergers = mergers;
    this.comparator = comparator;
    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            (1L + outputColumnCount)
                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i)) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        canCallNext[i] = true;
      } else {
        listenableFutures.add(blocked);
      }
    }
    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  @Override
  public TsBlock next() {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    tsBlockBuilder.reset();
    if (!prepareInput()) {
      return null;
    }

    // end time for returned TsBlock this time, it's the min/max end time among all the children
    // TsBlocks order by asc/desc
    long currentEndTime = 0;
    boolean init = false;

    // get TsBlock for each input, put their time stamp into TimeSelector and then use the min Time
    // among all the input TsBlock as the current output TsBlock's endTime.
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i]) {
        // update the currentEndTime if the TsBlock is not empty
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
    long currentTime;
    do {
      currentTime = timeSelector.pollFirst();
      timeBuilder.writeLong(currentTime);
      for (int i = 0; i < outputColumnCount; i++) {
        ColumnMerger merger = mergers.get(i);
        merger.mergeColumn(
            inputTsBlocks,
            inputIndex,
            shadowInputIndex,
            currentTime,
            tsBlockBuilder.getColumnBuilder(i));
      }

      for (int i = 0; i < inputOperatorsCount; i++) {
        if (inputIndex[i] != shadowInputIndex[i]) {
          inputIndex[i] = shadowInputIndex[i];
          if (!isEmpty(i)) {
            updateTimeSelector(i);
          }
        }
      }
      tsBlockBuilder.declarePosition();
    } while (currentTime < currentEndTime && !timeSelector.isEmpty());

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
      if (!isEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
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
      if (!noMoreTsBlocks[i] || !isEmpty(i)) {
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

  private void updateTimeSelector(int index) {
    timeSelector.add(inputTsBlocks[index].getTimeByIndex(inputIndex[index]));
  }

  /**
   * Try to cache one result of each child.
   *
   * @return true if results of all children are ready or have no more TsBlocks. Return false if
   *     some children is blocked or return null.
   */
  @Override
  protected boolean prepareInput() {
    boolean allReady = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i)) {
        continue;
      }
      if (canCallNext[i]) {
        if (children.get(i).hasNextWithTimer()) {
          inputTsBlocks[i] = getNextTsBlock(i);
          canCallNext[i] = false;
          if (isEmpty(i)) {
            allReady = false;
          } else {
            updateTimeSelector(i);
          }
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      } else {
        allReady = false;
      }
    }
    return allReady;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return children;
  }

  @Override
  protected TsBlock getNextTsBlock(int childIndex) {
    inputIndex[childIndex] = 0;
    return children.get(childIndex).nextWithTimer();
  }

  /**
   * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  @Override
  protected boolean isEmpty(int columnIndex) {
    return inputTsBlocks[columnIndex] == null
        || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
  }
}
