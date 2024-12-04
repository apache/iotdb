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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.AbstractOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_RESERVED_MEMORY;

public class MergeSortInnerJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortInnerJoinOperator.class);

  protected boolean leftFinished;
  protected final Operator leftChild;
  protected TsBlock leftBlock;
  protected int leftIndex; // start index of leftTsBlock
  protected final int leftJoinKeyPosition;
  protected final int[] leftOutputSymbolIdx;

  protected boolean rightFinished;
  protected final Operator rightChild;
  protected List<TsBlock> rightBlockList = new ArrayList<>();
  protected final int rightJoinKeyPosition;
  protected int rightBlockListIdx;
  protected int rightIndex; // start index of rightTsBlock
  protected final int[] rightOutputSymbolIdx;
  protected TsBlock cachedNextRightBlock; // next candidate right block after rightBlockList
  protected boolean rightConsumedUp = false; // if all data of right child are consumed up

  protected final JoinKeyComparator comparator;
  protected final TsBlockBuilder resultBuilder;

  protected final Type joinKeyType;

  protected MemoryReservationManager memoryReservationManager;

  protected long maxUsedMemory;
  protected long usedMemory;

  public MergeSortInnerJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int leftJoinKeyPosition,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int rightJoinKeyPosition,
      int[] rightOutputSymbolIdx,
      JoinKeyComparator joinKeyComparator,
      List<TSDataType> dataTypes,
      Type joinKeyType) {
    this.operatorContext = operatorContext;
    this.leftChild = leftChild;
    this.leftJoinKeyPosition = leftJoinKeyPosition;
    this.leftOutputSymbolIdx = leftOutputSymbolIdx;
    this.rightChild = rightChild;
    this.rightJoinKeyPosition = rightJoinKeyPosition;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;

    this.comparator = joinKeyComparator;
    this.joinKeyType = joinKeyType;
    this.resultBuilder = new TsBlockBuilder(dataTypes);

    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> leftBlocked = leftBlockNotEmpty() ? NOT_BLOCKED : leftChild.isBlocked();
    ListenableFuture<?> rightBlocked =
        rightBlockNotEmpty() && gotNextRightBlock() ? NOT_BLOCKED : rightChild.isBlocked();
    if (leftBlocked.isDone()) {
      return rightBlocked;
    } else if (rightBlocked.isDone()) {
      return leftBlocked;
    } else {
      return successfulAsList(leftBlocked, rightBlocked);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNext();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return !leftFinished && !rightFinished;
  }

  @Override
  public TsBlock next() throws Exception {

    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    // prepare leftBlock and rightBlockList with cachedNextRightBlock
    if (!prepareInput()) {
      return null;
    }

    // all the rightTsBlock is less than leftTsBlock, just skip right
    if (allRightLessThanLeft()) {
      resetRightBlockList();
      return null;
    }

    // all the leftTsBlock is less than rightTsBlock, just skip left
    else if (allLeftLessThanRight()) {
      resetLeftBlock();
      return null;
    }

    while (!resultBuilder.isFull()) {
      if (appendResultFinished() || System.nanoTime() - start > maxRuntime) {
        break;
      }
    }

    if (resultBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
  }

  protected boolean prepareInput() throws Exception {
    gotCandidateBlocks();
    return leftBlockNotEmpty() && rightBlockNotEmpty() && gotNextRightBlock();
  }

  protected boolean allRightLessThanLeft() {
    // check if the last value of the right is less than left
    return comparator.lessThan(
        rightBlockList.get(rightBlockList.size() - 1),
        rightJoinKeyPosition,
        rightBlockList.get(rightBlockList.size() - 1).getPositionCount() - 1,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex);
  }

  protected boolean allLeftLessThanRight() {
    return comparator.lessThan(
        leftBlock,
        leftJoinKeyPosition,
        leftBlock.getPositionCount() - 1,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex);
  }

  /**
   * @return true if current round of next() invoking should be finished
   */
  private boolean appendResultFinished() {
    // continue right < left, unless right >= left
    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex)) {
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }

    if (currentRoundNeedStop()) {
      return true;
    }

    // append all matched right values equal to current left
    appendMatchedValues();

    leftIndex++;
    if (leftIndex >= leftBlock.getPositionCount()) {
      resetLeftBlock();
      return true;
    }

    return false;
  }

  /**
   * @return true if right block is consumed up
   */
  protected boolean rightFinishedWithIncIndex() {
    rightIndex++;

    if (rightIndex >= rightBlockList.get(rightBlockListIdx).getPositionCount()) {
      rightBlockListIdx++;
      rightIndex = 0;
    }

    if (rightBlockListIdx >= rightBlockList.size()) {
      resetRightBlockList();
      return true;
    }

    return false;
  }

  /**
   * @return true if last value of rightBlockList.get(0) is less than current left or current right
   *     value. Need stop the round and rebuild rightBlockLists.
   */
  protected boolean currentRoundNeedStop() {
    if (comparator.lessThan(
            rightBlockList.get(0),
            rightJoinKeyPosition,
            rightBlockList.get(0).getPositionCount() - 1,
            rightBlockList.get(rightBlockListIdx),
            rightJoinKeyPosition,
            rightIndex)
        || comparator.lessThan(
            rightBlockList.get(0),
            rightJoinKeyPosition,
            rightBlockList.get(0).getPositionCount() - 1,
            leftBlock,
            leftJoinKeyPosition,
            leftIndex)) {
      for (int i = 0; i < rightBlockListIdx; i++) {
        long size = rightBlockList.get(i).getRetainedSizeInBytes();
        usedMemory -= size;
        memoryReservationManager.releaseMemoryCumulatively(size);
      }
      rightBlockList = rightBlockList.subList(rightBlockListIdx, rightBlockList.size());
      rightBlockListIdx = 0;
      return true;
    }

    return false;
  }

  private void appendMatchedValues() {
    int tmpBlockIdx = rightBlockListIdx;
    int tmpIdx = rightIndex;
    while (comparator.equalsTo(
        leftBlock,
        leftJoinKeyPosition,
        leftIndex,
        rightBlockList.get(tmpBlockIdx),
        rightJoinKeyPosition,
        tmpIdx)) {
      appendValueToResult(tmpBlockIdx, tmpIdx);

      tmpIdx++;
      if (tmpIdx >= rightBlockList.get(tmpBlockIdx).getPositionCount()) {
        tmpIdx = 0;
        tmpBlockIdx++;
      }

      if (tmpBlockIdx >= rightBlockList.size()) {
        break;
      }
    }
  }

  protected boolean leftBlockNotEmpty() {
    return leftBlock != null && leftIndex < leftBlock.getPositionCount();
  }

  protected boolean rightBlockNotEmpty() {
    return (!rightBlockList.isEmpty()
            && rightBlockListIdx < rightBlockList.size()
            && rightIndex < rightBlockList.get(rightBlockListIdx).getPositionCount())
        || cachedNextRightBlock != null;
  }

  protected void appendValueToResult(int tmpRightBlockListIdx, int tmpRightIndex) {
    appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);

    appendRightBlockData(
        rightBlockList,
        tmpRightBlockListIdx,
        tmpRightIndex,
        leftOutputSymbolIdx,
        rightOutputSymbolIdx,
        resultBuilder);

    resultBuilder.declarePosition();
  }

  protected void appendLeftBlockData(
      int[] leftOutputSymbolIdx, TsBlockBuilder resultBuilder, TsBlock leftBlock, int leftIndex) {
    for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
      int idx = leftOutputSymbolIdx[i];
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      if (leftBlock.getColumn(idx).isNull(leftIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(leftBlock.getColumn(idx), leftIndex);
      }
    }
  }

  protected void appendRightBlockData(
      List<TsBlock> rightBlockList,
      int rightBlockListIdx,
      int rightIndex,
      int[] leftOutputSymbolIdxArray,
      int[] rightOutputSymbolIdxArray,
      TsBlockBuilder resultBuilder) {
    for (int i = 0; i < rightOutputSymbolIdxArray.length; i++) {
      ColumnBuilder columnBuilder =
          resultBuilder.getColumnBuilder(leftOutputSymbolIdxArray.length + i);

      if (rightBlockList
          .get(rightBlockListIdx)
          .getColumn(rightOutputSymbolIdxArray[i])
          .isNull(rightIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(
            rightBlockList.get(rightBlockListIdx).getColumn(rightOutputSymbolIdxArray[i]),
            rightIndex);
      }
    }
  }

  protected void resetLeftBlock() {
    leftBlock = null;
    leftIndex = 0;
  }

  protected void resetRightBlockList() {
    for (TsBlock tsBlock : rightBlockList) {
      long size = tsBlock.getRetainedSizeInBytes();
      usedMemory -= size;
      memoryReservationManager.releaseMemoryCumulatively(size);
    }

    rightBlockList.clear();
    rightBlockListIdx = 0;
    rightIndex = 0;
  }

  protected void addRightBlockWithMemoryReservation(TsBlock block) {
    reserveMemory(block.getRetainedSizeInBytes());
    rightBlockList.add(block);
  }

  protected void reserveMemory(long size) {
    usedMemory += size;
    memoryReservationManager.reserveMemoryCumulatively(size);
    if (usedMemory > maxUsedMemory) {
      maxUsedMemory = usedMemory;
      operatorContext.recordSpecifiedInfo(MAX_RESERVED_MEMORY, Long.toString(maxUsedMemory));
    }
  }

  protected void gotCandidateBlocks() throws Exception {
    if (!leftBlockNotEmpty()) {
      if (leftChild.hasNextWithTimer()) {
        leftBlock = leftChild.nextWithTimer();
        leftIndex = 0;
      } else {
        leftFinished = true;
      }
    }

    if (rightBlockList.isEmpty()) {
      if (cachedNextRightBlock != null) {
        addRightBlockWithMemoryReservation(cachedNextRightBlock);
        cachedNextRightBlock = null;
        tryCachedNextRightTsBlock();
      } else {
        if (rightChild.hasNextWithTimer()) {
          TsBlock block = rightChild.nextWithTimer();
          if (block != null && !block.isEmpty()) {
            addRightBlockWithMemoryReservation(block);
          }
        } else {
          rightFinished = true;
        }
      }
    } else {
      if (cachedNextRightBlock == null) {
        tryCachedNextRightTsBlock();
      }
    }
  }

  protected void tryCachedNextRightTsBlock() throws Exception {
    if (!rightConsumedUp && rightChild.hasNextWithTimer()) {
      TsBlock block = rightChild.nextWithTimer();
      if (block != null) {
        // if first value of block equals to last value of rightBlockList.get(0), append this block
        // to
        // rightBlockList
        if (comparator.equalsTo(
            block,
            rightJoinKeyPosition,
            0,
            rightBlockList.get(0),
            rightJoinKeyPosition,
            rightBlockList.get(0).getPositionCount() - 1)) {
          addRightBlockWithMemoryReservation(block);
        } else {
          cachedNextRightBlock = block;
        }
      }
    } else {
      rightConsumedUp = true;
      cachedNextRightBlock = null;
    }
  }

  protected boolean gotNextRightBlock() {
    return cachedNextRightBlock != null || rightConsumedUp;
  }

  protected void buildResultTsBlock() {
    resultTsBlock =
        resultBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
    resultBuilder.reset();
  }

  @Override
  public void close() throws Exception {
    if (leftChild != null) {
      leftChild.close();
    }
    if (rightChild != null) {
      rightChild.close();
    }

    if (!rightBlockList.isEmpty()) {
      for (TsBlock block : rightBlockList) {
        memoryReservationManager.releaseMemoryCumulatively(block.getRetainedSizeInBytes());
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        Math.max(
            leftChild.calculateMaxPeekMemoryWithCounter(),
            rightChild.calculateMaxPeekMemoryWithCounter()),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    // leftTsBlock + leftChild.RetainedSizeAfterCallingNext + rightTsBlock +
    // rightChild.RetainedSizeAfterCallingNext
    return leftChild.calculateMaxReturnSize()
        + leftChild.calculateRetainedSizeAfterCallingNext()
        + rightChild.calculateMaxReturnSize()
        + rightChild.calculateRetainedSizeAfterCallingNext()
        + maxReturnSize;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(leftChild)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(rightChild)
        + RamUsageEstimator.sizeOf(leftOutputSymbolIdx)
        + RamUsageEstimator.sizeOf(rightOutputSymbolIdx)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes();
  }
}
