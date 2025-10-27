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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_RESERVED_MEMORY;

public abstract class AbstractMergeSortJoinOperator extends AbstractOperator {
  protected boolean leftFinished;
  protected final Operator leftChild;
  protected TsBlock leftBlock;
  protected int leftIndex; // start index of leftTsBlock
  protected final int[] leftJoinKeyPositions;
  protected final int[] leftOutputSymbolIdx;

  protected boolean rightFinished;
  protected final Operator rightChild;
  protected List<TsBlock> rightBlockList = new ArrayList<>();
  protected final int[] rightJoinKeyPositions;
  protected int rightBlockListIdx;
  protected int rightIndex; // start index of rightTsBlock
  protected final int[] rightOutputSymbolIdx;
  protected TsBlock cachedNextRightBlock; // next candidate right block after rightBlockList
  protected boolean rightConsumedUp = false; // if all data of right child are consumed up

  protected final List<JoinKeyComparator> comparators;
  protected final TsBlockBuilder resultBuilder;

  protected final MemoryReservationManager memoryReservationManager;

  protected long maxUsedMemory;
  protected long usedMemory;

  protected AbstractMergeSortJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int[] leftJoinKeyPositions,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int[] rightJoinKeyPositions,
      int[] rightOutputSymbolIdx,
      List<JoinKeyComparator> comparators,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.leftChild = leftChild;
    this.leftJoinKeyPositions = leftJoinKeyPositions;
    this.leftOutputSymbolIdx = leftOutputSymbolIdx;
    this.rightChild = rightChild;
    this.rightJoinKeyPositions = rightJoinKeyPositions;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;
    this.comparators = comparators;

    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();

    this.resultBuilder = new TsBlockBuilder(dataTypes);
  }

  /** prepare leftBlock and rightBlockList with cachedNextRightBlock */
  protected abstract boolean prepareInput() throws Exception;

  /**
   * @return true if current round of next() invoking should be finished
   */
  protected abstract boolean processFinished();

  /**
   * This method will be invoked when any left data matches right data on the join criteria. Only
   * outer join will use this method currently, outer join may record the last matched rows to avoid
   * append results which is already output.
   */
  protected abstract void recordsWhenDataMatches();

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
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    if (!prepareInput()) {
      return null;
    }

    while (!resultBuilder.isFull()) {
      if (processFinished() || System.nanoTime() - start > maxRuntime) {
        break;
      }
    }

    if (resultBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
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

  protected boolean gotNextRightBlock() {
    return cachedNextRightBlock != null || rightConsumedUp;
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

  protected boolean currentLeftHasNullValue() {
    for (int leftJoinKeyPosition : leftJoinKeyPositions) {
      if (leftBlock.getColumn(leftJoinKeyPosition).isNull(leftIndex)) {
        return true;
      }
    }
    return false;
  }

  protected boolean currentRightHasNullValue() {
    for (int rightJoinKeyPosition : rightJoinKeyPositions) {
      if (rightBlockList
          .get(rightBlockListIdx)
          .getColumn(rightJoinKeyPosition)
          .isNull(rightIndex)) {
        return true;
      }
    }
    return false;
  }

  // check if the last value of the right is less than left
  protected boolean allRightLessThanLeft() {
    return lessThan(
        rightBlockList.get(rightBlockList.size() - 1),
        rightJoinKeyPositions,
        rightBlockList.get(rightBlockList.size() - 1).getPositionCount() - 1,
        leftBlock,
        leftJoinKeyPositions,
        leftIndex);
  }

  // check if the last value of the left is less than right
  protected boolean allLeftLessThanRight() {
    return lessThan(
        leftBlock,
        leftJoinKeyPositions,
        leftBlock.getPositionCount() - 1,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPositions,
        rightIndex);
  }

  /**
   * Examine if stop this round and rebuild rightBlockLists.
   *
   * @return true if last value of rightBlockList.get(0) is less than current left or current right
   *     value.
   */
  protected boolean currentRoundNeedStop() {
    if (lessThan(
            rightBlockList.get(0),
            rightJoinKeyPositions,
            rightBlockList.get(0).getPositionCount() - 1,
            rightBlockList.get(rightBlockListIdx),
            rightJoinKeyPositions,
            rightIndex)
        || lessThan(
            rightBlockList.get(0),
            rightJoinKeyPositions,
            rightBlockList.get(0).getPositionCount() - 1,
            leftBlock,
            leftJoinKeyPositions,
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

  /**
   * @return true if current left block is consumed up
   */
  protected boolean leftFinishedWithIncIndex() {
    leftIndex++;
    if (leftIndex >= leftBlock.getPositionCount()) {
      resetLeftBlock();
      return true;
    }
    return false;
  }

  /**
   * @return true if current right block is consumed up
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
        if (rightChild.isBlocked().isDone()) {
          tryCacheNextRightTsBlock();
        }
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
        tryCacheNextRightTsBlock();
      } else {
        // if first value of block equals to last value of rightBlockList.get(0), append this block
        // to
        // rightBlockList
        if (equalsTo(
            cachedNextRightBlock,
            rightJoinKeyPositions,
            0,
            rightBlockList.get(0),
            rightJoinKeyPositions,
            rightBlockList.get(0).getPositionCount() - 1)) {
          addRightBlockWithMemoryReservation(cachedNextRightBlock);
          cachedNextRightBlock = null;
        }
      }
    }
  }

  protected void tryCacheNextRightTsBlock() throws Exception {
    if (!rightConsumedUp && rightChild.hasNextWithTimer()) {
      TsBlock block = rightChild.nextWithTimer();
      if (block != null && !block.isEmpty()) {
        // if first value of block equals to last value of rightBlockList.get(0), append this block
        // to
        // rightBlockList
        if (equalsTo(
            block,
            rightJoinKeyPositions,
            0,
            rightBlockList.get(0),
            rightJoinKeyPositions,
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

  protected void appendValueToResultWhenMatches(int tmpRightBlockListIdx, int tmpRightIndex) {
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

  protected boolean hasMatchedRightValueToProbeLeft() {
    int tmpBlockIdx = rightBlockListIdx;
    int tmpIdx = rightIndex;
    boolean hasMatched = false;
    while (equalsTo(
        leftBlock,
        leftJoinKeyPositions,
        leftIndex,
        rightBlockList.get(tmpBlockIdx),
        rightJoinKeyPositions,
        tmpIdx)) {
      hasMatched = true;
      recordsWhenDataMatches();
      appendValueToResultWhenMatches(tmpBlockIdx, tmpIdx);

      tmpIdx++;
      if (tmpIdx >= rightBlockList.get(tmpBlockIdx).getPositionCount()) {
        tmpIdx = 0;
        tmpBlockIdx++;
      }

      if (tmpBlockIdx >= rightBlockList.size()) {
        break;
      }
    }
    return hasMatched;
  }

  protected boolean lessThan(
      TsBlock leftBlock,
      int[] leftPositions,
      int lIndex,
      TsBlock rightBlock,
      int[] rightPositions,
      int rIndex) {
    return examineLessThan(leftBlock, leftPositions, lIndex, rightBlock, rightPositions, rIndex);
  }

  // examine lessThan( L: [a, b], R[a', b'])
  // if a < a' ==> L < R
  // else
  //   if a == a', continue examine if b < b'
  //   else ==> L > R
  protected boolean examineLessThan(
      TsBlock leftBlock,
      int[] leftPositions,
      int lIndex,
      TsBlock rightBlock,
      int[] rightPositions,
      int rIndex) {
    for (int i = 0; i < comparators.size(); i++) {
      if (comparators
          .get(i)
          .lessThan(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return true;
      } else if (!comparators
          .get(i)
          .equalsTo(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return false;
      }
    }

    return false;
  }

  protected boolean equalsTo(
      TsBlock leftBlock,
      int[] leftPositions,
      int lIndex,
      TsBlock rightBlock,
      int[] rightPositions,
      int rIndex) {
    for (int i = 0; i < comparators.size(); i++) {
      if (!comparators
          .get(i)
          .equalsTo(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return false;
      }
    }
    return true;
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

  protected void appendLeftWithEmptyRight() {
    while (leftIndex < leftBlock.getPositionCount()) {
      appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);

      for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
        ColumnBuilder columnBuilder =
            resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);
        columnBuilder.appendNull();
      }

      resultBuilder.declarePosition();
      leftIndex++;
    }
  }

  protected void appendOneRightRowWithEmptyLeft() {
    for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      columnBuilder.appendNull();
    }

    appendRightBlockData(
        rightBlockList,
        rightBlockListIdx,
        rightIndex,
        leftOutputSymbolIdx,
        rightOutputSymbolIdx,
        resultBuilder);

    resultBuilder.declarePosition();
  }

  protected void appendOneLeftRowWithEmptyRight() {
    appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);

    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);
      columnBuilder.appendNull();
    }

    resultBuilder.declarePosition();
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
    return leftChild.calculateMaxReturnSize()
        + leftChild.calculateRetainedSizeAfterCallingNext()
        + rightChild.calculateMaxReturnSize()
        + rightChild.calculateRetainedSizeAfterCallingNext()
        + maxReturnSize;
  }
}
