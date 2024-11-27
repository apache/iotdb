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
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TableFullOuterJoinOperator extends TableInnerJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableFullOuterJoinOperator.class);

  private boolean leftFinished;
  private boolean rightFinished;
  private long lastMatchedRightTime = Long.MIN_VALUE;

  public TableFullOuterJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int leftTimeColumnPosition,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int rightTimeColumnPosition,
      int[] rightOutputSymbolIdx,
      TimeComparator timeComparator,
      List<TSDataType> dataTypes) {
    super(
        operatorContext,
        leftChild,
        leftTimeColumnPosition,
        leftOutputSymbolIdx,
        rightChild,
        rightTimeColumnPosition,
        rightOutputSymbolIdx,
        timeComparator,
        dataTypes);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return (leftBlockNotEmpty() || leftChild.hasNextWithTimer())
        || (rightBlockNotEmpty() || rightChild.hasNextWithTimer());
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultBuilder.reset();

    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    // prepare leftBlock and rightBlockList with cachedNextRightBlock
    if (!prepareInput(start, maxRuntime)) {
      return null;
    }

    if (leftFinished || rightFinished) {
      if (leftFinished) {
        appendRightWithEmptyLeft();
        resetRightBlockList();
      } else {
        appendLeftWithEmptyRight();
        leftBlock = null;
        leftIndex = 0;
      }

      resultTsBlock = buildResultTsBlock(resultBuilder);
      return checkTsBlockSizeAndGetResult();
    }

    // all the rightTsBlock is less than leftTsBlock, append right with empty left
    if (comparator.lessThan(getRightEndTime(), getCurrentLeftTime())) {
      appendRightWithEmptyLeft();
      resetRightBlockList();
      resultTsBlock = buildResultTsBlock(resultBuilder);
      return checkTsBlockSizeAndGetResult();
    }

    // all the leftTsBlock is less than rightTsBlock, append left with empty right
    else if (comparator.lessThan(getLeftEndTime(), getCurrentRightTime())) {
      appendLeftWithEmptyRight();
      leftBlock = null;
      leftIndex = 0;
      resultTsBlock = buildResultTsBlock(resultBuilder);
      return checkTsBlockSizeAndGetResult();
    }

    long leftProbeTime = getCurrentLeftTime();
    while (!resultBuilder.isFull()) {

      // all right block time is not matched
      if (!comparator.canContinueInclusive(leftProbeTime, getRightEndTime())) {
        appendRightWithEmptyLeft();
        resetRightBlockList();
        break;
      }

      appendResult(leftProbeTime);

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }

      leftProbeTime = getCurrentLeftTime();
    }

    if (resultBuilder.isEmpty()) {
      return null;
    }

    resultTsBlock = buildResultTsBlock(resultBuilder);
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  protected boolean prepareInput(long start, long maxRuntime) throws Exception {

    if (!leftFinished && (leftBlock == null || leftBlock.getPositionCount() == leftIndex)) {
      if (leftChild.hasNextWithTimer()) {
        leftBlock = leftChild.nextWithTimer();
        leftIndex = 0;
      } else {
        leftFinished = true;
      }
    }

    if (!rightFinished) {
      if (rightBlockList.isEmpty()) {
        if (hasCachedNextRightBlock && cachedNextRightBlock != null) {
          rightBlockList.add(cachedNextRightBlock);
          hasCachedNextRightBlock = false;
          cachedNextRightBlock = null;
          tryCachedNextRightTsBlock();
        } else if (rightChild.hasNextWithTimer()) {
          TsBlock block = rightChild.nextWithTimer();
          if (block != null) {
            rightBlockList.add(block);
            tryCachedNextRightTsBlock();
          }
        } else {
          rightFinished = true;
          hasCachedNextRightBlock = true;
          cachedNextRightBlock = null;
        }
      } else {
        if (!hasCachedNextRightBlock) {
          tryCachedNextRightTsBlock();
        }
      }
    }

    return (leftBlockNotEmpty() && rightBlockNotEmpty() && hasCachedNextRightBlock)
        || (leftBlockNotEmpty() && rightFinished)
        || (leftFinished && rightBlockNotEmpty() && hasCachedNextRightBlock);
  }

  @Override
  protected void appendResult(long leftTime) {

    while (comparator.lessThan(getCurrentRightTime(), leftTime)) {
      // getCurrentRightTime() can only be greater than lastMatchedRightTime
      // if greater than, then put right
      // if equals, it has been put in last round
      // notice: must examine `comparator.lessThan(getCurrentRightTime(), leftTime)` then examine
      // `comparator.lessThan(leftTime, getCurrentRightTime())`
      if (getCurrentRightTime() > lastMatchedRightTime) {
        appendOneRightRowWithEmptyLeft();
      }

      if (rightBlockFinish()) {
        return;
      }
    }

    if (comparator.lessThan(leftTime, getCurrentRightTime())) {
      appendOneLeftRowWithEmptyRight();
      leftIndex++;
      return;
    }

    int tmpBlockIdx = rightBlockListIdx, tmpIdx = rightIndex;
    while (leftTime == getRightTime(tmpBlockIdx, tmpIdx)) {
      // lastMatchedRightBlockListIdx = rightBlockListIdx;
      // lastMatchedRightIdx = rightIndex;
      lastMatchedRightTime = leftTime;
      appendValueToResult(tmpBlockIdx, tmpIdx);

      resultBuilder.declarePosition();

      tmpIdx++;
      if (tmpIdx >= rightBlockList.get(tmpBlockIdx).getPositionCount()) {
        tmpIdx = 0;
        tmpBlockIdx++;
      }

      if (tmpBlockIdx >= rightBlockList.size()) {
        break;
      }
    }
    leftIndex++;
  }

  private void appendLeftWithEmptyRight() {
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

  private void appendRightWithEmptyLeft() {
    while (rightBlockListIdx < rightBlockList.size()) {

      if (getCurrentRightTime() > lastMatchedRightTime) {
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

      rightIndex++;
      if (rightIndex >= rightBlockList.get(rightBlockListIdx).getPositionCount()) {
        rightIndex = 0;
        rightBlockListIdx++;
      }
    }
  }

  private void appendOneRightRowWithEmptyLeft() {
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

  private void appendOneLeftRowWithEmptyRight() {
    appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);

    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);
      columnBuilder.appendNull();
    }

    resultBuilder.declarePosition();
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
    return maxReturnSize * 2;
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
