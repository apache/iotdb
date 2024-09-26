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
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableInnerJoinOperator.buildResultTsBlock;

public class TableFullOuterJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableFullOuterJoinOperator.class);

  private final Operator leftChild;
  private TsBlock leftBlock;
  private final int leftTimeColumnPosition;
  private int leftIndex; // start index of leftTsBlock
  private final int[] leftOutputSymbolIdx;
  private boolean leftFinished;

  private final Operator rightChild;
  private final List<TsBlock> rightBlockList = new ArrayList<>();
  private final int rightTimeColumnPosition;
  private int rightBlockListIdx;
  private int rightIndex; // start index of rightTsBlock
  private final int[] rightOutputSymbolIdx;
  private TsBlock cachedNextRightBlock;
  private boolean hasCachedNextRightBlock;
  private boolean rightFinished;
  private long lastMatchedRightTime = Long.MIN_VALUE;

  private final TimeComparator comparator;
  private final TsBlockBuilder resultBuilder;

  protected MemoryReservationManager memoryReservationManager;

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
    this.operatorContext = operatorContext;
    this.leftChild = leftChild;
    this.leftTimeColumnPosition = leftTimeColumnPosition;
    this.leftOutputSymbolIdx = leftOutputSymbolIdx;
    this.rightChild = rightChild;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;
    this.rightTimeColumnPosition = rightTimeColumnPosition;

    this.comparator = timeComparator;
    this.resultBuilder = new TsBlockBuilder(dataTypes);

    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> leftBlocked = leftChild.isBlocked();
    ListenableFuture<?> rightBlocked = rightChild.isBlocked();
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
    if (retainedTsBlock != null) {
      return false;
    }

    return !leftBlockNotEmpty()
        && leftChild.isFinished()
        && !rightBlockNotEmpty()
        && rightChild.isFinished();
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
        for (int i = 1; i < rightBlockList.size(); i++) {
          memoryReservationManager.releaseMemoryCumulatively(
              rightBlockList.get(i).getRetainedSizeInBytes());
        }
        rightBlockList.clear();
        rightBlockListIdx = 0;
        rightIndex = 0;
        resultTsBlock = buildResultTsBlock(resultBuilder);
        return checkTsBlockSizeAndGetResult();
      } else {
        appendLeftWithEmptyRight();
        leftBlock = null;
        leftIndex = 0;
        resultTsBlock = buildResultTsBlock(resultBuilder);
        return checkTsBlockSizeAndGetResult();
      }
    }

    // all the rightTsBlock is less than leftTsBlock, append right with empty left
    if (comparator.lessThan(getRightEndTime(), getCurrentLeftTime())) {
      appendRightWithEmptyLeft();
      for (int i = 1; i < rightBlockList.size(); i++) {
        memoryReservationManager.releaseMemoryCumulatively(
            rightBlockList.get(i).getRetainedSizeInBytes());
      }
      rightBlockList.clear();
      rightBlockListIdx = 0;
      rightIndex = 0;
      return null;
    }

    // all the leftTsBlock is less than rightTsBlock, append left with empty right
    else if (comparator.lessThan(getLeftEndTime(), getCurrentRightTime())) {
      appendLeftWithEmptyRight();
      leftBlock = null;
      leftIndex = 0;
      return null;
    }

    long leftProbeTime = getCurrentLeftTime();
    while (!resultBuilder.isFull()) {

      // all right block time is not matched
      if (!comparator.canContinueInclusive(leftProbeTime, getRightEndTime())) {
        appendRightWithEmptyLeft();
        for (int i = 1; i < rightBlockList.size(); i++) {
          memoryReservationManager.releaseMemoryCumulatively(
              rightBlockList.get(i).getRetainedSizeInBytes());
        }
        rightBlockList.clear();
        rightBlockListIdx = 0;
        rightIndex = 0;
        break;
      }

      appendResult(leftProbeTime);

      leftIndex++;

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

  private boolean prepareInput(long start, long maxRuntime) throws Exception {

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

  private void tryCachedNextRightTsBlock() throws Exception {
    if (rightChild.hasNextWithTimer()) {
      TsBlock block = rightChild.nextWithTimer();
      if (block != null) {
        if (block.getColumn(rightTimeColumnPosition).getLong(0) == getRightEndTime()) {
          memoryReservationManager.reserveMemoryCumulatively(block.getRetainedSizeInBytes());
          rightBlockList.add(block);
        } else {
          hasCachedNextRightBlock = true;
          cachedNextRightBlock = block;
        }
      }
    } else {
      hasCachedNextRightBlock = true;
      cachedNextRightBlock = null;
    }
  }

  private long getCurrentLeftTime() {
    return leftBlock.getColumn(leftTimeColumnPosition).getLong(leftIndex);
  }

  private long getLeftEndTime() {
    return leftBlock.getColumn(leftTimeColumnPosition).getLong(leftBlock.getPositionCount() - 1);
  }

  private long getCurrentRightTime() {
    return rightBlockList
        .get(rightBlockListIdx)
        .getColumn(rightTimeColumnPosition)
        .getLong(rightIndex);
  }

  private long getRightTime(int idx1, int idx2) {
    return rightBlockList.get(idx1).getColumn(rightTimeColumnPosition).getLong(idx2);
  }

  private long getRightEndTime() {
    TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
    return lastRightTsBlock
        .getColumn(rightTimeColumnPosition)
        .getLong(lastRightTsBlock.getPositionCount() - 1);
  }

  private void appendResult(long leftTime) {

    while (comparator.lessThan(getCurrentRightTime(), leftTime)) {
      if (getCurrentRightTime() > lastMatchedRightTime) {
        appendOneRightRowWithEmptyLeft();
      }

      rightIndex++;

      if (rightIndex >= rightBlockList.get(rightBlockListIdx).getPositionCount()) {
        rightBlockListIdx++;
        rightIndex = 0;
      }

      if (rightBlockListIdx >= rightBlockList.size()) {
        rightBlockListIdx = 0;
        rightIndex = 0;
        return;
      }
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
  }

  private void appendLeftWithEmptyRight() {
    while (leftIndex < leftBlock.getPositionCount()) {
      for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
        ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
        if (leftBlock.getColumn(leftOutputSymbolIdx[i]).isNull(leftIndex)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(leftBlock.getColumn(leftOutputSymbolIdx[i]), leftIndex);
        }
      }

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

        for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
          ColumnBuilder columnBuilder =
              resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);

          if (rightBlockList
              .get(rightBlockListIdx)
              .getColumn(rightOutputSymbolIdx[i])
              .isNull(rightIndex)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(
                rightBlockList.get(rightBlockListIdx).getColumn(rightOutputSymbolIdx[i]),
                rightIndex);
          }
        }

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

    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);

      if (rightBlockList
          .get(rightBlockListIdx)
          .getColumn(rightOutputSymbolIdx[i])
          .isNull(rightIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(
            rightBlockList.get(rightBlockListIdx).getColumn(rightOutputSymbolIdx[i]), rightIndex);
      }
    }

    resultBuilder.declarePosition();
  }

  private boolean leftBlockNotEmpty() {
    return leftBlock != null && leftIndex < leftBlock.getPositionCount();
  }

  private boolean rightBlockNotEmpty() {
    return !rightBlockList.isEmpty()
        && rightBlockListIdx < rightBlockList.size()
        && rightIndex < rightBlockList.get(rightBlockListIdx).getPositionCount();
  }

  private void appendValueToResult(int tmpRightBlockListIdx, int tmpRightIndex) {
    for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      if (leftBlock.getColumn(leftOutputSymbolIdx[i]).isNull(leftIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(leftBlock.getColumn(leftOutputSymbolIdx[i]), leftIndex);
      }
    }

    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);

      if (rightBlockList
          .get(tmpRightBlockListIdx)
          .getColumn(rightOutputSymbolIdx[i])
          .isNull(tmpRightIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(
            rightBlockList.get(tmpRightBlockListIdx).getColumn(rightOutputSymbolIdx[i]),
            tmpRightIndex);
      }
    }
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
        memoryReservationManager.reserveMemoryCumulatively(block.getRetainedSizeInBytes());
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
