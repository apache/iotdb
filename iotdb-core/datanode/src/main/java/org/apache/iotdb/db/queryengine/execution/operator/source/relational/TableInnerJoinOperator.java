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
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_RESERVED_MEMORY;

public class TableInnerJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableInnerJoinOperator.class);

  protected final Operator leftChild;
  protected TsBlock leftBlock;
  protected int leftIndex; // start index of leftTsBlock
  protected final int leftTimeColumnPosition;
  protected final int[] leftOutputSymbolIdx;

  protected final Operator rightChild;
  protected final List<TsBlock> rightBlockList = new ArrayList<>();
  protected final int rightTimeColumnPosition;
  protected int rightBlockListIdx;
  protected int rightIndex; // start index of rightTsBlock
  protected final int[] rightOutputSymbolIdx;
  protected TsBlock cachedNextRightBlock;
  protected boolean hasCachedNextRightBlock;

  protected final TimeComparator comparator;
  protected final TsBlockBuilder resultBuilder;

  protected MemoryReservationManager memoryReservationManager;

  protected long maxUsedMemory;
  protected long usedMemory;

  public TableInnerJoinOperator(
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
    this.rightTimeColumnPosition = rightTimeColumnPosition;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;

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
      return true;
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
        && (rightBlockNotEmpty() || rightChild.hasNextWithTimer());
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

    // all the rightTsBlock is less than leftTsBlock, just skip right
    if (comparator.lessThan(getRightEndTime(), getCurrentLeftTime())) {
      // releaseMemory();
      resetRightBlockList();
      return null;
    }

    // all the leftTsBlock is less than rightTsBlock, just skip left
    else if (comparator.lessThan(getLeftEndTime(), getRightTime(rightBlockListIdx, rightIndex))) {
      leftBlock = null;
      leftIndex = 0;
      return null;
    }

    long leftProbeTime = getCurrentLeftTime();
    while (!resultBuilder.isFull()) {

      // all right block time is not matched
      if (!comparator.canContinueInclusive(leftProbeTime, getRightEndTime())) {
        // releaseMemory();
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

  protected boolean prepareInput(long start, long maxRuntime) throws Exception {
    if ((leftBlock == null || leftBlock.getPositionCount() == leftIndex)
        && leftChild.hasNextWithTimer()) {
      leftBlock = leftChild.nextWithTimer();
      leftIndex = 0;
    }

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
        hasCachedNextRightBlock = true;
        cachedNextRightBlock = null;
      }
    } else {
      if (!hasCachedNextRightBlock) {
        tryCachedNextRightTsBlock();
      }
    }

    return leftBlockNotEmpty() && rightBlockNotEmpty() && hasCachedNextRightBlock;
  }

  protected void tryCachedNextRightTsBlock() throws Exception {
    if (rightChild.hasNextWithTimer()) {
      TsBlock block = rightChild.nextWithTimer();
      if (block != null) {
        if (block.getColumn(rightTimeColumnPosition).getLong(0) == getRightEndTime()) {
          reserveMemory(block.getRetainedSizeInBytes());
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

  protected long getCurrentLeftTime() {
    return leftBlock.getColumn(leftTimeColumnPosition).getLong(leftIndex);
  }

  protected long getLeftEndTime() {
    return leftBlock.getColumn(leftTimeColumnPosition).getLong(leftBlock.getPositionCount() - 1);
  }

  protected long getRightTime(int blockIdx, int rowIdx) {
    return rightBlockList.get(blockIdx).getColumn(rightTimeColumnPosition).getLong(rowIdx);
  }

  protected long getCurrentRightTime() {
    return getRightTime(rightBlockListIdx, rightIndex);
  }

  protected long getRightEndTime() {
    TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
    return lastRightTsBlock
        .getColumn(rightTimeColumnPosition)
        .getLong(lastRightTsBlock.getPositionCount() - 1);
  }

  protected void appendResult(long leftTime) {

    while (comparator.lessThan(getCurrentRightTime(), leftTime)) {
      if (rightBlockFinish()) {
        return;
      }
    }

    int tmpBlockIdx = rightBlockListIdx, tmpIdx = rightIndex;
    while (leftTime == getRightTime(tmpBlockIdx, tmpIdx)) {
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

  /**
   * @return true if right block is consumed up
   */
  protected boolean rightBlockFinish() {
    rightIndex++;

    if (rightIndex >= rightBlockList.get(rightBlockListIdx).getPositionCount()) {
      rightBlockListIdx++;
      rightIndex = 0;
    }

    if (rightBlockListIdx >= rightBlockList.size()) {
      rightBlockListIdx = 0;
      rightIndex = 0;
      return true;
    }

    return false;
  }

  protected boolean leftBlockNotEmpty() {
    return leftBlock != null && leftIndex < leftBlock.getPositionCount();
  }

  protected boolean rightBlockNotEmpty() {
    return (!rightBlockList.isEmpty()
            && rightBlockListIdx < rightBlockList.size()
            && rightIndex < rightBlockList.get(rightBlockListIdx).getPositionCount())
        || (hasCachedNextRightBlock && cachedNextRightBlock != null);
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

  protected void resetRightBlockList() {
    for (int i = 1; i < rightBlockList.size(); i++) {
      long size = rightBlockList.get(i).getRetainedSizeInBytes();
      usedMemory -= size;
      memoryReservationManager.releaseMemoryCumulatively(size);
    }

    rightBlockList.clear();
    rightBlockListIdx = 0;
    rightIndex = 0;
  }

  protected void reserveMemory(long size) {
    usedMemory += size;
    memoryReservationManager.reserveMemoryCumulatively(size);
    if (usedMemory > maxUsedMemory) {
      maxUsedMemory = usedMemory;
      operatorContext.recordSpecifiedInfo(MAX_RESERVED_MEMORY, Long.toString(maxUsedMemory));
    }
  }

  public static TsBlock buildResultTsBlock(TsBlockBuilder resultBuilder) {
    Column[] valueColumns = new Column[resultBuilder.getValueColumnBuilders().length];
    for (int i = 0; i < valueColumns.length; ++i) {
      valueColumns[i] = resultBuilder.getValueColumnBuilders()[i].build();
      if (valueColumns[i].getPositionCount() != resultBuilder.getPositionCount()) {
        throw new IllegalStateException(
            String.format(
                "Declared positions (%s) does not match column %s's number of entries (%s)",
                resultBuilder.getPositionCount(), i, valueColumns[i].getPositionCount()));
      }
    }

    TsBlock result =
        TsBlock.wrapBlocksWithoutCopy(
            resultBuilder.getPositionCount(),
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()),
            valueColumns);
    resultBuilder.reset();
    return result;
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
