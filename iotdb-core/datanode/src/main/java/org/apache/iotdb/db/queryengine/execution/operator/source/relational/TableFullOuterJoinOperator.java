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
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableFullOuterJoinOperator extends TableInnerJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableFullOuterJoinOperator.class);

  private boolean leftFinished;
  private boolean rightFinished;

  private TsBlock lastMatchedRightBlock;

  private boolean lastMatchedRightBlockIsNull = true;

  public TableFullOuterJoinOperator(
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
    super(
        operatorContext,
        leftChild,
        leftJoinKeyPosition,
        leftOutputSymbolIdx,
        rightChild,
        rightJoinKeyPosition,
        rightOutputSymbolIdx,
        joinKeyComparator,
        dataTypes,
        joinKeyType);
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
    if (allRightLessThanLeft()) {
      appendRightWithEmptyLeft();
      resetRightBlockList();
      resultTsBlock = buildResultTsBlock(resultBuilder);
      return checkTsBlockSizeAndGetResult();
    }

    // all the leftTsBlock is less than rightTsBlock, append left with empty right
    else if (allLeftLessThanRight()) {
      appendLeftWithEmptyRight();
      leftBlock = null;
      leftIndex = 0;
      resultTsBlock = buildResultTsBlock(resultBuilder);
      return checkTsBlockSizeAndGetResult();
    }

    while (!resultBuilder.isFull()) {
      // all right block time is not matched
      TsBlock lastRightBlock = rightBlockList.get(rightBlockList.size() - 1);
      if (!comparator.lessThanOrEqual(
          leftBlock,
          leftJoinKeyPosition,
          leftIndex,
          lastRightBlock,
          rightJoinKeyPosition,
          lastRightBlock.getPositionCount() - 1)) {
        appendRightWithEmptyLeft();
        resetRightBlockList();
        break;
      }

      appendResult();

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
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

  protected void appendResult() {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex)) {
      // getCurrentRightTime() can only be greater than lastMatchedRightTime
      // if greater than, then put right
      // if equals, it has been put in last round
      // notice: must examine `comparator.lessThan(getCurrentRightTime(), leftTime)` then examine
      // `comparator.lessThan(leftTime, getCurrentRightTime())`
      if (lastMatchedRightBlockIsNull
          || comparator.lessThanOrEqual(
              lastMatchedRightBlock,
              0,
              0,
              rightBlockList.get(rightBlockListIdx),
              rightJoinKeyPosition,
              rightIndex)) {
        appendOneRightRowWithEmptyLeft();
      }

      if (rightBlockFinish()) {
        return;
      }
    }

    if (comparator.lessThan(
        leftBlock,
        leftJoinKeyPosition,
        leftIndex,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex)) {
      appendOneLeftRowWithEmptyRight();
      leftIndex++;
      return;
    }

    int tmpBlockIdx = rightBlockListIdx, tmpIdx = rightIndex;
    while (comparator.equalsTo(
        leftBlock,
        leftJoinKeyPosition,
        leftIndex,
        rightBlockList.get(tmpBlockIdx),
        rightJoinKeyPosition,
        tmpIdx)) {
      // lastMatchedRightBlockListIdx = rightBlockListIdx;
      // lastMatchedRightIdx = rightIndex;
      initLastMatchedRightBlock(leftBlock, leftJoinKeyPosition, leftIndex);
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

      if (lastMatchedRightBlockIsNull
          || comparator.lessThanOrEqual(
              lastMatchedRightBlock,
              0,
              0,
              rightBlockList.get(rightBlockListIdx),
              rightJoinKeyPosition,
              rightIndex)) {
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

  private void initLastMatchedRightBlock(TsBlock block, int columnIndex, int rowIndex) {
    lastMatchedRightBlockIsNull = false;
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
      case DATE:
        lastMatchedRightBlock =
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new IntColumn(
                    1,
                    Optional.empty(),
                    new int[] {block.getColumn(columnIndex).getInt(rowIndex)}));
        break;
      case INT64:
      case TIMESTAMP:
        lastMatchedRightBlock =
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new LongColumn(
                    1,
                    Optional.empty(),
                    new long[] {block.getColumn(columnIndex).getLong(rowIndex)}));
        break;
      case FLOAT:
        lastMatchedRightBlock =
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new FloatColumn(
                    1,
                    Optional.empty(),
                    new float[] {block.getColumn(columnIndex).getFloat(rowIndex)}));
        break;
      case DOUBLE:
        lastMatchedRightBlock =
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new DoubleColumn(
                    1,
                    Optional.empty(),
                    new double[] {block.getColumn(columnIndex).getDouble(rowIndex)}));
        break;
      case STRING:
      case TEXT:
      case BLOB:
        lastMatchedRightBlock =
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new BinaryColumn(
                    1,
                    Optional.empty(),
                    new Binary[] {block.getColumn(columnIndex).getBinary(rowIndex)}));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + joinKeyType);
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
