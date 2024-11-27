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
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableInnerJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableInnerJoinOperator.class);

  private final Operator leftChild;
  private TsBlock leftBlock;
  private int leftIndex; // start index of leftTsBlock
  private final int leftJoinKeyPosition;
  private final int[] leftOutputSymbolIdx;

  private final Operator rightChild;
  private final List<TsBlock> rightBlockList = new ArrayList<>();
  private final int rightJoinKeyPosition;
  private int rightBlockListIdx;
  private int rightIndex; // start index of rightTsBlock
  private final int[] rightOutputSymbolIdx;
  private TsBlock cachedNextRightBlock;
  private boolean hasCachedNextRightBlock;

  private final JoinKeyComparator comparator;
  private final TsBlockBuilder resultBuilder;

  private final Type joinKeyType;

  protected MemoryReservationManager memoryReservationManager;

  public TableInnerJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int leftJoinKeyPosition,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int rightJoinKeyPosition,
      int[] rightOutputSymbolIdx,
      JoinKeyComparator timeComparator,
      List<TSDataType> dataTypes,
      Type joinKeyType) {
    this.operatorContext = operatorContext;
    this.leftChild = leftChild;
    this.leftJoinKeyPosition = leftJoinKeyPosition;
    this.leftOutputSymbolIdx = leftOutputSymbolIdx;
    this.rightChild = rightChild;
    this.rightJoinKeyPosition = rightJoinKeyPosition;
    this.rightOutputSymbolIdx = rightOutputSymbolIdx;

    this.comparator = timeComparator;
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
    if (allRightLessThanLeft()) {
      clearRightBlockList();
      return null;
    }

    // all the leftTsBlock is less than rightTsBlock, just skip left
    else if (allLeftLessThanRight()) {
      leftBlock = null;
      leftIndex = 0;
      return null;
    }

    mainJoinLoop();

    if (resultBuilder.isEmpty()) {
      return null;
    }

    resultTsBlock = buildResultTsBlock(resultBuilder);
    return checkTsBlockSizeAndGetResult();
  }

  private boolean prepareInput(long start, long maxRuntime) throws Exception {
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

  private void tryCachedNextRightTsBlock() throws Exception {
    if (rightChild.hasNextWithTimer()) {
      TsBlock block = rightChild.nextWithTimer();
      if (block != null) {
        if (equalsTo(
            block,
            rightJoinKeyPosition,
            0,
            rightBlockList.get(rightBlockList.size() - 1),
            rightJoinKeyPosition,
            rightBlockList.get(rightBlockList.size() - 1).getPositionCount() - 1)) {
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

  private void mainJoinLoop() {
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
      case DATE:
        intJoinLoop();
        break;
      case INT64:
      case TIMESTAMP:
        longJoinLoop();
        break;
      case FLOAT:
        floatJoinLoop();
        break;
      case DOUBLE:
        doubleJoinLoop();
        break;
      case STRING:
        binaryJoinLoop();
        break;
      case BLOB:
      case TEXT:
      case BOOLEAN:
      default:
        throw new IllegalArgumentException("Unsupported dataType: " + joinKeyType.getTypeEnum());
    }
  }

  private boolean allRightLessThanLeft() {
    // check if the last value of the right is less than left
    return lessThan(
        rightBlockList.get(rightBlockList.size() - 1),
        rightJoinKeyPosition,
        rightBlockList.get(rightBlockList.size() - 1).getPositionCount() - 1,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex);
  }

  private boolean allLeftLessThanRight() {
    return lessThan(
        leftBlock,
        leftJoinKeyPosition,
        leftBlock.getPositionCount() - 1,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex);
  }

  /* Get value from the two blocks, and returns comparator.lessThan(firstValue, secondValue). */
  private boolean lessThan(
      TsBlock firstBlock,
      int firstColumnIndex,
      int firstRowIndex,
      TsBlock secondBlock,
      int secondColumnIndex,
      int secondRowIndex) {
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
      case DATE:
        return comparator.lessThan(
            getIntFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getIntFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case INT64:
      case TIMESTAMP:
        return comparator.lessThan(
            getLongFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getLongFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case FLOAT:
        return comparator.lessThan(
            getFloatFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getFloatFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case DOUBLE:
        return comparator.lessThan(
            getDoubleFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getDoubleFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case STRING:
        return comparator.lessThan(
            getBinaryFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getBinaryFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case BLOB:
      case TEXT:
      case BOOLEAN:
      default:
        throw new IllegalArgumentException("Unsupported type: " + joinKeyType.getTypeEnum());
    }
  }

  /* Get value from the two blocks, and returns comparator.equalsTo(firstValue, secondValue). */
  private boolean equalsTo(
      TsBlock firstBlock,
      int firstColumnIndex,
      int firstRowIndex,
      TsBlock secondBlock,
      int secondColumnIndex,
      int secondRowIndex) {
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
      case DATE:
        return comparator.equalsTo(
            getIntFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getIntFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case INT64:
      case TIMESTAMP:
        return comparator.equalsTo(
            getLongFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getLongFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case FLOAT:
        return comparator.equalsTo(
            getFloatFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getFloatFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case DOUBLE:
        return comparator.equalsTo(
            getDoubleFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getDoubleFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case STRING:
        return comparator.equalsTo(
            getBinaryFromBlock(firstBlock, firstColumnIndex, firstRowIndex),
            getBinaryFromBlock(secondBlock, secondColumnIndex, secondRowIndex));
      case BLOB:
      case TEXT:
      case BOOLEAN:
      default:
        throw new IllegalArgumentException("Unsupported type: " + joinKeyType.getTypeEnum());
    }
  }

  private void clearRightBlockList() {
    for (int i = 1; i < rightBlockList.size(); i++) {
      memoryReservationManager.releaseMemoryCumulatively(
          rightBlockList.get(i).getRetainedSizeInBytes());
    }
    rightBlockList.clear();
    rightBlockListIdx = 0;
    rightIndex = 0;
  }

  private boolean leftBlockNotEmpty() {
    return leftBlock != null && leftIndex < leftBlock.getPositionCount();
  }

  private boolean rightBlockNotEmpty() {
    return (!rightBlockList.isEmpty()
            && rightBlockListIdx < rightBlockList.size()
            && rightIndex < rightBlockList.get(rightBlockListIdx).getPositionCount())
        || (hasCachedNextRightBlock && cachedNextRightBlock != null);
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

  // region helper function for each datatype

  private int getIntFromBlock(TsBlock block, int columnIndex, int rowIndex) {
    return block.getColumn(columnIndex).getInt(rowIndex);
  }

  private long getLongFromBlock(TsBlock block, int columnIndex, int rowIndex) {
    return block.getColumn(columnIndex).getLong(rowIndex);
  }

  private float getFloatFromBlock(TsBlock block, int columnIndex, int rowIndex) {
    return block.getColumn(columnIndex).getFloat(rowIndex);
  }

  private double getDoubleFromBlock(TsBlock block, int columnIndex, int rowIndex) {
    return block.getColumn(columnIndex).getDouble(rowIndex);
  }

  private Binary getBinaryFromBlock(TsBlock block, int columnIndex, int rowIndex) {
    return block.getColumn(columnIndex).getBinary(rowIndex);
  }

  private void intJoinLoop() {
    while (!resultBuilder.isFull()) {
      int leftJoinKey = leftBlock.getColumn(leftJoinKeyPosition).getInt(leftIndex);
      TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
      int rightJoinKey =
          lastRightTsBlock
              .getColumn(rightJoinKeyPosition)
              .getInt(lastRightTsBlock.getPositionCount() - 1);
      // all right block value is not matched
      if (!comparator.lessThanOrEqual(leftJoinKey, rightJoinKey)) {
        clearRightBlockList();
        break;
      }

      appendIntResult(leftJoinKey);

      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
    }
  }

  private void longJoinLoop() {
    while (!resultBuilder.isFull()) {
      long leftJoinKey = leftBlock.getColumn(leftJoinKeyPosition).getLong(leftIndex);
      TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
      long rightJoinKey =
          lastRightTsBlock
              .getColumn(rightJoinKeyPosition)
              .getLong(lastRightTsBlock.getPositionCount() - 1);
      // all right block value is not matched
      if (!comparator.lessThanOrEqual(leftJoinKey, rightJoinKey)) {
        clearRightBlockList();
        break;
      }

      appendLongResult(leftJoinKey);

      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
    }
  }

  private void floatJoinLoop() {
    while (!resultBuilder.isFull()) {
      float leftJoinKey = leftBlock.getColumn(leftJoinKeyPosition).getFloat(leftIndex);
      TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
      float rightJoinKey =
          lastRightTsBlock
              .getColumn(rightJoinKeyPosition)
              .getFloat(lastRightTsBlock.getPositionCount() - 1);
      // all right block value is not matched
      if (!comparator.lessThanOrEqual(leftJoinKey, rightJoinKey)) {
        clearRightBlockList();
        break;
      }

      appendFloatResult(leftJoinKey);

      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
    }
  }

  private void doubleJoinLoop() {
    while (!resultBuilder.isFull()) {
      double leftJoinKey = leftBlock.getColumn(leftJoinKeyPosition).getDouble(leftIndex);
      TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
      double rightJoinKey =
          lastRightTsBlock
              .getColumn(rightJoinKeyPosition)
              .getDouble(lastRightTsBlock.getPositionCount() - 1);
      // all right block value is not matched
      if (!comparator.lessThanOrEqual(leftJoinKey, rightJoinKey)) {
        clearRightBlockList();
        break;
      }

      appendDoubleResult(leftJoinKey);

      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
    }
  }

  private void binaryJoinLoop() {
    while (!resultBuilder.isFull()) {
      Binary leftJoinKey = leftBlock.getColumn(leftJoinKeyPosition).getBinary(leftIndex);
      TsBlock lastRightTsBlock = rightBlockList.get(rightBlockList.size() - 1);
      Binary rightJoinKey =
          lastRightTsBlock
              .getColumn(rightJoinKeyPosition)
              .getBinary(lastRightTsBlock.getPositionCount() - 1);
      // all right block value is not matched
      if (!comparator.lessThanOrEqual(leftJoinKey, rightJoinKey)) {
        clearRightBlockList();
        break;
      }

      appendBinaryResult(leftJoinKey);

      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        leftBlock = null;
        leftIndex = 0;
        break;
      }
    }
  }

  private void appendIntResult(int leftJoinKey) {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx).getColumn(rightJoinKeyPosition).getInt(rightIndex),
        leftJoinKey)) {
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
    while (leftJoinKey
        == rightBlockList.get(tmpBlockIdx).getColumn(rightJoinKeyPosition).getInt(tmpIdx)) {
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

  private void appendLongResult(long leftJoinKey) {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx).getColumn(rightJoinKeyPosition).getLong(rightIndex),
        leftJoinKey)) {
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
    while (leftJoinKey
        == rightBlockList.get(tmpBlockIdx).getColumn(rightJoinKeyPosition).getLong(tmpIdx)) {
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

  private void appendFloatResult(float leftJoinKey) {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx).getColumn(rightJoinKeyPosition).getFloat(rightIndex),
        leftJoinKey)) {
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
    while (leftJoinKey
        == rightBlockList.get(tmpBlockIdx).getColumn(rightJoinKeyPosition).getFloat(tmpIdx)) {
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

  private void appendDoubleResult(double leftJoinKey) {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx).getColumn(rightJoinKeyPosition).getDouble(rightIndex),
        leftJoinKey)) {
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
    while (leftJoinKey
        == rightBlockList.get(tmpBlockIdx).getColumn(rightJoinKeyPosition).getDouble(tmpIdx)) {
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

  private void appendBinaryResult(Binary leftJoinKey) {

    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx).getColumn(rightJoinKeyPosition).getBinary(rightIndex),
        leftJoinKey)) {
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
    while (comparator.equalsTo(
        leftJoinKey,
        rightBlockList.get(tmpBlockIdx).getColumn(rightJoinKeyPosition).getBinary(tmpIdx))) {
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

  // end region
}
