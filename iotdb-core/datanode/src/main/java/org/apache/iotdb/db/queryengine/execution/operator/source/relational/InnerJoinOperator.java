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

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class InnerJoinOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final Operator leftChild;
  private TsBlock leftTsBlock;
  private int leftIndex; // start index of leftTsBlock
  private final int leftTimeColumnPosition;
  private final int[] leftOutputSymbolIdx;

  private final Operator rightChild;
  private TsBlock rightTsBlock;
  private int rightIndex; // start index of rightTsBlock
  private final int rightTimeColumnPosition;
  private final int[] rightOutputSymbolIdx;

  private final TimeComparator comparator;
  private final TsBlockBuilder resultBuilder;

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  public InnerJoinOperator(
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
  public boolean hasNext() throws Exception {
    return (tsBlockIsNotEmpty(leftTsBlock, leftIndex) || leftChild.hasNextWithTimer())
        && (tsBlockIsNotEmpty(rightTsBlock, rightIndex) || rightChild.hasNextWithTimer());
  }

  @Override
  public TsBlock next() throws Exception {
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    if (!prepareInput(start, maxRuntime)) {
      return null;
    }

    // all the rightTsBlock is less than leftTsBlock, just skip it
    if (comparator.largerThan(getCurrentLeftTime(), getRightEndTime())) {
      // clean rightTsBlock
      rightTsBlock = null;
      rightIndex = 0;
      return null;
    }

    // all the leftTsBlock is less than rightTsBlock, just skip it
    else if (comparator.largerThan(getCurrentRightTime(), getLeftEndTime())) {
      // clean rightTsBlock
      leftTsBlock = null;
      leftIndex = 0;
      return null;
    }

    long leftProbeTime = getCurrentLeftTime();
    long currentEndTime = comparator.getCurrentEndTime(getLeftEndTime(), getRightEndTime());
    while (!resultBuilder.isFull()
        && comparator.canContinueInclusive(leftProbeTime, currentEndTime)) {
      appendTableRows(leftProbeTime);
      leftIndex++;

      // all left tsblock has been consumed
      if (leftIndex >= leftTsBlock.getPositionCount()) {
        leftTsBlock = null;
        leftIndex = 0;
        break;
      }

      leftProbeTime = getCurrentLeftTime();
    }

    // TODO if will return empty tsblock?

    Column[] valueColumns = new Column[resultBuilder.getValueColumnBuilders().length];

    int declaredPositions = resultBuilder.getPositionCount();
    for (int i = 0; i < valueColumns.length; ++i) {
      valueColumns[i] = resultBuilder.getValueColumnBuilders()[i].build();
      if (valueColumns[i].getPositionCount() != declaredPositions) {
        throw new IllegalStateException(
            String.format(
                "Declared positions (%s) does not match column %s's number of entries (%s)",
                declaredPositions, i, valueColumns[i].getPositionCount()));
      }
    }

    TsBlock result =
        TsBlock.wrapBlocksWithoutCopy(
            this.resultBuilder.getPositionCount(),
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, this.resultBuilder.getPositionCount()),
            valueColumns);
    resultBuilder.reset();
    return result;
  }

  private long getCurrentLeftTime() {
    return leftTsBlock.getColumn(leftTimeColumnPosition).getLong(leftIndex);
  }

  private long getCurrentRightTime() {
    return rightTsBlock.getColumn(rightTimeColumnPosition).getLong(rightIndex);
  }

  private long getRightTime(int idx) {
    return rightTsBlock.getColumn(rightTimeColumnPosition).getLong(idx);
  }

  private long getLeftEndTime() {
    return leftTsBlock
        .getColumn(leftTimeColumnPosition)
        .getLong(leftTsBlock.getPositionCount() - 1);
  }

  private long getRightEndTime() {
    return rightTsBlock
        .getColumn(rightTimeColumnPosition)
        .getLong(rightTsBlock.getPositionCount() - 1);
  }

  private void appendTableRows(long leftTime) {
    while (rightIndex < rightTsBlock.getPositionCount()
        && comparator.lessThan(getCurrentRightTime(), leftTime)) {
      rightIndex++;
    }

    if (rightIndex >= rightTsBlock.getPositionCount()) {
      rightTsBlock = null;
      rightIndex = 0;
      return;
    }

    int idx = rightIndex;
    while (idx < rightTsBlock.getPositionCount() && leftTime == getRightTime(idx)) {

      for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
        ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
        if (leftTsBlock.getColumn(leftOutputSymbolIdx[i]).isNull(leftIndex)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(leftTsBlock.getColumn(leftOutputSymbolIdx[i]), leftIndex);
        }
      }

      for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
        ColumnBuilder columnBuilder =
            resultBuilder.getColumnBuilder(leftOutputSymbolIdx.length + i);

        if (rightTsBlock.getColumn(rightOutputSymbolIdx[i]).isNull(idx)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(rightTsBlock.getColumn(rightOutputSymbolIdx[i]), idx);
        }
      }

      resultBuilder.declarePosition();
      idx++;
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
  }

  @Override
  public boolean isFinished() throws Exception {
    return !tsBlockIsNotEmpty(leftTsBlock, leftIndex)
        && leftChild.isFinished()
        && !tsBlockIsNotEmpty(rightTsBlock, rightIndex)
        && rightChild.isFinished();
  }

  private boolean prepareInput(long start, long maxRuntime) throws Exception {
    if ((leftTsBlock == null || leftTsBlock.getPositionCount() == leftIndex)
        && leftChild.hasNextWithTimer()) {
      leftTsBlock = leftChild.nextWithTimer();
      leftIndex = 0;
    }

    if ((System.nanoTime() - start < maxRuntime)
        && (rightTsBlock == null || rightTsBlock.getPositionCount() == rightIndex)) {
      if (rightChild.hasNextWithTimer()) {
        rightTsBlock = rightChild.nextWithTimer();
        rightIndex = 0;
        if (rightTsBlock != null) {
          System.out.println("===");
        }
      }
    }
    return tsBlockIsNotEmpty(leftTsBlock, leftIndex) && tsBlockIsNotEmpty(rightTsBlock, rightIndex);
  }

  private boolean tsBlockIsNotEmpty(TsBlock tsBlock, int index) {
    return tsBlock != null && index < tsBlock.getPositionCount();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
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
        + rightChild.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
