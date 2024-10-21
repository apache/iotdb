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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

abstract class AbstractLinearFillOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractLinearFillOperator.class);
  private final OperatorContext operatorContext;
  protected final ILinearFill[] fillArray;
  private final Operator child;
  protected final int outputColumnCount;
  private final List<TsBlock> cachedTsBlock;

  private final List<Long> cachedRowIndex;

  private final List<Integer> cachedLastRowIndexForNonNullHelperColumn;

  private long currentRowIndex = 0;
  // next TsBlock Index for each Column
  private final int[] nextTsBlockIndex;

  /**
   * indicate whether we can call child.next(). it's used to make sure that child.next() will only
   * be called once in AbstractLinearFillOperator.next().
   */
  private boolean canCallNext;

  // indicate whether there is more TsBlock for child operator
  protected boolean noMoreTsBlock;

  AbstractLinearFillOperator(
      OperatorContext operatorContext, ILinearFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    this.cachedTsBlock = new ArrayList<>();
    this.cachedRowIndex = new ArrayList<>();
    this.cachedLastRowIndexForNonNullHelperColumn = new ArrayList<>();
    this.nextTsBlockIndex = new int[outputColumnCount];
    Arrays.fill(this.nextTsBlockIndex, 1);
    this.canCallNext = false;
    this.noMoreTsBlock = true;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @SuppressWarnings("squid:S3776")
  @Override
  public TsBlock next() throws Exception {

    // make sure we call child.next() at most once
    if (cachedTsBlock.isEmpty() && !tryToGetNextTsBlock()) {
      return null;
    }

    TsBlock tempResult = null;
    while (tempResult == null && !cachedTsBlock.isEmpty()) {
      TsBlock originTsBlock = cachedTsBlock.get(0);
      long currentEndRowIndex =
          cachedRowIndex.get(0) + cachedLastRowIndexForNonNullHelperColumn.get(0);
      // Step 1: judge whether we can fill current TsBlock, if TsBlock that we can get is not
      // enough,
      // we just return null
      for (int columnIndex = 0; columnIndex < outputColumnCount; columnIndex++) {
        // current valueColumn can't be filled using current information
        if (fillArray[columnIndex].needPrepareForNext(
            currentEndRowIndex,
            originTsBlock.getColumn(columnIndex),
            cachedLastRowIndexForNonNullHelperColumn.get(0))) {
          // current cached TsBlock is not enough to fill this column
          while (!isCachedTsBlockEnough(columnIndex, currentEndRowIndex)) {
            // if we failed to get next TsBlock
            if (!tryToGetNextTsBlock()) {
              // there is no more TsBlock for current group, so we have to fill this Column
              if (noMoreTsBlockForCurrentGroup()) {
                // break the while-loop, continue to judge next Column
                break;
              } else {
                // there is still more TsBlock, so current calculation is not finished, and we just
                // return null
                return buildFinalResult(tempResult);
              }
            }
          }
        }
      }
      // Step 2: fill current TsBlock
      originTsBlock = cachedTsBlock.remove(0);
      long startRowIndex = cachedRowIndex.remove(0);
      cachedLastRowIndexForNonNullHelperColumn.remove(0);
      resetFill();

      Column[] columns = new Column[outputColumnCount];
      for (int i = 0; i < outputColumnCount; i++) {
        columns[i] =
            fillArray[i].fill(
                getHelperColumn(originTsBlock), originTsBlock.getColumn(i), startRowIndex);
      }
      tempResult = append(originTsBlock.getPositionCount(), originTsBlock.getTimeColumn(), columns);
      for (int i = 0; i < outputColumnCount; i++) {
        // make sure nextTsBlockIndex for each column >= 1
        nextTsBlockIndex[i] = Math.max(1, nextTsBlockIndex[i] - 1);
      }
    }
    return buildFinalResult(tempResult);
  }

  boolean noMoreTsBlockForCurrentGroup() {
    return noMoreTsBlock;
  }

  abstract Column getHelperColumn(TsBlock tsBlock);

  // -1 means all values of helper column in @param{tsBlock} are null
  abstract Integer getLastRowIndexForNonNullHelperColumn(TsBlock tsBlock);

  TsBlock append(int length, Column timeColumn, Column[] valueColumns) {
    return new TsBlock(length, timeColumn, valueColumns);
  }

  TsBlock buildFinalResult(TsBlock tempResult) {
    return tempResult;
  }

  void resetFill() {
    // do nothing
  }

  @Override
  public boolean hasNext() throws Exception {
    // if child.hasNext() return false, it means that there is no more tsBlocks
    noMoreTsBlock = !child.hasNextWithTimer();
    // if there is more tsBlock, we can call child.next() once
    canCallNext = !noMoreTsBlock;
    return !cachedTsBlock.isEmpty() || !noMoreTsBlock;
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return cachedTsBlock.isEmpty() && child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    // while doing linear fill, we may need to copy the corresponding column if there exists null
    // values, and we may also need to cache next TsBlock to get next not null value
    // so the max peek memory may be triple or more, here we just use 3 as the estimated factor
    // because in most cases, we will get next not null value in next TsBlock
    return 3 * child.calculateMaxPeekMemory() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    // we can safely ignore two lines cached in LinearFill
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(nextTsBlockIndex);
  }

  /**
   * Judge whether we can use current cached TsBlock to fill Column.
   *
   * @param columnIndex index for column which need to be filled
   * @param currentEndRowIndex row index for endTime of column which need to be filled
   * @return true if current cached TsBlock is enough to fill Column at columnIndex, otherwise
   *     false.
   */
  private boolean isCachedTsBlockEnough(int columnIndex, long currentEndRowIndex) {
    // next TsBlock has already been in the cachedTsBlock
    while (nextTsBlockIndex[columnIndex] < cachedTsBlock.size()) {
      TsBlock nextTsBlock = cachedTsBlock.get(nextTsBlockIndex[columnIndex]);
      long startRowIndex = cachedRowIndex.get(nextTsBlockIndex[columnIndex]);
      nextTsBlockIndex[columnIndex]++;
      if (fillArray[columnIndex].prepareForNext(
          startRowIndex,
          currentEndRowIndex,
          getHelperColumn(nextTsBlock),
          nextTsBlock.getColumn(columnIndex))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Try to get next TsBlock
   *
   * @return true if we succeed to get next TsBlock and add it into cachedTsBlock, otherwise false
   * @throws Exception errors happened while getting next batch data
   */
  private boolean tryToGetNextTsBlock() throws Exception {
    if (canCallNext) { // if we can call child.next(), we call that and cache it in
      // cachedTsBlock
      canCallNext = false;
      TsBlock nextTsBlock = child.nextWithTimer();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        return false;
      } else { // otherwise, we cache it
        updateCachedData(nextTsBlock);
        return true;
      }
    }
    return false;
  }

  void updateCachedData(TsBlock tsBlock) {
    cachedTsBlock.add(tsBlock);
    cachedRowIndex.add(currentRowIndex);
    cachedLastRowIndexForNonNullHelperColumn.add(getLastRowIndexForNonNullHelperColumn(tsBlock));
    currentRowIndex += tsBlock.getPositionCount();
  }
}
