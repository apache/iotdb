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
package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/** Used for linear fill */
public class LinearFillOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final ILinearFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;
  // TODO need to spill it to disk if it consumes too much memory
  private final List<TsBlock> cachedTsBlock;

  private final List<Long> cachedRowIndex;

  private long currentRowIndex = 0;
  // next TsBlock Index for each Column
  private final int[] nextTsBlockIndex;

  // indicate whether we can call child.next()
  // it's used to make sure that child.next() will only be called once in LinearFillOperator.next();
  private boolean canCallNext;
  // indicate whether there is more TsBlock for child operator
  private boolean noMoreTsBlock;

  public LinearFillOperator(
      OperatorContext operatorContext, ILinearFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    this.cachedTsBlock = new ArrayList<>();
    this.cachedRowIndex = new ArrayList<>();
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

  @Override
  public TsBlock next() {

    // make sure we call child.next() at most once
    if (cachedTsBlock.isEmpty()) {
      canCallNext = false;
      TsBlock nextTsBlock = child.nextWithTimer();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        return nextTsBlock;
      } else { // otherwise, we cache it
        cachedTsBlock.add(nextTsBlock);
        cachedRowIndex.add(currentRowIndex);
        currentRowIndex += nextTsBlock.getPositionCount();
      }
    }

    TsBlock originTsBlock = cachedTsBlock.get(0);
    long currentEndRowIndex = cachedRowIndex.get(0) + originTsBlock.getPositionCount() - 1;
    // Step 1: judge whether we can fill current TsBlock, if TsBlock that we can get is not enough,
    // we just return null
    for (int columnIndex = 0; columnIndex < outputColumnCount; columnIndex++) {
      // current valueColumn can't be filled using current information
      if (fillArray[columnIndex].needPrepareForNext(
          currentEndRowIndex, originTsBlock.getColumn(columnIndex))) {
        // current cached TsBlock is not enough to fill this column
        while (!isCachedTsBlockEnough(columnIndex, currentEndRowIndex)) {
          // if we failed to get next TsBlock
          if (!tryToGetNextTsBlock()) {
            // there is no more TsBlock, so we have to fill this Column
            if (noMoreTsBlock) {
              // break the while-loop, continue to judge next Column
              break;
            } else {
              // there is still more TsBlock, so current calculation is not finished, and we just
              // return null
              return null;
            }
          }
        }
      }
    }
    // Step 2: fill current TsBlock
    originTsBlock = cachedTsBlock.remove(0);
    long startRowIndex = cachedRowIndex.remove(0);
    Column[] columns = new Column[outputColumnCount];
    for (int i = 0; i < outputColumnCount; i++) {
      columns[i] =
          fillArray[i].fill(
              originTsBlock.getTimeColumn(), originTsBlock.getColumn(i), startRowIndex);
    }
    TsBlock result =
        new TsBlock(originTsBlock.getPositionCount(), originTsBlock.getTimeColumn(), columns);
    for (int i = 0; i < outputColumnCount; i++) {
      // make sure nextTsBlockIndex for each column >= 1
      nextTsBlockIndex[i] = Math.max(1, nextTsBlockIndex[i] - 1);
    }
    return result;
  }

  @Override
  public boolean hasNext() {
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
  public boolean isFinished() {
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

  /**
   * Judge whether we can use current cached TsBlock to fill Column
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
          nextTsBlock.getTimeColumn(),
          nextTsBlock.getColumn(columnIndex))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if we succeed to get next TsBlock and add it into cachedTsBlock, otherwise false
   */
  private boolean tryToGetNextTsBlock() {
    if (canCallNext) { // if we can call child.next(), we call that and cache it in
      // cachedTsBlock
      canCallNext = false;
      TsBlock nextTsBlock = child.nextWithTimer();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        return false;
      } else { // otherwise, we cache it
        cachedTsBlock.add(nextTsBlock);
        cachedRowIndex.add(currentRowIndex);
        currentRowIndex += nextTsBlock.getPositionCount();
        return true;
      }
    }
    return false;
  }
}
