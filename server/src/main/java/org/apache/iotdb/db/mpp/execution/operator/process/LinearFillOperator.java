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
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.LinearFill;
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
  private final LinearFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;
  // TODO need to spill it to disk if it consumes too much memory
  private final List<TsBlock> cachedTsBlock;
  // next TsBlock Index for each Column
  private final int[] nextTsBlockIndex;
  // cache already filled column in last unfinished filling
  private final Column[] cachedFilledValueColumns;

  // indicate which column we are filling
  private int currentFilledColumnIndex;
  // indicate whether we can call child.next()
  // it's used to make sure that child.next() will only be called once in LinearFillOperator.next();
  private boolean canCallNext;
  // indicate whether there is more TsBlock for child operator
  private boolean noMoreTsBlock;

  public LinearFillOperator(
      OperatorContext operatorContext, LinearFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    this.cachedTsBlock = new ArrayList<>();
    this.nextTsBlockIndex = new int[outputColumnCount];
    Arrays.fill(this.nextTsBlockIndex, 1);
    this.cachedFilledValueColumns = new Column[outputColumnCount];
    this.currentFilledColumnIndex = 0;
    this.canCallNext = false;
    this.noMoreTsBlock = true;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {

    // make sure we call child.next() at most once
    if (cachedTsBlock.isEmpty()) {
      canCallNext = false;
      TsBlock nextTsBlock = child.next();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        return nextTsBlock;
      } else { // otherwise, we cache it
        cachedTsBlock.add(nextTsBlock);
      }
    }

    TsBlock block = cachedTsBlock.get(0);
    long currentEndTime = block.getEndTime();
    // use cached TsBlock to keep filling remaining column
    while (currentFilledColumnIndex < outputColumnCount) {
      // current valueColumn can't be filled using current information
      if (fillArray[currentFilledColumnIndex].needPrepareForNext(
          currentEndTime, block.getColumn(currentFilledColumnIndex))) {

        if (canCallNext) { // if we can call child.next(), we call that and cache it in
          // cachedTsBlock
          canCallNext = false;
          TsBlock nextTsBlock = child.next();
          // child operator's calculation is not finished, so we just return null
          if (nextTsBlock == null || nextTsBlock.isEmpty()) {
            return nextTsBlock;
          } else { // otherwise, we cache it
            cachedTsBlock.add(nextTsBlock);
          }
        }

        // next TsBlock has already been in the cachedTsBlock
        while (nextTsBlockIndex[currentFilledColumnIndex] < cachedTsBlock.size()) {
          TsBlock nextTsBlock = cachedTsBlock.get(nextTsBlockIndex[currentFilledColumnIndex]);
          nextTsBlockIndex[currentFilledColumnIndex]++;
          if (fillArray[currentFilledColumnIndex].prepareForNext(
              currentEndTime,
              nextTsBlock.getTimeColumn(),
              nextTsBlock.getColumn(currentFilledColumnIndex))) {
            cachedFilledValueColumns[currentFilledColumnIndex] =
                fillArray[currentFilledColumnIndex].fill(
                    block.getTimeColumn(), block.getColumn(currentFilledColumnIndex));
            break;
          }
        }

        // current column's filling is not finished using current owning TsBlock
        if (cachedFilledValueColumns[currentFilledColumnIndex] == null) {
          if (noMoreTsBlock) { // there is no more TsBlock, just use current owing TsBlock to fill
            cachedFilledValueColumns[currentFilledColumnIndex] =
                fillArray[currentFilledColumnIndex].fill(
                    block.getTimeColumn(), block.getColumn(currentFilledColumnIndex));
          } else { // next TsBlock is not ready, more TsBlocks is needed to do current column's
            // fill, so current calculation is not finished, and we just return null
            return null;
          }
        }
      } else { // current valueColumn can be filled using current information
        cachedFilledValueColumns[currentFilledColumnIndex] =
            fillArray[currentFilledColumnIndex].fill(
                block.getTimeColumn(), block.getColumn(currentFilledColumnIndex));
      }
      currentFilledColumnIndex++;
    }

    TsBlock originTsBlock = cachedTsBlock.remove(0);
    checkArgument(
        outputColumnCount == originTsBlock.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");
    TsBlock result =
        new TsBlock(
            originTsBlock.getPositionCount(),
            originTsBlock.getTimeColumn(),
            cachedFilledValueColumns);
    Arrays.fill(cachedFilledValueColumns, null);
    for (int i = 0; i < outputColumnCount; i++) {
      nextTsBlockIndex[i]--;
    }
    currentFilledColumnIndex = 0;
    return result;
  }

  @Override
  public boolean hasNext() {
    // if child.hasNext() return false, it means that there is no more tsBlocks
    noMoreTsBlock = !child.hasNext();
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
}
