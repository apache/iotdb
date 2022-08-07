/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.mpp.execution.operator.process.fill.IPreviousUntilLastFill;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PreviousUntilLastFillOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final IPreviousUntilLastFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;
  // TODO need to spill it to disk if it consumes too much memory
  private Map<Integer, TsBlock> cachedTsBlock;
  /** Record the index of the next tsblock to be put into the cache */
  private AtomicInteger blockPutIndex;
  /** Record the index of the next tsblock to be filled */
  private AtomicInteger blockGetIndex;
  /** indicate whether there is more TsBlock for child operator */
  private boolean noMoreTsBlock;
  /**
   * When obtaining the next tsblock, if the child operator's calculation is not finished, then set
   * the needNextTsBlock field to true. Prevent repeated execution of the method
   * needPrepareForNext() and the method isCachedTsBlockEnough()
   */
  private boolean needNextTsBlock;
  /**
   * When obtaining the next tsblock, if the child operator's calculation is not finished, then
   * record the index of the current column Prevent repeated execution of previously executed
   * columns
   */
  private int currentColumnIndex;

  public PreviousUntilLastFillOperator(
      OperatorContext operatorContext, IPreviousUntilLastFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    this.cachedTsBlock = new HashMap<>();
    this.blockPutIndex = new AtomicInteger(0);
    this.blockGetIndex = new AtomicInteger(0);
    this.noMoreTsBlock = false;
    this.needNextTsBlock = false;
    this.currentColumnIndex = 0;
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

    // When there is no cache, it will actively get the next tsblock
    if (cachedTsBlock.size() == 0) {
      TsBlock nextTsBlock = child.next();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        return nextTsBlock;
      } else {
        cachedTsBlock.put(blockPutIndex.getAndIncrement(), nextTsBlock);
      }
    }

    // If the child operator has no more tsblocks, it can be filled directly
    if (!noMoreTsBlock) {
      TsBlock originTsBlock = cachedTsBlock.get(blockGetIndex.get());
      // Each column needs to know if it needs the next tsblock
      // Start looping from the previous column
      for (currentColumnIndex = needNextTsBlock ? currentColumnIndex : 0;
          currentColumnIndex < outputColumnCount;
          currentColumnIndex++) {
        // Determine whether it is because the null was obtained when the next tsblock was obtained
        // last time. If it is obtained, then go directly to the next tsblock.
        if (needNextTsBlock) {
          if (!tryToGetNextNonEmptyTsBlock(currentColumnIndex)) {
            if (!noMoreTsBlock) {
              return null;
            }
          }
        }
        // current valueColumn can't be filled using current information
        if (fillArray[currentColumnIndex].needPrepareForNext(
            originTsBlock.getColumn(currentColumnIndex), blockGetIndex.get())) {
          // current cached TsBlock is not enough to fill this column
          if (!isCachedTsBlockEnough(currentColumnIndex)) {
            // if we failed to get next TsBlock
            if (!tryToGetNextNonEmptyTsBlock(currentColumnIndex)) {
              // there is still more TsBlock, so current calculation is not finished, and we just
              // return null
              if (!noMoreTsBlock) {
                return null;
              }
            }
          }
        }
      }
    }

    Column[] valueColumns = new Column[outputColumnCount];
    TsBlock block = cachedTsBlock.get(blockGetIndex.get());
    checkArgument(
        outputColumnCount == block.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");
    for (int i = 0; i < outputColumnCount; i++) {
      valueColumns[i] = fillArray[i].fill(block.getColumn(i), blockGetIndex.get());
    }
    cachedTsBlock.remove(blockGetIndex.getAndIncrement());
    return TsBlock.wrapBlocksWithoutCopy(
        block.getPositionCount(), block.getTimeColumn(), valueColumns);
  }

  /** Determine if there are enough tsblocks in the cache */
  private boolean isCachedTsBlockEnough(int columnIndex) {
    // If cachedTsBlock.size() == 1, then this tsblock is itself and needs to return false
    if (cachedTsBlock.size() > 1) {
      // Start looping from the next tsblock of the current tsblock
      for (int nextIndex = blockGetIndex.get() + 1;
          nextIndex <= cachedTsBlock.size();
          nextIndex++) {
        TsBlock nextTsBlock = cachedTsBlock.get(nextIndex);
        // Determine whether the next tsblock has a value, and if so, update the index of the
        // lastHasValueTsBlockIndex
        if (!fillArray[columnIndex].allValueIsNull(nextTsBlock.getColumn(columnIndex))) {
          fillArray[columnIndex].updateLastHasValueTsBlockIndex(nextIndex);
          needNextTsBlock = false;
          return true;
        }
      }
      return false;
    }
    return false;
  }

  /**
   * to get the next tsblock that has a value.Because whether the previous tsblock can be filled
   * needs to know whether the subsequent tsblock has a value
   */
  private boolean tryToGetNextNonEmptyTsBlock(int columnIndex) {
    while (child.hasNext()) {
      TsBlock nextTsBlock = child.next();
      // child operator's calculation is not finished, so we just return null
      if (nextTsBlock == null || nextTsBlock.isEmpty()) {
        needNextTsBlock = true;
        noMoreTsBlock = false;
        return false;
      }
      cachedTsBlock.put(blockPutIndex.getAndIncrement(), nextTsBlock);
      if (!fillArray[columnIndex].allValueIsNull(nextTsBlock.getColumn(columnIndex))) {
        // update the index of the lastHasValueTsBlockIndex
        fillArray[columnIndex].updateLastHasValueTsBlockIndex(blockPutIndex.get() - 1);
        needNextTsBlock = false;
        return true;
      }
    }
    // No more tsBlock
    needNextTsBlock = false;
    noMoreTsBlock = true;
    return false;
  }

  @Override
  public boolean hasNext() {
    return child.hasNext() || !cachedTsBlock.isEmpty();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return child.isFinished() && cachedTsBlock.isEmpty();
  }
}
