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

import java.util.Arrays;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/** Used for linear fill */
public class LinearFillOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final LinearFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;
  // TODO need to spill it to disk if it consumes too much memory
  private final LinkedList<TsBlock> cachedTsBlock;
  private final Column[] cachedFilledValueColumns;

  private int currentFilledColumnIndex;

  public LinearFillOperator(
      OperatorContext operatorContext, LinearFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    this.cachedTsBlock = new LinkedList<>();
    this.cachedFilledValueColumns = new Column[outputColumnCount];
    this.currentFilledColumnIndex = 0;
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
    boolean alreadyCallNext = false;
    if (cachedTsBlock.isEmpty()) {
      alreadyCallNext = true;
      TsBlock nextBlock = child.next();
      // child operator's calculation is not finished, so we just return null
      if (nextBlock == null || nextBlock.isEmpty()) {
        return nextBlock;
      } else { // otherwise, we cache it
        cachedTsBlock.addLast(nextBlock);
      }
    }

    TsBlock block = cachedTsBlock.getFirst();
    long currentEndTime = block.getEndTime();
    // use cached TsBlock to keep filling remaining column
    while (currentFilledColumnIndex < outputColumnCount) {
      if (fillArray[currentFilledColumnIndex].needPrepareForNext(
          currentEndTime, block.getColumn(currentFilledColumnIndex))) {
        if (!alreadyCallNext) {
          alreadyCallNext = true;
          TsBlock nextBlock = child.next();
          // child operator's calculation is not finished, so we just return null
          if (nextBlock == null || nextBlock.isEmpty()) {
            return nextBlock;
          } else { // otherwise, we cache it
            cachedTsBlock.addLast(nextBlock);
          }
          if (fillArray[currentFilledColumnIndex].prepareForNext(
              currentEndTime,
              cachedTsBlock.getLast().getTimeColumn(),
              cachedTsBlock.getLast().getColumn(currentFilledColumnIndex))) {
            cachedFilledValueColumns[currentFilledColumnIndex] =
                fillArray[currentFilledColumnIndex].fill(
                    block.getTimeColumn(), block.getColumn(currentFilledColumnIndex));
          } else { // more TsBlocks is needed to do current column's fill, so current calculation is
            // not finished, and we just return null
            return null;
          }
        } else { // more TsBlocks is needed to do current column's fill, so current calculation is
          // not finished, and we just return null
          return null;
        }
      } else {
        cachedFilledValueColumns[currentFilledColumnIndex] =
            fillArray[currentFilledColumnIndex].fill(
                block.getTimeColumn(), block.getColumn(currentFilledColumnIndex));
      }
      currentFilledColumnIndex++;
    }

    TsBlock originTsBlock = cachedTsBlock.removeFirst();
    checkArgument(
        outputColumnCount == originTsBlock.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");
    TsBlock result =
        TsBlock.wrapBlocksWithoutCopy(
            originTsBlock.getPositionCount(),
            originTsBlock.getTimeColumn(),
            cachedFilledValueColumns);
    Arrays.fill(cachedFilledValueColumns, null);
    currentFilledColumnIndex = 0;
    return result;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = child.hasNext();
    if (!hasNext) {
      for (LinearFill linearFill : fillArray) {
        linearFill.setNoMoreData();
      }
    }
    return !cachedTsBlock.isEmpty() || hasNext;
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
