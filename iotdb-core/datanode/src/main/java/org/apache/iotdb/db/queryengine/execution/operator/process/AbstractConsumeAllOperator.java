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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.operator.AbstractOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

/** ConsumeAllOperator will consume all children's result every time. */
public abstract class AbstractConsumeAllOperator extends AbstractOperator
    implements ProcessOperator {
  protected final List<Operator> children;
  protected final int inputOperatorsCount;

  /** TsBlock from child operator. Only one cache now. */
  protected TsBlock[] inputTsBlocks;

  protected final boolean[] canCallNext;
  protected int readyChildIndex;

  /** Index of the child that is currently fetching input */
  protected int currentChildIndex = 0;

  /** Indicate whether we found an empty child input in one loop */
  protected boolean hasEmptyChildInput = false;

  protected AbstractConsumeAllOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.canCallNext = new boolean[inputOperatorsCount];
    for (int i = 0; i < inputOperatorsCount; i++) {
      canCallNext[i] = false;
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    readyChildIndex = 0;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i) || children.get(i) == null) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        canCallNext[i] = true;
        readyChildIndex = i;
      } else {
        listenableFutures.add(blocked);
      }
    }
    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  /**
   * Try to cache one result of each child.
   *
   * @return true if results of all children are ready. Return false if some children is blocked or
   *     return null.
   * @throws Exception errors happened while getting tsblock from children
   */
  protected boolean prepareInput() throws Exception {
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    while (System.nanoTime() - start < maxRuntime && currentChildIndex < inputOperatorsCount) {
      if (canSkipCurrentChild(currentChildIndex)) {
        currentChildIndex++;
        continue;
      }
      if (canCallNext[currentChildIndex]) {
        if (children.get(currentChildIndex).hasNextWithTimer()) {
          inputTsBlocks[currentChildIndex] = getNextTsBlock(currentChildIndex);
          canCallNext[currentChildIndex] = false;
          // child operator has next but return an empty TsBlock which means that it may not
          // finish calculation in given time slice.
          // In such case, TimeJoinOperator can't go on calculating, so we just return null.
          // We can also use the while loop here to continuously call the hasNext() and next()
          // methods of the child operator until its hasNext() returns false or the next() gets
          // the data that is not empty, but this will cause the execution time of the while loop
          // to be uncontrollable and may exceed all allocated time slice
          if (isEmpty(currentChildIndex)) {
            hasEmptyChildInput = true;
          } else {
            processCurrentInputTsBlock(currentChildIndex);
          }
        } else {
          handleFinishedChild(currentChildIndex);
        }
      } else {
        hasEmptyChildInput = true;
      }
      currentChildIndex++;
    }

    if (currentChildIndex == inputOperatorsCount) {
      // start a new loop
      currentChildIndex = 0;
      if (!hasEmptyChildInput) {
        // all children are ready now
        return true;
      } else {
        // In a new loop, previously empty child input could be non-empty now, and we can skip the
        // children that have generated input
        hasEmptyChildInput = false;
      }
    }
    return false;
  }

  /** If the tsBlock is null or has no more data in the tsBlock, return true; else return false. */
  protected boolean isEmpty(int index) {
    return inputTsBlocks[index] == null || inputTsBlocks[index].isEmpty();
  }

  // region helper function used in prepareInput, the subclass can have its own implementation

  /**
   * @param currentChildIndex the index of the child
   * @return true if we can skip the currentChild in prepareInput
   */
  protected boolean canSkipCurrentChild(int currentChildIndex) {
    return !isEmpty(currentChildIndex) || children.get(currentChildIndex) == null;
  }

  /**
   * @param currentInputIndex index of the input TsBlock
   */
  protected void processCurrentInputTsBlock(int currentInputIndex) {
    // do nothing here, the subclass have its own implementation
  }

  /**
   * @param currentChildIndex the index of the child
   * @throws Exception Potential Exception thrown by Operator.close()
   */
  protected void handleFinishedChild(int currentChildIndex) throws Exception {
    children.get(currentChildIndex).close();
    children.set(currentChildIndex, null);
  }

  // endregion

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      if (child != null) {
        child.close();
      }
    }
    // friendly for gc
    inputTsBlocks = null;
  }

  protected TsBlock getNextTsBlock(int childIndex) throws Exception {
    return children.get(childIndex).nextWithTimer();
  }
}
