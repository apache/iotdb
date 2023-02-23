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

import org.apache.iotdb.db.mpp.execution.operator.AbstractOperator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.successfulAsList;

/** ConsumeAllOperator will consume all children's result every time. */
public abstract class AbstractConsumeAllOperator extends AbstractOperator
    implements ProcessOperator {
  protected final List<Operator> children;
  protected final int inputOperatorsCount;
  /** TsBlock from child operator. Only one cache now. */
  protected final TsBlock[] inputTsBlocks;

  protected final boolean[] canCallNext;
  protected int readyChildIndex;

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
      if (!isEmpty(i)) {
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
   */
  protected boolean prepareInput() {
    boolean allReady = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i)) {
        continue;
      }
      if (canCallNext[i] && children.get(i).hasNextWithTimer()) {
        inputTsBlocks[i] = getNextTsBlock(i);
        canCallNext[i] = false;
        // child operator has next but return an empty TsBlock which means that it may not
        // finish calculation in given time slice.
        // In such case, TimeJoinOperator can't go on calculating, so we just return null.
        // We can also use the while loop here to continuously call the hasNext() and next()
        // methods of the child operator until its hasNext() returns false or the next() gets
        // the data that is not empty, but this will cause the execution time of the while loop
        // to be uncontrollable and may exceed all allocated time slice
        if (isEmpty(i)) {
          allReady = false;
        }
      } else {
        allReady = false;
      }
    }
    return allReady;
  }

  /** If the tsBlock is null or has no more data in the tsBlock, return true; else return false. */
  protected boolean isEmpty(int index) {
    return inputTsBlocks[index] == null || inputTsBlocks[index].isEmpty();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  protected TsBlock getNextTsBlock(int childIndex) {
    return children.get(childIndex).nextWithTimer();
  }
}
