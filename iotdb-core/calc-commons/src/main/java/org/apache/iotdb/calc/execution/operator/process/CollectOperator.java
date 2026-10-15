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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.utils.TestOnly;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static com.google.common.util.concurrent.Futures.getDone;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class CollectOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CollectOperator.class);

  private final CommonOperatorContext operatorContext;
  protected final List<Operator> children;
  private AnyChildBlocked anyChildBlocked;
  private boolean inited;
  protected int currentIndex;
  private ListenableFuture<Integer> selectionFuture;
  private ListenableFuture<Integer> exposedFuture;
  private int remainingChildren;
  private boolean closed;

  public CollectOperator(CommonOperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.remainingChildren = children.size();
  }

  /**
   * Enable only after the complete table FI has passed eligibility checks, before drivers start.
   */
  public void enableReadyInputConsumption() {
    if (anyChildBlocked == null) {
      anyChildBlocked = new AnyChildBlocked(children);
    }
  }

  @Override
  public boolean hasNext() {
    return anyChildBlocked == null
        ? currentIndex < children.size()
        : !closed && remainingChildren > 0;
  }

  @Override
  public TsBlock next() throws Exception {
    if (anyChildBlocked == null) {
      if (children.get(currentIndex).hasNextWithTimer()) {
        TsBlock block = children.get(currentIndex).nextWithTimer();
        return block == null ? null : transformBlock(currentIndex, block);
      }
      closeCurrentChild(currentIndex);
      currentIndex++;
      return null;
    }
    anyChildBlocked.checkFailure();
    if (!hasNext()) {
      return null;
    }
    if (selectionFuture == null) {
      isBlocked();
    }
    anyChildBlocked.checkFailure();
    if (!selectionFuture.isDone()) {
      return null;
    }
    int childId = getDone(selectionFuture);
    // Consume this selection before invoking any operator method. Another next must acquire a
    // new readiness observation even when this advancement returns null or an empty block.
    selectionFuture = null;
    exposedFuture = null;
    if (childId < 0 || closed || children.get(childId) == null) {
      return null;
    }
    Operator child = children.get(childId);
    try {
      if (child.hasNextWithTimer()) {
        TsBlock block = child.nextWithTimer();
        return block == null ? null : transformBlock(childId, block);
      }
      if (child.isFinished()) {
        closeCurrentChild(childId);
      }
      return null;
    } finally {
      anyChildBlocked.invalidateChild(childId);
    }
  }

  protected TsBlock transformBlock(int childId, TsBlock block) {
    return block;
  }

  protected void closeCurrentChild(int index) throws Exception {
    if (anyChildBlocked == null) {
      children.get(index).close();
      children.set(index, null);
      return;
    }
    Operator child = children.get(index);
    if (child == null) {
      return;
    }
    // Retire the record even if closing its subtree throws.
    anyChildBlocked.finishChild(index);
    children.set(index, null);
    remainingChildren--;
    child.close();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (anyChildBlocked == null) {
      if (!inited) {
        inited = true;
        for (Operator child : children) {
          child.isBlocked();
        }
      }
      return currentIndex >= children.size() ? NOT_BLOCKED : children.get(currentIndex).isBlocked();
    }
    if (!hasNext()) {
      return NOT_BLOCKED;
    }
    if (selectionFuture == null) {
      selectionFuture = anyChildBlocked.getAnyChild();
    }
    if (exposedFuture == null || exposedFuture.isCancelled()) {
      exposedFuture = nonCancellationPropagating(selectionFuture);
    }
    return exposedFuture;
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    if (anyChildBlocked == null) {
      for (int i = currentIndex; i < children.size(); i++) {
        if (children.get(i) != null) {
          closeCurrentChild(i);
        }
      }
      return;
    }
    if (closed) {
      return;
    }
    closed = true;
    anyChildBlocked.close();
    selectionFuture = null;
    exposedFuture = null;
    Exception firstException = null;
    for (int i = 0; i < children.size(); i++) {
      try {
        closeCurrentChild(i);
      } catch (Exception e) {
        if (firstException == null) {
          firstException = e;
        } else if (firstException != e) {
          firstException.addSuppressed(e);
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    if (anyChildBlocked == null) {
      long maxPeekMemory = 0;
      for (Operator child : children) {
        maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemoryWithCounter());
      }
      return maxPeekMemory;
    }
    long retained = calculateRetainedSizeAfterCallingNext();
    long peak = retained;
    for (Operator child : children) {
      if (child != null) {
        peak =
            Math.max(
                peak,
                retained
                    - child.calculateRetainedSizeAfterCallingNext()
                    + child.calculateMaxPeekMemoryWithCounter());
      }
    }
    return peak;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnSize = 0;
    for (Operator child : children) {
      if (child != null) {
        maxReturnSize = Math.max(maxReturnSize, child.calculateMaxReturnSize());
      }
    }
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    if (anyChildBlocked == null) {
      return 0;
    }
    long retained = 0;
    for (Operator child : children) {
      if (child != null) {
        retained += child.calculateRetainedSizeAfterCallingNext();
      }
    }
    return retained;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return children;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + (anyChildBlocked == null ? 0 : anyChildBlocked.ramBytesUsed())
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
