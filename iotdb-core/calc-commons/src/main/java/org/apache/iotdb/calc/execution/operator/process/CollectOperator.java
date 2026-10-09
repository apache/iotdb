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

public class CollectOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CollectOperator.class);

  private final CommonOperatorContext operatorContext;
  protected final List<Operator> children;
  private final AnyChildBlocked childBlocker;
  private int remainingChildren;

  protected int currentIndex;

  public CollectOperator(CommonOperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.currentIndex = 0;
    this.remainingChildren = children.size();
    this.childBlocker =
        new AnyChildBlocked(
            children.size(),
            index -> {
              Operator child = this.children.get(index);
              return child == null ? null : child.isBlocked();
            });
  }

  @Override
  public boolean hasNext() throws Exception {
    return remainingChildren > 0;
  }

  @Override
  public TsBlock next() throws Exception {
    int index = childBlocker.takeReadyChildIndex();
    if (index < 0) {
      return null;
    }

    // Keep the selected index on the driver thread, including while subclasses map the result.
    currentIndex = index;
    Operator child = children.get(index);
    if (child.hasNextWithTimer()) {
      return child.nextWithTimer();
    }
    closeCurrentChild(index);
    return null;
  }

  protected void closeCurrentChild(int index) throws Exception {
    Operator child = children.get(index);
    if (child != null) {
      child.close();
      children.set(index, null);
      remainingChildren--;
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return childBlocker.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return remainingChildren == 0;
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    childBlocker.close();
    for (int i = 0, n = children.size(); i < n; i++) {
      Operator currentChild = children.get(i);
      if (currentChild != null) {
        closeCurrentChild(i);
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    // Switching away from a blocked child may leave its intermediate state retained.
    long retainedSize = calculateRetainedSizeAfterCallingNext();
    long maxPeekMemory = retainedSize;
    for (Operator child : children) {
      if (child != null) {
        maxPeekMemory =
            Math.max(
                maxPeekMemory,
                retainedSize
                    - child.calculateRetainedSizeAfterCallingNext()
                    + child.calculateMaxPeekMemoryWithCounter());
      }
    }
    return maxPeekMemory;
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
    long retainedSize = 0;
    for (Operator child : children) {
      if (child != null) {
        retainedSize += child.calculateRetainedSizeAfterCallingNext();
      }
    }
    return retainedSize;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return children;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + childBlocker.ramBytesUsed()
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
