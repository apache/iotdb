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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class CollectOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CollectOperator.class);

  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private int currentIndex;

  public CollectOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.currentIndex = 0;
  }

  @Override
  public boolean hasNext() throws Exception {
    return currentIndex < children.size();
  }

  @Override
  public TsBlock next() throws Exception {
    if (children.get(currentIndex).hasNextWithTimer()) {
      return children.get(currentIndex).nextWithTimer();
    } else {
      closeCurrentChild(currentIndex);
      currentIndex++;
      return null;
    }
  }

  private void closeCurrentChild(int index) throws Exception {
    children.get(index).close();
    children.set(index, null);
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex >= children.size()) {
      return NOT_BLOCKED;
    }
    return children.get(currentIndex).isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return currentIndex >= children.size();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    for (int i = currentIndex, n = children.size(); i < n; i++) {
      Operator currentChild = children.get(i);
      if (currentChild != null) {
        closeCurrentChild(i);
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemoryWithCounter());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnSize = 0;
    for (Operator child : children) {
      maxReturnSize = Math.max(maxReturnSize, child.calculateMaxReturnSize());
    }
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return children;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
