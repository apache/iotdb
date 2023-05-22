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

package org.apache.iotdb.db.mpp.execution.operator.sink;

import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ShuffleHelperOperator implements Operator {
  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final ISinkHandle sinkHandle;

  private final Set<Integer> unfinishedChildren;

  private boolean needToReturnNull = false;

  public ShuffleHelperOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      DownStreamChannelIndex downStreamChannelIndex,
      ISinkHandle sinkHandle) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.downStreamChannelIndex = downStreamChannelIndex;
    this.sinkHandle = sinkHandle;
    this.unfinishedChildren = new HashSet<>(children.size());
    for (int i = 0; i < children.size(); i++) {
      unfinishedChildren.add(i);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    int currentIndex = downStreamChannelIndex.getCurrentIndex();
    boolean currentChannelClosed = sinkHandle.isChannelClosed(currentIndex);
    if (!currentChannelClosed && children.get(currentIndex).hasNext()) {
      return true;
    } else if (currentChannelClosed) {
      // we close the child directly. The child could be an ExchangeOperator which is the downstream
      // of an ISinkChannel of a pipeline driver.
      closeCurrentChild(currentIndex);
    } else {
      // current channel has no more data
      closeCurrentChild(currentIndex);
      sinkHandle.setNoMoreTsBlocksOfOneChannel(currentIndex);
    }
    unfinishedChildren.remove(currentIndex);
    currentIndex = (currentIndex + 1) % children.size();
    downStreamChannelIndex.setCurrentIndex(currentIndex);
    // if we reach here, it means that isBlocked() is called on a different child
    // we need to ensure that this child is not blocked. We set this field to true here so that we
    // can begin another loop in Driver.
    needToReturnNull = true;
    // tryOpenChannel first
    sinkHandle.tryOpenChannel(currentIndex);
    return true;
  }

  private void closeCurrentChild(int index) throws Exception {
    children.get(index).close();
    children.set(index, null);
  }

  @Override
  public TsBlock next() throws Exception {
    if (needToReturnNull) {
      needToReturnNull = false;
      return null;
    }
    return children.get(downStreamChannelIndex.getCurrentIndex()).next();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    int steps = 0;
    int currentIndex = downStreamChannelIndex.getCurrentIndex();
    // skip closed children
    while (children.get(currentIndex) == null && steps < children.size()) {
      currentIndex = (currentIndex + 1) % children.size();
      steps++;
    }
    downStreamChannelIndex.setCurrentIndex(currentIndex);
    Operator child = children.get(currentIndex);
    return child == null ? NOT_BLOCKED : child.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return unfinishedChildren.isEmpty() || sinkHandle.isClosed();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      if (child != null) {
        child.close();
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemory());
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
}
