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

import java.util.List;

public class IdentitySinkOperator implements Operator {

  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final ISinkHandle sinkHandle;

  private boolean needToReturnNull = false;

  private boolean isFinished = false;

  public IdentitySinkOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      DownStreamChannelIndex downStreamChannelIndex,
      ISinkHandle sinkHandle) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.downStreamChannelIndex = downStreamChannelIndex;
    this.sinkHandle = sinkHandle;
  }

  @Override
  public boolean hasNext() {
    if (children.get(downStreamChannelIndex.getCurrentIndex()).hasNext()) {
      return true;
    }
    int currentIndex = downStreamChannelIndex.getCurrentIndex();
    // current channel have no more data
    sinkHandle.setNoMoreTsBlocksOfOneChannel(downStreamChannelIndex.getCurrentIndex());
    currentIndex++;
    if (currentIndex >= children.size()) {
      isFinished = true;
      return false;
    }
    downStreamChannelIndex.setCurrentIndex(currentIndex);
    // if we reach here, it means that isBlocked() is called on a different child
    // we need to ensure that this child is not blocked. We set this field to true here so that we
    // can begin another loop in Driver.
    needToReturnNull = true;
    // tryOpenChannel first
    sinkHandle.tryOpenChannel(currentIndex);
    return true;
  }

  @Override
  public TsBlock next() {
    if (needToReturnNull) {
      needToReturnNull = false;
      return null;
    }
    return children.get(downStreamChannelIndex.getCurrentIndex()).next();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return children.get(downStreamChannelIndex.getCurrentIndex()).isBlocked();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
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
