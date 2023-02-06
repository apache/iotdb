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

import java.util.List;

public class IdentityOperator implements Operator {

  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final ISinkHandle sinkHandle;

  public IdentityOperator(
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
    while (downStreamChannelIndex.getCurrentIndex() < children.size()) {
      if (children.get(downStreamChannelIndex.getCurrentIndex()).hasNext()) {
        return true;
      } else {
        // current channel will have no input data
        sinkHandle.setNoMoreTsBlocksOfOneChannel(downStreamChannelIndex.getCurrentIndex());
      }
      // see if next channel has data
      downStreamChannelIndex.increment();
    }
    return false;
  }

  @Override
  public TsBlock next() {
    return children.get(downStreamChannelIndex.getCurrentIndex()).next();
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
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
