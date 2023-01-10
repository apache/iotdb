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
package org.apache.iotdb.db.mpp.execution.operator.process.last;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class LastQueryCollectOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private final int inputOperatorsCount;

  private int currentIndex;

  public LastQueryCollectOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.currentIndex = 0;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex < inputOperatorsCount) {
      return children.get(currentIndex).isBlocked();
    } else {
      return Futures.immediateVoidFuture();
    }
  }

  @Override
  public TsBlock next() {
    if (children.get(currentIndex).hasNextWithTimer()) {
      return children.get(currentIndex).nextWithTimer();
    } else {
      currentIndex++;
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return currentIndex < inputOperatorsCount;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemory());
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateRetainedSizeAfterCallingNext());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnMemory = 0;
    for (Operator child : children) {
      maxReturnMemory = Math.max(maxReturnMemory, child.calculateMaxReturnSize());
    }
    return maxReturnMemory;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long sum = 0;
    for (Operator operator : children) {
      sum += operator.calculateRetainedSizeAfterCallingNext();
    }
    return sum;
  }
}
