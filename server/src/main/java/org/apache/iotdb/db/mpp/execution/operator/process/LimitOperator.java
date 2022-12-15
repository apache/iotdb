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

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LimitOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private long remainingLimit;
  private final Operator child;

  public LimitOperator(OperatorContext operatorContext, long limit, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(limit >= 0, "limit must be at least zero");
    this.remainingLimit = limit;
    this.child = requireNonNull(child, "child operator is null");
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    TsBlock block = child.nextWithTimer();
    if (block == null) {
      return null;
    }
    TsBlock res = block;
    if (block.getPositionCount() <= remainingLimit) {
      remainingLimit -= block.getPositionCount();
    } else {
      res = block.getRegion(0, (int) remainingLimit);
      remainingLimit = 0;
    }
    return res;
  }

  @Override
  public boolean hasNext() {
    return remainingLimit > 0 && child.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return remainingLimit == 0 || child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }
}
