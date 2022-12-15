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

public class OffsetOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private long remainingOffset;
  private final Operator child;

  public OffsetOperator(OperatorContext operatorContext, long offset, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(offset >= 0, "offset must be at least zero");
    this.remainingOffset = offset;
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
    if (remainingOffset > 0) {
      int offset = Math.min((int) remainingOffset, block.getPositionCount());
      remainingOffset -= offset;
      return block.getRegion(offset, block.getPositionCount() - offset);
    } else {
      return block;
    }
  }

  @Override
  public boolean hasNext() {
    return child.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return child.isFinished();
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
