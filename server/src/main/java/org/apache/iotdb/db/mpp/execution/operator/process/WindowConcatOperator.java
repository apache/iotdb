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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.window.subWindowQueue.AbstractWindowQueue;
import org.apache.iotdb.db.mpp.execution.operator.window.subWindowQueue.CycleWindowQueue;
import org.apache.iotdb.db.mpp.execution.operator.window.subWindowQueue.SingleWindowQueue;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus;

import com.google.common.util.concurrent.ListenableFuture;

public class WindowConcatOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator child;
  private final AbstractWindowQueue subWindowQueue;

  public WindowConcatOperator(
      OperatorContext operatorContext, Operator child, long interval, long step, int valueCount) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.subWindowQueue = genSubWindowQueue(interval, step, valueCount);
  }

  private AbstractWindowQueue genSubWindowQueue(long interval, long step, int valueCount) {
    if (step > interval) {
      return new SingleWindowQueue(valueCount, interval);
    } else {
      return new CycleWindowQueue(valueCount, interval, step);
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {

    boolean processing = false;
    if (subWindowQueue.isReady() || !child.hasNext()) {
      // if tsBlock is full or timeout, finish is false
      // else if current window is finished, finish is true
      processing = subWindowQueue.buildWindow();
    }

    if (!processing) {
      TsBlock tsBlock = child.next();
      if (tsBlock != null) {
        subWindowQueue.cache(tsBlock);
      }
    }

    return subWindowQueue.build();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> blocked = child.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNext() || !subWindowQueue.isEmpty();
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }
}
