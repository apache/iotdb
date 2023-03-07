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
package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.db.mpp.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ExchangeOperator implements SourceOperator {

  private final OperatorContext operatorContext;

  private final ISourceHandle sourceHandle;

  private final PlanNodeId sourceId;

  private ListenableFuture<?> isBlocked = NOT_BLOCKED;

  private long maxReturnSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  public ExchangeOperator(
      OperatorContext operatorContext, ISourceHandle sourceHandle, PlanNodeId sourceId) {
    this.operatorContext = operatorContext;
    this.sourceHandle = sourceHandle;
    this.sourceId = sourceId;
  }

  /**
   * For ExchangeOperator in pipeline, the maxReturnSize is equal to the maxReturnSize of the child
   * operator.
   *
   * @param maxReturnSize max return size of child operator
   */
  public ExchangeOperator(
      OperatorContext operatorContext,
      ISourceHandle sourceHandle,
      PlanNodeId sourceId,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.sourceHandle = sourceHandle;
    this.sourceId = sourceId;
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    return sourceHandle.receive();
  }

  @Override
  public boolean hasNext() {
    return !sourceHandle.isFinished();
  }

  @Override
  public boolean isFinished() {
    return sourceHandle.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  public ISourceHandle getSourceHandle() {
    return sourceHandle;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    // Avoid registering a new callback in the source handle when one is already pending
    if (isBlocked.isDone()) {
      isBlocked = sourceHandle.isBlocked();
      if (isBlocked.isDone()) {
        isBlocked = NOT_BLOCKED;
      }
    }
    return isBlocked;
  }

  @Override
  public void close() throws Exception {
    sourceHandle.close();
  }
}
