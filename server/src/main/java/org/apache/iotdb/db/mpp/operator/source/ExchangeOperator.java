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
package org.apache.iotdb.db.mpp.operator.source;

import org.apache.iotdb.db.mpp.buffer.ISourceHandle;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;

public class ExchangeOperator implements SourceOperator {

  private final OperatorContext operatorContext;

  private final ISourceHandle sourceHandle;

  private final PlanNodeId sourceId;

  private ListenableFuture<Void> isBlocked = NOT_BLOCKED;

  public ExchangeOperator(
      OperatorContext operatorContext, ISourceHandle sourceHandle, PlanNodeId sourceId) {
    this.operatorContext = operatorContext;
    this.sourceHandle = sourceHandle;
    this.sourceId = sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    try {
      return sourceHandle.receive();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error happened while reading from source handle " + sourceHandle, e);
    }
  }

  @Override
  public boolean hasNext() {
    return !sourceHandle.isFinished();
  }

  @Override
  public boolean isFinished() throws IOException {
    return sourceHandle.isFinished();
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
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
