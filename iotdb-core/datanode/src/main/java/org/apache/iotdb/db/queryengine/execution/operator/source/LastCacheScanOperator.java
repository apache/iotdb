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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class LastCacheScanOperator implements SourceOperator {

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;
  private TsBlock tsBlock;

  public LastCacheScanOperator(
      OperatorContext operatorContext, PlanNodeId sourceId, TsBlock tsBlock) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.tsBlock = tsBlock;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock res = tsBlock;
    tsBlock = null;
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    return tsBlock != null && !tsBlock.isEmpty();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return tsBlock.getRetainedSizeInBytes();
  }

  @Override
  public long calculateMaxReturnSize() {
    return tsBlock.getRetainedSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }
}
