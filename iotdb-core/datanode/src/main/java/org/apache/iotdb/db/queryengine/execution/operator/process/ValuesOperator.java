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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ValuesOperator implements Operator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ValuesOperator.class);

  private final OperatorContext operatorContext;
  private final Iterator<TsBlock> tsBlocks;
  private final long maxTsBlockSize;
  private long currentRetainedSize;

  public ValuesOperator(OperatorContext operatorContext, List<TsBlock> tsBlocks) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    requireNonNull(tsBlocks, "tsBlocks is null");

    this.tsBlocks = ImmutableList.copyOf(tsBlocks).iterator();

    long maxSize = 0;
    long totalSize = 0;
    for (TsBlock tsBlock : tsBlocks) {
      long blockSize = tsBlock.getRetainedSizeInBytes();
      maxSize = Math.max(maxSize, blockSize);
      totalSize += blockSize;
    }

    this.maxTsBlockSize = maxSize;
    this.currentRetainedSize = totalSize;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!tsBlocks.hasNext()) {
      return null;
    }

    TsBlock tsBlock = tsBlocks.next();
    if (tsBlock != null) {
      currentRetainedSize -= tsBlock.getRetainedSizeInBytes();
    }

    return tsBlock;
  }

  @Override
  public boolean hasNext() throws Exception {
    return tsBlocks.hasNext();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean isFinished() throws Exception {
    return !tsBlocks.hasNext();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxTsBlockSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxTsBlockSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return currentRetainedSize;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
