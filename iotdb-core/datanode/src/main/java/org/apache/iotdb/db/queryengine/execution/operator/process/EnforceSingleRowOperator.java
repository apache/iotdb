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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class EnforceSingleRowOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(EnforceSingleRowOperator.class);

  private static final String ERROR_MESSAGE = "Scalar sub-query has returned multiple rows.";

  private final OperatorContext operatorContext;
  private final Operator child;

  private boolean finished = false;

  public EnforceSingleRowOperator(OperatorContext operatorContext, Operator child) {
    this.operatorContext = operatorContext;
    this.child = child;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    if (finished) {
      throw new IllegalStateException(ERROR_MESSAGE);
    }
    TsBlock tsBlock = child.next();
    if (tsBlock != null && tsBlock.getPositionCount() > 1) {
      throw new IllegalStateException(ERROR_MESSAGE);
    }
    if (tsBlock != null) {
      finished = true;
    }
    return tsBlock;
  }

  @Override
  public boolean hasNext() throws Exception {
    boolean hasNext = child.hasNext();
    if (finished && hasNext) {
      throw new IllegalStateException(ERROR_MESSAGE);
    }
    return hasNext;
  }

  @Override
  public void close() throws Exception {
    if (child != null) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished || child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize()
        / TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
