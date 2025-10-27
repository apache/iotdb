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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

public class LastQueryTransformOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryTransformOperator.class);

  private String viewPath;

  private String dataType;

  private final OperatorContext operatorContext;

  // the child of LastQueryTransformOperator will always be AggOperator
  private final Operator child;

  private TsBlockBuilder tsBlockBuilder;

  public LastQueryTransformOperator(
      String viewPath, String dataType, OperatorContext operatorContext, Operator child) {
    this.viewPath = viewPath;
    this.dataType = dataType;
    this.operatorContext = operatorContext;
    this.child = child;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder(1);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    if (!tsBlockBuilder.isFull()) {
      TsBlock tsBlock = child.nextWithTimer();
      if (tsBlock == null) {
        return null;
      } else if (!tsBlock.isEmpty()) {
        if (tsBlock.getColumn(1).isNull(0)) {
          return null;
        }
        LastQueryUtil.appendLastValueRespectBlob(
            tsBlockBuilder,
            tsBlock.getColumn(0).getLong(0),
            viewPath,
            tsBlock.getColumn(1).getTsPrimitiveType(0),
            dataType);
      }
    } else {
      child.close();
    }

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNext();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(child.calculateMaxPeekMemory(), child.calculateRetainedSizeAfterCallingNext());
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public void close() throws Exception {
    if (child != null) {
      child.close();
    }
    tsBlockBuilder = null;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + RamUsageEstimator.sizeOf(viewPath)
        + RamUsageEstimator.sizeOf(dataType)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
