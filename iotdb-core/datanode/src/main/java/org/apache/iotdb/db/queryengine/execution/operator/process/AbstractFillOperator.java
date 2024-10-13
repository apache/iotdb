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
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/** Used for previous and constant value fill. */
abstract class AbstractFillOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractFillOperator.class);
  private final OperatorContext operatorContext;
  private final IFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;

  AbstractFillOperator(OperatorContext operatorContext, IFill[] fillArray, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
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
  public TsBlock next() throws Exception {
    TsBlock block = child.nextWithTimer();
    if (block == null) {
      return null;
    }

    checkArgument(
        outputColumnCount == block.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");

    Column[] valueColumns = new Column[outputColumnCount];

    for (int i = 0; i < outputColumnCount; i++) {
      valueColumns[i] = fillArray[i].fill(getHelperColumn(block), block.getColumn(i));
    }

    return TsBlock.wrapBlocksWithoutCopy(
        block.getPositionCount(), block.getTimeColumn(), valueColumns);
  }

  abstract Column getHelperColumn(TsBlock tsBlock);

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    // while doing constant and previous fill, we may need to copy the corresponding column if there
    // exists null values
    // so the max peek memory may be double
    return 2 * child.calculateMaxPeekMemory() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    // we can safely ignore one line cached in IFill
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
