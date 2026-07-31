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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.fill.IFill;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/** Used for previous and constant value fill. */
public abstract class AbstractFillOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractFillOperator.class);
  private final CommonOperatorContext operatorContext;
  private final IFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;

  protected AbstractFillOperator(
      CommonOperatorContext operatorContext, IFill[] fillArray, Operator child) {
    this.operatorContext =
        requireNonNull(operatorContext, CalcMessages.EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB);
    checkArgument(
        fillArray != null && fillArray.length > 0,
        CalcMessages.EXCEPTION_FILLARRAY_SHOULD_NOT_BE_NULL_OR_EMPTY_118FB134);
    this.fillArray = fillArray;
    this.child = requireNonNull(child, CalcMessages.EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C);
    this.outputColumnCount = fillArray.length;
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
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
        CalcMessages
            .EXCEPTION_OUTPUTCOLUMNCOUNT_IS_NOT_EQUAL_TO_VALUE_COLUMN_COUNT_OF_CHILD_OPERATOR_QUOTE_S_T_8E30BAD8);

    Column[] valueColumns = new Column[outputColumnCount];

    for (int i = 0; i < outputColumnCount; i++) {
      valueColumns[i] = fillArray[i].fill(getHelperColumn(block), block.getColumn(i));
    }

    return TsBlock.wrapBlocksWithoutCopy(
        block.getPositionCount(), block.getTimeColumn(), valueColumns);
  }

  protected abstract Column getHelperColumn(TsBlock tsBlock);

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
