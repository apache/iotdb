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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class EnforceSingleRowOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(EnforceSingleRowOperator.class);

  private final CommonOperatorContext operatorContext;
  private final Operator child;
  private final TsBlock nullRow;

  private boolean hasOutput = false;

  public EnforceSingleRowOperator(
      CommonOperatorContext operatorContext, Operator child, List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.child = child;
    TsBlockBuilder builder = new TsBlockBuilder(1, dataTypes);
    builder.getTimeColumnBuilder().writeLong(0);
    for (int i = 0; i < dataTypes.size(); i++) {
      builder.getColumnBuilder(i).appendNull();
    }
    builder.declarePosition();
    nullRow = builder.build();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    if (child.isFinished()) {
      if (hasOutput) {
        return null;
      }
      // An empty scalar subquery has the value NULL, with its original output types.
      hasOutput = true;
      return nullRow;
    }
    TsBlock tsBlock = child.nextWithTimer();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return tsBlock;
    }
    if (tsBlock.getPositionCount() > 1 || hasOutput) {
      throw new SemanticException(
          CalcMessages.EXCEPTION_SCALAR_SUB_QUERY_HAS_RETURNED_MULTIPLE_ROWS_0C87D6E6);
    }
    hasOutput = true;
    return tsBlock;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public void close() throws Exception {
    if (child != null) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    // Keep consuming after the first row to detect a scalar subquery returning multiple rows.
    // If the child is empty, next() must still emit the single NULL row.
    return child.isFinished() && hasOutput;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(child.calculateMaxPeekMemory(), calculateMaxReturnSize())
        + nullRow.getRetainedSizeInBytes();
  }

  @Override
  public long calculateMaxReturnSize() {
    return Math.max(
        child.calculateMaxReturnSize()
            / TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
        nullRow.getRetainedSizeInBytes());
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext() + nullRow.getRetainedSizeInBytes();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + nullRow.getRetainedSizeInBytes()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return operatorContext;
  }
}
