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
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;

// TODO:
public class PatternRecognitionOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PatternRecognitionOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;

  public PatternRecognitionOperator(OperatorContext operatorContext, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.child = requireNonNull(child, "child operator is null");
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
    TsBlock input = child.nextWithTimer();
    if (input == null) {
      return null;
    }
    int sz = input.getValueColumns().length;
    Column[] valueColumns = new Column[sz];
    for (int i = 0; i < sz; i++) {
      valueColumns[i] = input.getColumn(i);
    }
    return new TsBlock(input.getPositionCount(), input.getTimeColumn(), valueColumns);
  }

  //  public TsBlock next() throws Exception {
  //    // 获取子节点的下一个数据块，并直接返回
  //    return child.next();
  //  }

  @Override
  public boolean hasNext() throws Exception {
    // 如果子节点有数据，则本操作符也有数据
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
    return child.calculateMaxPeekMemory();
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
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
