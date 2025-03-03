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
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

public class AssignUniqueIdOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AssignUniqueIdOperator.class);

  private static final long ROW_IDS_PER_REQUEST = 1L << 20L;
  private static final long MAX_ROW_ID = 1L << 40L;

  private final OperatorContext operatorContext;
  private final Operator child;

  private final AtomicLong rowIdPool = new AtomicLong();
  private final long uniqueValueMask;

  private long rowIdCounter;
  private long maxRowIdCounterValue;

  public AssignUniqueIdOperator(OperatorContext operatorContext, Operator child) {
    this.operatorContext = operatorContext;
    this.child = child;

    DriverTaskId id = operatorContext.getDriverContext().getDriverTaskID();
    this.uniqueValueMask =
        (((long) id.getFragmentId().getId()) << 54) | (((long) id.getPipelineId()) << 40);
    requestValues();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock tsBlock = child.next();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return null;
    }
    return tsBlock.appendValueColumns(new Column[] {generateIdColumn(tsBlock.getPositionCount())});
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNext();
  }

  @Override
  public void close() throws Exception {
    if (child != null) {
      child.close();
    }
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
    return child.calculateMaxReturnSize()
        + (long) Long.SIZE * TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
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

  private Column generateIdColumn(int positionCount) {
    LongColumnBuilder columnBuilder = new LongColumnBuilder(null, positionCount);
    for (int currentPosition = 0; currentPosition < positionCount; currentPosition++) {
      if (rowIdCounter >= maxRowIdCounterValue) {
        requestValues();
      }
      long rowId = rowIdCounter++;
      verify((rowId & uniqueValueMask) == 0, "RowId and uniqueValue mask overlaps");
      columnBuilder.writeLong(uniqueValueMask | rowId);
    }
    return columnBuilder.build();
  }

  private void requestValues() {
    rowIdCounter = rowIdPool.getAndAdd(ROW_IDS_PER_REQUEST);
    maxRowIdCounterValue = Math.min(rowIdCounter + ROW_IDS_PER_REQUEST, MAX_ROW_ID);
    checkState(rowIdCounter < MAX_ROW_ID, "Unique row id exceeds a limit: %s", MAX_ROW_ID);
  }
}
