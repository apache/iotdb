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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.MarkDistinctHash;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.CURRENT_USED_MEMORY;

public class MarkDistinctOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MarkDistinctOperator.class);
  private final OperatorContext operatorContext;

  private final Operator child;

  private final MarkDistinctHash markDistinctHash;
  private final int[] markDistinctChannels;

  private final MemoryReservationManager memoryReservationManager;
  // memory already occupied by MarkDistinctHash
  private long previousRetainedSize = 0;

  private final int maxResultLines =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

  public MarkDistinctOperator(
      OperatorContext operatorContext,
      Operator child,
      List<Type> types,
      List<Integer> markDistinctChannels,
      Optional<Integer> hashChannel) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.child = child;

    requireNonNull(hashChannel, "hashChannel is null");
    requireNonNull(markDistinctChannels, "markDistinctChannels is null");

    ImmutableList.Builder<Type> distinctTypes = ImmutableList.builder();
    for (int channel : markDistinctChannels) {
      distinctTypes.add(types.get(channel));
    }
    if (hashChannel.isPresent()) {
      this.markDistinctChannels = new int[markDistinctChannels.size() + 1];
      for (int i = 0; i < markDistinctChannels.size(); i++) {
        this.markDistinctChannels[i] = markDistinctChannels.get(i);
      }
      this.markDistinctChannels[markDistinctChannels.size()] = hashChannel.get();
    } else {
      this.markDistinctChannels = Ints.toArray(markDistinctChannels);
    }

    this.markDistinctHash =
        new MarkDistinctHash(distinctTypes.build(), hashChannel.isPresent(), UpdateMemory.NOOP);
    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
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

    Column[] markColumn =
        new Column[] {markDistinctHash.markDistinctRows(input.getColumns(markDistinctChannels))};
    updateOccupiedMemorySize();
    return input.appendValueColumns(markColumn);
  }

  private void updateOccupiedMemorySize() {
    long memorySize = markDistinctHash.getEstimatedSize();
    operatorContext.recordSpecifiedInfo(CURRENT_USED_MEMORY, Long.toString(memorySize));
    long delta = memorySize - previousRetainedSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousRetainedSize = memorySize;
  }

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
    return child.calculateMaxPeekMemory() + maxResultLines;
  }

  @Override
  public long calculateMaxReturnSize() {
    // all positions are non-null, so size of each position in the mask column is 1
    return child.calculateMaxReturnSize() + maxResultLines;
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
