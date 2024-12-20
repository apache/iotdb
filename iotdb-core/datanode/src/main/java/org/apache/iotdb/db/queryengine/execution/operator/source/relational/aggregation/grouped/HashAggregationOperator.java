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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.AbstractOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.builder.HashAggregationBuilder;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.builder.InMemoryHashAggregationBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory.NOOP;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.CURRENT_USED_MEMORY;

public class HashAggregationOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HashAggregationOperator.class);

  private final OperatorContext operatorContext;

  private final Operator child;

  private final List<Type> groupByTypes;
  private final List<Integer> groupByChannels;

  private final List<GroupedAggregator> aggregators;
  private final AggregationNode.Step step;

  private final int expectedGroups;

  private final long maxPartialMemory;

  private final boolean spillEnabled;
  private final long unspillMemoryLimit;

  private HashAggregationBuilder aggregationBuilder;

  private final MemoryReservationManager memoryReservationManager;
  // memory already occupied by aggregationBuilder
  private long previousRetainedSize = 0;

  private boolean finished = false;

  public HashAggregationOperator(
      OperatorContext operatorContext,
      Operator child,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      List<GroupedAggregator> aggregators,
      AggregationNode.Step step,
      int expectedGroups,
      long maxPartialMemory,
      boolean spillEnabled,
      long unspillMemoryLimit) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.groupByTypes = ImmutableList.copyOf(groupByTypes);
    this.groupByChannels = ImmutableList.copyOf(groupByChannels);
    this.aggregators = aggregators;
    this.step = step;
    this.expectedGroups = expectedGroups;
    this.maxPartialMemory = maxPartialMemory;
    this.spillEnabled = spillEnabled;
    this.unspillMemoryLimit = unspillMemoryLimit;
    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished || retainedTsBlock != null;
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    if (aggregationBuilder == null) {
      if (spillEnabled) {
        throw new UnsupportedOperationException();
      } else {
        aggregationBuilder =
            new InMemoryHashAggregationBuilder(
                aggregators,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                Optional.empty(),
                operatorContext,
                maxPartialMemory,
                NOOP);
      }
      updateOccupiedMemorySize();
    } else {
      checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
    }

    // Each call only calculate at most once, no need to check time slice.
    TsBlock block;
    if (child.hasNextWithTimer()) {
      block = child.nextWithTimer();
      if (block == null) {
        return null;
      }

      aggregationBuilder.processBlock(block);
      aggregationBuilder.updateMemory();
      updateOccupiedMemorySize();
      return null;
    } else {
      // evaluate output
      resultTsBlock = getOutput();
      return checkTsBlockSizeAndGetResult();
    }
  }

  private void updateOccupiedMemorySize() {
    long memorySize = aggregationBuilder.getEstimatedSize();
    operatorContext.recordSpecifiedInfo(CURRENT_USED_MEMORY, Long.toString(memorySize));
    long delta = memorySize - previousRetainedSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousRetainedSize = memorySize;
  }

  private TsBlock getOutput() {
    checkState(aggregationBuilder != null);

    TsBlock result = aggregationBuilder.buildResult();

    if (aggregationBuilder.finished()) {
      closeAggregationBuilder();
      finished = true;
    }
    return result;
  }

  private void closeAggregationBuilder() {
    // outputPages = null;
    if (aggregationBuilder != null) {
      aggregationBuilder.close();
      // aggregationBuilder.close() will release all memory reserved in memory accounting.
      // The reference must be set to null afterwards to avoid unaccounted memory.
      aggregationBuilder = null;
    }
    // memoryContext.setBytes(0);
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished && retainedTsBlock == null;
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        child.calculateMaxPeekMemoryWithCounter(),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
