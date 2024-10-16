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
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.builder.HashAggregationBuilder;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.builder.InMemoryHashAggregationBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory.NOOP;

public class HashAggregationOperator implements ProcessOperator {
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

  private final TsBlockBuilder resultBuilder;

  private final ColumnBuilder[] resultColumnsBuilder;

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  protected MemoryReservationManager memoryReservationManager;

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
    this.resultBuilder =
        new TsBlockBuilder(
            Stream.concat(
                    groupByTypes.stream().map(InternalTypeManager::getTSDataType),
                    aggregators.stream().map(GroupedAggregator::getType))
                .collect(Collectors.toList()));
    this.resultColumnsBuilder = resultBuilder.getValueColumnBuilders();
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
    return !finished;
  }

  @Override
  public TsBlock next() throws Exception {
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
      return null;
    } else {
      // evaluate output
      return getOutput();
    }
  }

  private TsBlock getOutput() {
    // only flush if we are finishing or the aggregation builder is full
    if ((aggregationBuilder == null || !aggregationBuilder.isFull())) {
      return null;
    }

    TsBlock result = aggregationBuilder.buildResult();

    closeAggregationBuilder();
    finished = true;
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
    return finished;
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
        + aggregators.stream().mapToLong(GroupedAggregator::getEstimatedSize).count()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes();
  }
}
