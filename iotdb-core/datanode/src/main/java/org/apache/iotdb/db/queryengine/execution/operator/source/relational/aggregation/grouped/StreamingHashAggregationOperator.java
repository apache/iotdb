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
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory.NOOP;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.CURRENT_USED_MEMORY;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_USED_MEMORY;

public class StreamingHashAggregationOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StreamingHashAggregationOperator.class);

  private final OperatorContext operatorContext;

  private final Operator child;

  private final int[] preGroupedChannels;

  // used for build result
  private final int[] preGroupedIndexInResult;
  private final int[] unPreGroupedIndexInResult;

  private final int valueColumnsCount;
  private final int resultColumnsCount;

  private HashAggregationBuilder aggregationBuilder;

  private final MemoryReservationManager memoryReservationManager;
  // memory already occupied by aggregationBuilder
  private long previousRetainedSize = 0;

  private boolean finished = false;

  // cached current group to judge row equality and construct preGroupedColumn in result
  private SortKey currentGroup;
  private final Comparator<SortKey> groupKeyComparator;

  // We limit the size of output block, but the process of one input block may produce more than one
  // output because:
  // 1. Input columns can be reused by multiple aggregations, so size of each row maybe larger than
  // input;
  // 2. There are many input has been added into HashAggregationBuilder before result produced of
  // this process, it may produce many rows which number is larger than limit.
  private final Deque<TsBlock> outputs = new LinkedList<>();

  private long maxUsedMemory;

  public StreamingHashAggregationOperator(
      OperatorContext operatorContext,
      Operator child,
      List<Integer> preGroupedChannels,
      List<Integer> preGroupedIndexInResult,
      List<Type> unPreGroupedTypes,
      List<Integer> unPreGroupedChannels,
      List<Integer> unPreGroupedIndexInResult,
      Comparator<SortKey> groupKeyComparator,
      List<GroupedAggregator> aggregators,
      AggregationNode.Step step,
      int expectedGroups,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    this.operatorContext = operatorContext;
    this.child = child;

    this.preGroupedChannels = Ints.toArray(preGroupedChannels);
    this.preGroupedIndexInResult = Ints.toArray(preGroupedIndexInResult);
    this.unPreGroupedIndexInResult = Ints.toArray(unPreGroupedIndexInResult);

    this.groupKeyComparator = groupKeyComparator;

    this.valueColumnsCount = aggregators.size();
    this.resultColumnsCount =
        this.preGroupedIndexInResult.length
            + this.unPreGroupedIndexInResult.length
            + aggregators.size();
    checkArgument(!spillEnabled, "spill is not supported");
    aggregationBuilder =
        new InMemoryHashAggregationBuilder(
            aggregators,
            step,
            expectedGroups,
            unPreGroupedTypes,
            unPreGroupedChannels,
            Optional.empty(),
            operatorContext,
            maxPartialMemory,
            NOOP);
    this.memoryReservationManager =
        operatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getMemoryReservationContext();
    updateOccupiedMemorySize();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished || retainedTsBlock != null || !outputs.isEmpty();
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    if (!outputs.isEmpty()) {
      resultTsBlock = outputs.removeFirst();
      return checkTsBlockSizeAndGetResult();
    }

    TsBlock block;
    if (child.hasNextWithTimer()) {
      block = child.nextWithTimer();
      if (block == null) {
        return null;
      }

      processInput(block);
    } else {
      // last evaluate
      if (currentGroup != null) {
        evaluateAndFlushGroup(currentGroup.tsBlock, currentGroup.rowIndex);
        currentGroup = null;
      }
      finished = true;
      closeAggregationBuilder();
    }

    if (outputs.isEmpty()) {
      return null;
    }

    resultTsBlock = outputs.removeFirst();
    return checkTsBlockSizeAndGetResult();
  }

  private void processInput(TsBlock page) {
    requireNonNull(page, "page is null");

    if (currentGroup != null) {
      if (groupKeyComparator.compare(currentGroup, new SortKey(page, 0)) != 0) {
        // page starts with new group, so flush it
        evaluateAndFlushGroup(currentGroup.tsBlock, currentGroup.rowIndex);
      }
      currentGroup = null;
    }

    int startPosition = 0;
    while (true) {
      // may be equal to page.getPositionCount() if the end is not found in this page
      int nextGroupStart = findNextGroupStart(startPosition, page);
      addRowsToAggregationBuilder(page, startPosition, nextGroupStart - 1);

      if (nextGroupStart < page.getPositionCount()) {
        // current group stops somewhere in the middle of the page, so flush it
        evaluateAndFlushGroup(page, startPosition);
        startPosition = nextGroupStart;
      } else {
        // late materialization requires that page being locally stored is materialized before the
        // next one is fetched
        currentGroup = new SortKey(page, page.getPositionCount() - 1);
        return;
      }
    }
  }

  private void addRowsToAggregationBuilder(TsBlock page, int startPosition, int endPosition) {
    TsBlock region = page.getRegion(startPosition, endPosition - startPosition + 1);
    aggregationBuilder.processBlock(region);
    updateOccupiedMemorySize();
  }

  private void resetAggregationBuilder() {
    aggregationBuilder.reset();
    updateOccupiedMemorySize();
  }

  private void evaluateAndFlushGroup(TsBlock page, int position) {
    Column[] result = new Column[resultColumnsCount];
    // offset of value columns index
    int offset = preGroupedIndexInResult.length + unPreGroupedIndexInResult.length;

    do {
      // contains unPreGrouped group by columns and value columns
      TsBlock buildResult = aggregationBuilder.buildResult();

      // preGrouped Columns
      for (int i = 0; i < preGroupedIndexInResult.length; i++) {
        Column column = page.getColumn(preGroupedChannels[i]).getRegion(position, 1);
        result[preGroupedIndexInResult[i]] =
            new RunLengthEncodedColumn(column, buildResult.getPositionCount());
      }

      // unPreGrouped Columns
      for (int i = 0; i < unPreGroupedIndexInResult.length; i++) {
        result[unPreGroupedIndexInResult[i]] = buildResult.getColumn(i);
      }

      // value Columns
      for (int i = 0; i < valueColumnsCount; i++) {
        result[offset + i] = buildResult.getColumn(i + unPreGroupedIndexInResult.length);
      }

      outputs.add(
          TsBlock.wrapBlocksWithoutCopy(
              buildResult.getPositionCount(), buildResult.getTimeColumn(), result));
    } while (!aggregationBuilder.finished());

    resetAggregationBuilder();
  }

  private int findNextGroupStart(int startPosition, TsBlock page) {
    int positionCount = page.getPositionCount();
    for (int i = startPosition + 1; i < positionCount; i++) {
      if (groupKeyComparator.compare(new SortKey(page, startPosition), new SortKey(page, i)) != 0) {
        return i;
      }
    }

    return positionCount;
  }

  private void updateOccupiedMemorySize() {
    long memorySize = aggregationBuilder.getEstimatedSize();
    operatorContext.recordSpecifiedInfo(CURRENT_USED_MEMORY, Long.toString(memorySize));
    if (memorySize > maxUsedMemory) {
      operatorContext.recordSpecifiedInfo(MAX_USED_MEMORY, Long.toString(memorySize));
      maxUsedMemory = memorySize;
    }
    long delta = memorySize - previousRetainedSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousRetainedSize = memorySize;
  }

  private void closeAggregationBuilder() {
    // outputPages = null;
    if (aggregationBuilder != null) {
      aggregationBuilder.close();
      // aggregationBuilder.close() will release all memory reserved in memory accounting.
      // The reference must be set to null afterward to avoid unaccounted memory.
      aggregationBuilder = null;
    }
    // memoryContext.setBytes(0);
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished && retainedTsBlock == null && outputs.isEmpty();
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
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + outputs.stream().mapToLong(TsBlock::getRetainedSizeInBytes).sum();
  }
}
