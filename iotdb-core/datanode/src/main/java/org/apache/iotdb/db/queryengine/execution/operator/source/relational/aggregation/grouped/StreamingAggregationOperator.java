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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StreamingAggregationOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StreamingAggregationOperator.class);

  private final OperatorContext operatorContext;

  private final Operator child;

  private final List<TableAggregator> aggregators;

  private final int[] groupByChannels;

  private final TsBlockBuilder resultBuilder;
  private final ColumnBuilder[] resultColumnsBuilder;

  private boolean finished = false;

  // cached current group to judge row equality
  private SortKey currentGroup;
  private final Comparator<SortKey> groupKeyComparator;

  // We limit the size of output block, but the process of one input block may produce more than one
  // output because:
  // Input columns can be reused by multiple aggregations, so size of each row maybe larger than
  // input.
  private final Deque<TsBlock> outputs = new LinkedList<>();

  public StreamingAggregationOperator(
      OperatorContext operatorContext,
      Operator child,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      Comparator<SortKey> groupKeyComparator,
      List<TableAggregator> aggregators,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.groupByChannels = Ints.toArray(groupByChannels);
    this.groupKeyComparator = groupKeyComparator;
    this.aggregators = aggregators;
    this.resultBuilder =
        new TsBlockBuilder(
            Stream.concat(
                    groupByTypes.stream().map(InternalTypeManager::getTSDataType),
                    aggregators.stream().map(TableAggregator::getType))
                .collect(Collectors.toList()));
    this.resultColumnsBuilder = resultBuilder.getValueColumnBuilders();
    checkArgument(!spillEnabled, "spill is not supported");
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
        evaluateAndFlushGroup(currentGroup.tsBlock, currentGroup.rowIndex, true);
        currentGroup = null;
      }
      finished = true;
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
        evaluateAndFlushGroup(currentGroup.tsBlock, currentGroup.rowIndex, false);
      }
      currentGroup = null;
    }

    int startPosition = 0;
    while (true) {
      // may be equal to page.getPositionCount() if the end is not found in this page
      int nextGroupStart = findNextGroupStart(startPosition, page);
      addRowsToAggregators(page, startPosition, nextGroupStart - 1);

      if (nextGroupStart < page.getPositionCount()) {
        // current group stops somewhere in the middle of the page, so flush it
        evaluateAndFlushGroup(page, startPosition, false);
        startPosition = nextGroupStart;
      } else {
        // late materialization requires that page being locally stored is materialized before the
        // next one is fetched
        currentGroup = new SortKey(page, page.getPositionCount() - 1);
        return;
      }
    }
  }

  private void addRowsToAggregators(TsBlock page, int startPosition, int endPosition) {
    TsBlock region = page.getRegion(startPosition, endPosition - startPosition + 1);
    for (TableAggregator aggregator : aggregators) {
      aggregator.processBlock(region);
    }
  }

  private void resetAggregationBuilder() {
    for (TableAggregator aggregator : aggregators) {
      aggregator.reset();
    }
  }

  private void evaluateAndFlushGroup(TsBlock page, int position, boolean lastCalculate) {
    resultBuilder.declarePosition();
    for (int i = 0; i < groupByChannels.length; i++) {
      Column input = page.getColumn(groupByChannels[i]);
      if (input.isNull(position)) {
        resultColumnsBuilder[i].appendNull();
      } else {
        resultColumnsBuilder[i].write(input, position);
      }
    }
    int offset = groupByChannels.length;
    for (int i = 0; i < aggregators.size(); i++) {
      aggregators.get(i).evaluate(resultColumnsBuilder[offset + i]);
    }

    if (lastCalculate || resultBuilder.isFull()) {
      outputs.add(
          resultBuilder.build(
              new RunLengthEncodedColumn(
                  TableScanOperator.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount())));
      resultBuilder.reset();
    }

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
        + outputs.stream().mapToLong(TsBlock::getRetainedSizeInBytes).sum()
        + resultBuilder.getRetainedSizeInBytes();
  }
}
