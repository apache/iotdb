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
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.LogicalIndexNavigation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternPartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternVariableRecognizer;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.PatternExpressionComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Matcher;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SkipToPosition;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_RESERVED_MEMORY;

public class PatternRecognitionOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PatternRecognitionOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;
  private final List<TSDataType> inputDataTypes;
  private final List<Integer> outputChannels;
  private final TsBlockBuilder tsBlockBuilder;
  private final MemoryReservationManager memoryReservationManager;

  // Partition
  private final List<Integer> partitionChannels;
  private final RowComparator partitionComparator;
  private final List<TsBlock> cachedTsBlocks;
  private int startIndexInFirstBlock;

  // Sort
  private final List<Integer> sortChannels;

  // Transformation
  private LinkedList<PatternPartitionExecutor> cachedPartitionExecutors;

  // Pattern Recognition
  private final RowsPerMatch rowsPerMatch;
  private final SkipToPosition skipToPosition;
  private final Optional<LogicalIndexNavigation> skipToNavigation;
  private final Matcher matcher;
  private final List<PatternVariableRecognizer.PatternVariableComputation>
      labelPatternVariableComputations;
  private final List<PatternAggregator> patternAggregators;
  private final List<PatternExpressionComputation> measureComputations;
  private final List<String> labelNames;

  private long totalMemorySize;
  private long maxUsedMemory;
  private final long maxRuntime;

  public PatternRecognitionOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      List<Integer> outputChannels,
      List<Integer> partitionChannels,
      List<Integer> sortChannels,
      RowsPerMatch rowsPerMatch,
      SkipToPosition skipToPosition,
      Optional<LogicalIndexNavigation> skipToNavigation,
      Matcher matcher,
      List<PatternVariableRecognizer.PatternVariableComputation> labelPatternVariableComputations,
      List<PatternAggregator> patternAggregators,
      List<PatternExpressionComputation> measureComputations,
      List<String> labelNames) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.outputChannels = ImmutableList.copyOf(outputChannels);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    // Partition Part
    this.partitionChannels = ImmutableList.copyOf(partitionChannels);
    // Acquire partition channels' data types
    List<TSDataType> partitionDataTypes = new ArrayList<>();
    for (Integer channel : partitionChannels) {
      partitionDataTypes.add(inputDataTypes.get(channel));
    }
    this.partitionComparator = new RowComparator(partitionDataTypes);

    // Sort Part
    this.sortChannels = ImmutableList.copyOf(sortChannels);

    // Transformation part
    this.cachedPartitionExecutors = new LinkedList<>();

    // Pattern Recognition part
    this.rowsPerMatch = rowsPerMatch;
    this.skipToPosition = skipToPosition;
    this.skipToNavigation = skipToNavigation;
    this.matcher = matcher;
    this.labelPatternVariableComputations = ImmutableList.copyOf(labelPatternVariableComputations);
    this.patternAggregators = ImmutableList.copyOf(patternAggregators);
    this.measureComputations = ImmutableList.copyOf(measureComputations);
    this.labelNames = ImmutableList.copyOf(labelNames);

    this.cachedTsBlocks = new ArrayList<>();
    this.startIndexInFirstBlock = -1;

    this.maxRuntime = this.operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    this.totalMemorySize = 0;
    this.maxUsedMemory = 0;
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
    long startTime = System.nanoTime();

    // Transform is not finished
    if (!cachedPartitionExecutors.isEmpty()) {
      TsBlock tsBlock = transform(startTime);
      if (tsBlock != null) {
        return tsBlock;
      }
      // Receive more data when result TsBlock builder is not full
      // In this case, all partition executors are done
    }

    if (child.hasNextWithTimer()) {
      // This TsBlock is pre-sorted with PARTITION BY and ORDER BY channels
      TsBlock preSortedBlock = child.nextWithTimer();
      // StreamSort Operator sometimes returns null
      if (preSortedBlock == null || preSortedBlock.isEmpty()) {
        return null;
      }

      cachedPartitionExecutors = partition(preSortedBlock);
      if (cachedPartitionExecutors.isEmpty()) {
        // No partition found
        // i.e., partition crosses multiple TsBlocks
        return null;
      }

      // May return null if builder is not full
      return transform(startTime);
    } else if (!cachedTsBlocks.isEmpty()) {
      // Form last partition
      TsBlock lastTsBlock = cachedTsBlocks.get(cachedTsBlocks.size() - 1);
      int endIndexOfLastTsBlock = lastTsBlock.getPositionCount();
      PatternPartitionExecutor partitionExecutor =
          new PatternPartitionExecutor(
              cachedTsBlocks,
              inputDataTypes,
              startIndexInFirstBlock,
              endIndexOfLastTsBlock,
              outputChannels,
              sortChannels,
              rowsPerMatch,
              skipToPosition,
              skipToNavigation,
              matcher,
              labelPatternVariableComputations,
              patternAggregators,
              measureComputations,
              labelNames);
      cachedPartitionExecutors.addLast(partitionExecutor);
      cachedTsBlocks.clear();
      releaseAllCachedTsBlockMemory();

      TsBlock tsBlock = transform(startTime);
      if (tsBlock == null) {
        // TsBlockBuilder is not full
        // Force build since this is the last partition
        tsBlock =
            tsBlockBuilder.build(
                new RunLengthEncodedColumn(
                    TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
        tsBlockBuilder.reset();
      }

      return tsBlock;
    } else if (!tsBlockBuilder.isEmpty()) {
      // Return remaining data in result TsBlockBuilder
      // This happens when last partition is too large
      // And TsBlockBuilder is not full at the end of transform
      return getTsBlockFromTsBlockBuilder();
    }

    return null;
  }

  private LinkedList<PatternPartitionExecutor> partition(TsBlock tsBlock) {
    LinkedList<PatternPartitionExecutor> partitionExecutors = new LinkedList<>();

    int partitionStartInCurrentBlock = 0;
    int partitionEndInCurrentBlock = partitionStartInCurrentBlock + 1;

    // In this stage, we only consider partition channels
    List<Column> partitionColumns = extractPartitionColumns(tsBlock);

    // Previous TsBlocks forms a partition
    if (!cachedTsBlocks.isEmpty()) {
      TsBlock lastTsBlock = cachedTsBlocks.get(cachedTsBlocks.size() - 1);
      int endIndexOfLastTsBlock = lastTsBlock.getPositionCount();

      // Whether the first row of current TsBlock is not equal to
      // last row of previous cached TsBlocks
      List<Column> lastPartitionColumns = extractPartitionColumns(lastTsBlock);
      if (!partitionComparator.equal(
          partitionColumns, 0, lastPartitionColumns, endIndexOfLastTsBlock - 1)) {

        PatternPartitionExecutor partitionExecutor =
            new PatternPartitionExecutor(
                cachedTsBlocks,
                inputDataTypes,
                startIndexInFirstBlock,
                endIndexOfLastTsBlock,
                outputChannels,
                sortChannels,
                rowsPerMatch,
                skipToPosition,
                skipToNavigation,
                matcher,
                labelPatternVariableComputations,
                patternAggregators,
                measureComputations,
                labelNames);

        partitionExecutors.addLast(partitionExecutor);
        cachedTsBlocks.clear();
        releaseAllCachedTsBlockMemory();
        startIndexInFirstBlock = -1;
      }
    }

    // Try to find all partitions
    int count = tsBlock.getPositionCount();
    while (count == 1 || partitionEndInCurrentBlock < count) {
      // Try to find one partition
      while (partitionEndInCurrentBlock < count
          && partitionComparator.equalColumns(
              partitionColumns, partitionStartInCurrentBlock, partitionEndInCurrentBlock)) {
        partitionEndInCurrentBlock++;
      }

      if (partitionEndInCurrentBlock != count) {
        // Find partition
        PatternPartitionExecutor partitionExecutor;
        if (partitionStartInCurrentBlock != 0 || startIndexInFirstBlock == -1) {
          // Small partition within this TsBlock
          partitionExecutor =
              new PatternPartitionExecutor(
                  Collections.singletonList(tsBlock),
                  inputDataTypes,
                  partitionStartInCurrentBlock,
                  partitionEndInCurrentBlock,
                  outputChannels,
                  sortChannels,
                  rowsPerMatch,
                  skipToPosition,
                  skipToNavigation,
                  matcher,
                  labelPatternVariableComputations,
                  patternAggregators,
                  measureComputations,
                  labelNames);
        } else {
          // Large partition crosses multiple TsBlocks
          reserveOneTsBlockMemory(tsBlock);
          cachedTsBlocks.add(tsBlock);
          partitionExecutor =
              new PatternPartitionExecutor(
                  cachedTsBlocks,
                  inputDataTypes,
                  startIndexInFirstBlock,
                  partitionEndInCurrentBlock,
                  outputChannels,
                  sortChannels,
                  rowsPerMatch,
                  skipToPosition,
                  skipToNavigation,
                  matcher,
                  labelPatternVariableComputations,
                  patternAggregators,
                  measureComputations,
                  labelNames);
          // Clear TsBlock of last partition
          cachedTsBlocks.clear();
          releaseAllCachedTsBlockMemory();
        }
        partitionExecutors.addLast(partitionExecutor);

        partitionStartInCurrentBlock = partitionEndInCurrentBlock;
        partitionEndInCurrentBlock = partitionStartInCurrentBlock + 1;
      } else {
        // Last partition of TsBlock
        // The beginning of next TsBlock may have rows in this partition
        if (startIndexInFirstBlock == -1) {
          startIndexInFirstBlock = partitionStartInCurrentBlock;
        }
        reserveOneTsBlockMemory(tsBlock);
        cachedTsBlocks.add(tsBlock);
        // For count == 1
        break;
      }
    }

    return partitionExecutors;
  }

  private TsBlock transform(long startTime) {
    while (!cachedPartitionExecutors.isEmpty()) {
      PatternPartitionExecutor partitionExecutor = cachedPartitionExecutors.getFirst();

      while (System.nanoTime() - startTime < maxRuntime
          && !tsBlockBuilder.isFull()
          && partitionExecutor.hasNext()) {
        partitionExecutor.processNextRow(tsBlockBuilder);
      }

      if (!partitionExecutor.hasNext()) {
        cachedPartitionExecutors.removeFirst();
      }

      if (System.nanoTime() - startTime >= maxRuntime || tsBlockBuilder.isFull()) {
        return getTsBlockFromTsBlockBuilder();
      }
    }

    // Reach partition end, but builder is not full yet
    return null;
  }

  private List<Column> extractPartitionColumns(TsBlock tsBlock) {
    List<Column> partitionColumns = new ArrayList<>(partitionChannels.size());
    for (int channel : partitionChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      partitionColumns.add(partitionColumn);
    }
    return partitionColumns;
  }

  private TsBlock getTsBlockFromTsBlockBuilder() {
    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    tsBlockBuilder.reset();
    return result;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !cachedPartitionExecutors.isEmpty()
        || child.hasNext()
        || !cachedTsBlocks.isEmpty()
        || !tsBlockBuilder.isEmpty();
  }

  @Override
  public void close() throws Exception {
    child.close();
    if (totalMemorySize != 0) {
      memoryReservationManager.releaseMemoryCumulatively(totalMemorySize);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  private void reserveOneTsBlockMemory(TsBlock tsBlock) {
    long reserved = tsBlock.getTotalInstanceSize();
    memoryReservationManager.reserveMemoryCumulatively(reserved);
    totalMemorySize += reserved;
    maxUsedMemory = Math.max(maxUsedMemory, totalMemorySize);
    operatorContext.recordSpecifiedInfo(MAX_RESERVED_MEMORY, Long.toString(maxUsedMemory));
  }

  private void releaseAllCachedTsBlockMemory() {
    long released = cachedTsBlocks.stream().mapToInt(TsBlock::getTotalInstanceSize).sum();
    memoryReservationManager.releaseMemoryCumulatively(released);
    totalMemorySize -= released;
    // No need to update maxUsedMemory
    operatorContext.recordSpecifiedInfo(MAX_RESERVED_MEMORY, Long.toString(maxUsedMemory));
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = child.calculateMaxPeekMemoryWithCounter();
    long maxPeekMemoryFromCurrent =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxPeekMemoryFromCurrent)
        + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
