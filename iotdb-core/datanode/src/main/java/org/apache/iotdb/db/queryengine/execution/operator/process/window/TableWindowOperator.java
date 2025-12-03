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

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

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
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.MAX_RESERVED_MEMORY;

public class TableWindowOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableWindowOperator.class);

  // Common fields
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final List<Integer> outputChannels;
  private final TsBlockBuilder tsBlockBuilder;
  private final MemoryReservationManager memoryReservationManager;

  // Basic information about window operator
  private final List<WindowFunction> windowFunctions;
  private final List<FrameInfo> frameInfoList;

  // Partition
  private final List<Integer> partitionChannels;
  private final RowComparator partitionComparator;
  private final List<TsBlock> cachedTsBlocks;
  private int startIndexInFirstBlock;

  // Sort
  private final List<Integer> sortChannels;

  // Transformation
  private LinkedList<PartitionExecutor> cachedPartitionExecutors;

  // Misc
  private long totalMemorySize;
  private long maxUsedMemory;
  private final long maxRuntime;

  public TableWindowOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      List<Integer> outputChannels,
      List<WindowFunction> windowFunctions,
      List<FrameInfo> frameInfoList,
      List<Integer> partitionChannels,
      List<Integer> sortChannels) {
    // Common part(among all other operators)
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.outputChannels = ImmutableList.copyOf(outputChannels);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    // Basic information part
    this.windowFunctions = ImmutableList.copyOf(windowFunctions);
    this.frameInfoList = ImmutableList.copyOf(frameInfoList);

    // Partition Part
    this.partitionChannels = ImmutableList.copyOf(partitionChannels);
    // Acquire partition channels' data types
    List<TSDataType> partitionDataTypes = new ArrayList<>();
    for (Integer channel : partitionChannels) {
      partitionDataTypes.add(inputDataTypes.get(channel));
    }
    this.partitionComparator = new RowComparator(partitionDataTypes);

    // Ordering part
    this.sortChannels = ImmutableList.copyOf(sortChannels);

    // Transformation part
    this.cachedPartitionExecutors = new LinkedList<>();

    // Misc
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
    return inputOperator.isBlocked();
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

    if (inputOperator.hasNextWithTimer()) {
      // This TsBlock is pre-sorted with PARTITION BY and ORDER BY channels
      TsBlock preSortedBlock = inputOperator.nextWithTimer();
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
      PartitionExecutor partitionExecutor =
          new PartitionExecutor(
              cachedTsBlocks,
              inputDataTypes,
              startIndexInFirstBlock,
              endIndexOfLastTsBlock,
              outputChannels,
              windowFunctions,
              frameInfoList,
              sortChannels);
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

  private LinkedList<PartitionExecutor> partition(TsBlock tsBlock) {
    LinkedList<PartitionExecutor> partitionExecutors = new LinkedList<>();

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
        PartitionExecutor partitionExecutor =
            new PartitionExecutor(
                cachedTsBlocks,
                inputDataTypes,
                startIndexInFirstBlock,
                endIndexOfLastTsBlock,
                outputChannels,
                windowFunctions,
                frameInfoList,
                sortChannels);

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
        PartitionExecutor partitionExecutor;
        if (partitionStartInCurrentBlock != 0 || startIndexInFirstBlock == -1) {
          // Small partition within this TsBlock
          partitionExecutor =
              new PartitionExecutor(
                  Collections.singletonList(tsBlock),
                  inputDataTypes,
                  partitionStartInCurrentBlock,
                  partitionEndInCurrentBlock,
                  outputChannels,
                  windowFunctions,
                  frameInfoList,
                  sortChannels);
        } else {
          // Large partition crosses multiple TsBlocks
          reserveOneTsBlockMemory(tsBlock);
          cachedTsBlocks.add(tsBlock);
          partitionExecutor =
              new PartitionExecutor(
                  cachedTsBlocks,
                  inputDataTypes,
                  startIndexInFirstBlock,
                  partitionEndInCurrentBlock,
                  outputChannels,
                  windowFunctions,
                  frameInfoList,
                  sortChannels);
          // Clear TsBlock of last partition
          cachedTsBlocks.clear();
          releaseAllCachedTsBlockMemory();
        }
        partitionExecutors.addLast(partitionExecutor);

        partitionStartInCurrentBlock = partitionEndInCurrentBlock;
        // Reset cross-TsBlock tracking after partition completion
        startIndexInFirstBlock = -1;
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
      PartitionExecutor partitionExecutor = cachedPartitionExecutors.getFirst();
      // Reset window functions for new partition
      partitionExecutor.resetWindowFunctions();

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
        || inputOperator.hasNext()
        || !cachedTsBlocks.isEmpty()
        || !tsBlockBuilder.isEmpty();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
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
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    long maxPeekMemoryFromCurrent =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxPeekMemoryFromCurrent)
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
