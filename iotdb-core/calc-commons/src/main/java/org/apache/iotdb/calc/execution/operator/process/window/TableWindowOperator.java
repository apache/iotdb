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

package org.apache.iotdb.calc.execution.operator.process.window;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.ProcessOperator;
import org.apache.iotdb.calc.execution.operator.process.function.PartitionRecognizer;
import org.apache.iotdb.calc.execution.operator.process.function.partition.PartitionCache;
import org.apache.iotdb.calc.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.calc.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.calc.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.calc.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.calc.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.calc.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
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

import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.MAX_RESERVED_MEMORY;

public class TableWindowOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableWindowOperator.class);

  // Common fields
  private final CommonOperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final List<Integer> outputChannels;
  private final TsBlockBuilder tsBlockBuilder;
  private final MemoryReservationManager memoryReservationManager;

  // Basic information about window operator
  private final List<WindowFunction> windowFunctions;
  private final List<FrameInfo> frameInfoList;

  // Partition
  private final PartitionRecognizer partitionRecognizer;
  private final PartitionCache partitionCache;

  // Sort
  private final List<Integer> sortChannels;

  // Transformation
  private LinkedList<PartitionExecutor> cachedPartitionExecutors;

  // Misc
  private long totalMemorySize;
  private long maxUsedMemory;
  private final long maxRuntime;
  private boolean noMoreDataSignaled;

  public TableWindowOperator(
      CommonOperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      List<Integer> outputChannels,
      List<WindowFunction> windowFunctions,
      List<FrameInfo> frameInfoList,
      List<Integer> partitionChannels,
      List<Integer> sortChannels) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.outputChannels = ImmutableList.copyOf(outputChannels);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    this.windowFunctions = ImmutableList.copyOf(windowFunctions);
    this.frameInfoList = ImmutableList.copyOf(frameInfoList);

    List<Integer> requiredChannels = new ArrayList<>(inputDataTypes.size());
    for (int i = 0; i < inputDataTypes.size(); i++) {
      requiredChannels.add(i);
    }
    this.partitionRecognizer =
        new PartitionRecognizer(
            partitionChannels, requiredChannels, Collections.emptyList(), inputDataTypes);
    this.partitionCache = new PartitionCache();

    this.sortChannels = ImmutableList.copyOf(sortChannels);

    this.cachedPartitionExecutors = new LinkedList<>();

    this.maxRuntime = this.operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    this.totalMemorySize = 0;
    this.maxUsedMemory = 0;
    this.noMoreDataSignaled = false;
    this.memoryReservationManager = operatorContext.getMemoryReservationContext();
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();

    if (!cachedPartitionExecutors.isEmpty()) {
      TsBlock tsBlock = transform(startTime);
      if (tsBlock != null) {
        return tsBlock;
      }
    }

    if (inputOperator.hasNextWithTimer()) {
      TsBlock preSortedBlock = inputOperator.nextWithTimer();
      if (preSortedBlock == null || preSortedBlock.isEmpty()) {
        return null;
      }

      partitionRecognizer.addTsBlock(preSortedBlock);
      processRecognizerStates();

      if (!cachedPartitionExecutors.isEmpty()) {
        return transform(startTime);
      }
      return null;
    } else if (!noMoreDataSignaled) {
      partitionRecognizer.noMoreData();
      noMoreDataSignaled = true;
      processRecognizerStates();

      if (!cachedPartitionExecutors.isEmpty()) {
        TsBlock tsBlock = transform(startTime);
        if (tsBlock == null && !tsBlockBuilder.isEmpty()) {
          tsBlock = getTsBlockFromTsBlockBuilder();
        }
        return tsBlock;
      }
    } else if (!tsBlockBuilder.isEmpty()) {
      return getTsBlockFromTsBlockBuilder();
    }

    return null;
  }

  private void processRecognizerStates() {
    while (true) {
      PartitionState state = partitionRecognizer.nextState();
      switch (state.getStateType()) {
        case INIT:
        case NEED_MORE_DATA:
          return;
        case FINISHED:
          finalizeCurrentPartition();
          return;
        case NEW_PARTITION:
          finalizeCurrentPartition();
          addSliceToCache(state.getSlice());
          break;
        case ITERATING:
          addSliceToCache(state.getSlice());
          break;
        default:
          break;
      }
    }
  }

  private void finalizeCurrentPartition() {
    if (!partitionCache.isEmpty()) {
      Partition partition = new Partition(partitionCache.getSlices());
      PartitionExecutor partitionExecutor =
          new PartitionExecutor(
              partition,
              inputDataTypes,
              outputChannels,
              windowFunctions,
              frameInfoList,
              sortChannels);
      cachedPartitionExecutors.addLast(partitionExecutor);
      releasePartitionCacheMemory();
      partitionCache.clear();
    }
  }

  private TsBlock transform(long startTime) {
    while (!cachedPartitionExecutors.isEmpty()) {
      PartitionExecutor partitionExecutor = cachedPartitionExecutors.getFirst();
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

    return null;
  }

  private TsBlock getTsBlockFromTsBlockBuilder() {
    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(
                CommonOperatorUtils.TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    tsBlockBuilder.reset();
    return result;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !cachedPartitionExecutors.isEmpty()
        || inputOperator.hasNext()
        || !partitionCache.isEmpty()
        || !tsBlockBuilder.isEmpty();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
    partitionCache.close();
    if (totalMemorySize != 0) {
      memoryReservationManager.releaseMemoryCumulatively(totalMemorySize);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  private void addSliceToCache(Slice slice) {
    long reserved = slice.getEstimatedSize();
    memoryReservationManager.reserveMemoryCumulatively(reserved);
    totalMemorySize += reserved;
    maxUsedMemory = Math.max(maxUsedMemory, totalMemorySize);
    operatorContext.recordSpecifiedInfo(MAX_RESERVED_MEMORY, Long.toString(maxUsedMemory));
    partitionCache.addSlice(slice);
  }

  private void releasePartitionCacheMemory() {
    long released = partitionCache.getEstimatedSize();
    memoryReservationManager.releaseMemoryCumulatively(released);
    totalMemorySize -= released;
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
        + tsBlockBuilder.getRetainedSizeInBytes()
        + partitionCache.getEstimatedSize();
  }
}
