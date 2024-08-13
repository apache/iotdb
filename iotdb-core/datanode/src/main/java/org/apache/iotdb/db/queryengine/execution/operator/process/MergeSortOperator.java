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
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public abstract class MergeSortOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortOperator.class);

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean[] noMoreTsBlocks;
  private final MergeSortHeap mergeSortHeap;
  private final Comparator<SortKey> comparator;

  private boolean finished;

  MergeSortOperator(
      OperatorContext operatorContext,
      List<Operator> inputOperators,
      List<TSDataType> dataTypes,
      Comparator<SortKey> comparator) {
    super(operatorContext, inputOperators);
    this.dataTypes = dataTypes;
    this.mergeSortHeap = new MergeSortHeap(inputOperatorsCount, comparator);
    this.comparator = comparator;
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    operatorContext.recordSpecifiedInfo("Merge sort branches", String.valueOf(inputOperatorsCount));
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        canCallNext[i] = true;
      } else {
        listenableFutures.add(blocked);
      }
    }
    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  @SuppressWarnings({"squid:S3776", "squid:S135"})
  @Override
  public TsBlock next() throws Exception {
    // start stopwatch
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    // 1. fill consumed up TsBlock
    if (!prepareInput()) {
      return null;
    }

    // 2. check if we can directly return the original TsBlock instead of merging way
    MergeSortKey minMergeSortKey = mergeSortHeap.poll();
    if (mergeSortHeap.isEmpty()
        || comparator.compare(
                new MergeSortKey(
                    minMergeSortKey.tsBlock, minMergeSortKey.tsBlock.getPositionCount() - 1),
                mergeSortHeap.peek())
            < 0) {
      inputTsBlocks[minMergeSortKey.inputChannelIndex] = null;
      return minMergeSortKey.rowIndex == 0
          ? minMergeSortKey.tsBlock
          : minMergeSortKey.tsBlock.subTsBlock(minMergeSortKey.rowIndex);
    }
    mergeSortHeap.push(minMergeSortKey);

    // 3. do merge sort until one TsBlock is consumed up
    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    while (!mergeSortHeap.isEmpty()) {
      MergeSortKey mergeSortKey = mergeSortHeap.poll();
      TsBlock targetBlock = mergeSortKey.tsBlock;
      int rowIndex = mergeSortKey.rowIndex;
      appendTime(timeBuilder, targetBlock.getTimeByIndex(rowIndex));
      for (int i = 0; i < valueColumnBuilders.length; i++) {
        if (targetBlock.getColumn(i).isNull(rowIndex)) {
          valueColumnBuilders[i].appendNull();
          continue;
        }
        valueColumnBuilders[i].write(targetBlock.getColumn(i), rowIndex);
      }
      tsBlockBuilder.declarePosition();
      if (mergeSortKey.rowIndex == mergeSortKey.tsBlock.getPositionCount() - 1) {
        inputTsBlocks[mergeSortKey.inputChannelIndex] = null;
        break;
      } else {
        mergeSortKey.rowIndex++;
        mergeSortHeap.push(mergeSortKey);
      }
      // break if time is out or tsBlockBuilder is full
      if (System.nanoTime() - startTime > maxRuntime || tsBlockBuilder.isFull()) {
        break;
      }
    }
    return buildResult(tsBlockBuilder);
  }

  protected abstract void appendTime(TimeColumnBuilder timeBuilder, long time);

  protected abstract TsBlock buildResult(TsBlockBuilder resultBuilder);

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
          return true;
        } else {
          children.get(i).close();
          children.set(i, null);
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || !isEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // MergeToolKit will cache startKey and endKey
    long maxPeekMemory = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    // inputTsBlocks will cache all the tsBlocks returned by inputOperators
    for (Operator operator : children) {
      maxPeekMemory += operator.calculateMaxReturnSize();
      maxPeekMemory += operator.calculateRetainedSizeAfterCallingNext();
    }
    for (Operator operator : children) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemoryWithCounter());
    }
    return Math.max(maxPeekMemory, calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return (1L + dataTypes.size()) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0;
    long minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
    }
    return currentRetainedSize - minChildReturnSize;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(canCallNext)
        + RamUsageEstimator.sizeOf(noMoreTsBlocks)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }

  // region helper function used in prepareInput

  /**
   * @param currentChildIndex the index of the child
   * @return true if we can skip the currentChild in prepareInput
   */
  @Override
  protected boolean canSkipCurrentChild(int currentChildIndex) {
    return noMoreTsBlocks[currentChildIndex]
        || !isEmpty(currentChildIndex)
        || children.get(currentChildIndex) == null;
  }

  /**
   * @param currentInputIndex index of the input TsBlock
   */
  @Override
  protected void processCurrentInputTsBlock(int currentInputIndex) {
    mergeSortHeap.push(new MergeSortKey(inputTsBlocks[currentInputIndex], 0, currentInputIndex));
  }

  /**
   * @param currentChildIndex the index of the child
   * @throws Exception Potential Exception thrown by Operator.close()
   */
  @Override
  protected void handleFinishedChild(int currentChildIndex) throws Exception {
    noMoreTsBlocks[currentChildIndex] = true;
    inputTsBlocks[currentChildIndex] = null;
    children.get(currentChildIndex).close();
    children.set(currentChildIndex, null);
  }

  // endregion
}
