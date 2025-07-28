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
import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class AggregationMergeSortOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AggregationMergeSortOperator.class);

  private final List<Accumulator> accumulators;

  private final List<TSDataType> dataTypes;

  private final TsBlockBuilder tsBlockBuilder;

  private final boolean[] noMoreTsBlocks;

  private final MergeSortHeap mergeSortHeap;

  private final boolean hasGroupBy;

  private boolean finished;

  private Binary lastDevice;

  private long lastTime;

  public AggregationMergeSortOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      List<Accumulator> accumulators,
      boolean hasGroupBy,
      Comparator<SortKey> comparator) {
    super(operatorContext, children);
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.accumulators = accumulators;
    this.hasGroupBy = hasGroupBy;
    this.mergeSortHeap = new MergeSortHeap(inputOperatorsCount, comparator);
  }

  @Override
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    // init all element in inputTsBlocks
    if (!prepareInput()) {
      return null;
    }

    tsBlockBuilder.reset();
    while (!mergeSortHeap.isEmpty()) {
      MergeSortKey mergeSortKey = mergeSortHeap.poll();
      TsBlock targetBlock = mergeSortKey.tsBlock;
      int rowIndex = mergeSortKey.rowIndex;
      Binary currentDevice = targetBlock.getColumn(0).getBinary(rowIndex);
      long currentTime = targetBlock.getTimeByIndex(rowIndex);
      if (lastDevice != null && (!currentDevice.equals(lastDevice) || currentTime != lastTime)) {
        outputResultToTsBlock();
      }

      lastDevice = currentDevice;
      lastTime = currentTime;

      int cnt = 1;
      for (Accumulator accumulator : accumulators) {
        if (accumulator.getPartialResultSize() == 2) {
          Column first =
              hasGroupBy
                  ? targetBlock.getColumn(cnt++).subColumn(rowIndex)
                  : targetBlock.getColumn(cnt++);
          Column second =
              hasGroupBy
                  ? targetBlock.getColumn(cnt++).subColumn(rowIndex)
                  : targetBlock.getColumn(cnt++);
          accumulator.addIntermediate(new Column[] {first, second});
        } else {
          Column column =
              hasGroupBy
                  ? targetBlock.getColumn(cnt++).subColumn(rowIndex)
                  : targetBlock.getColumn(cnt++);
          accumulator.addIntermediate(new Column[] {column});
        }
      }

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

    if (mergeSortHeap.isEmpty()) {
      outputResultToTsBlock();
    }

    return tsBlockBuilder.getPositionCount() > 0 ? tsBlockBuilder.build() : null;
  }

  private void outputResultToTsBlock() {
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    timeBuilder.writeLong(lastTime);
    valueColumnBuilders[0].writeBinary(lastDevice);
    for (int i = 1; i < dataTypes.size(); i++) {
      accumulators.get(i - 1).outputFinal(valueColumnBuilders[i]);
    }
    tsBlockBuilder.declarePosition();
    accumulators.forEach(Accumulator::reset);
    lastDevice = null;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (isInputNotEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
          return true;
        } else {
          handleFinishedChild(i);
        }
      }
    }

    return false;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || isInputNotEmpty(i) || children.get(i) == null) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        // only when not blocked, canCallNext[i] equals true
        canCallNext[i] = true;
      } else {
        listenableFutures.add(blocked);
      }
    }

    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }

    finished = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || isInputNotEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public void close() throws Exception {
    for (int i = 0; i < inputOperatorsCount; i++) {
      final Operator operator = children.get(i);
      if (operator != null) {
        operator.close();
      }
    }
  }

  @Override
  protected void handleFinishedChild(int currentChildIndex) throws Exception {
    // invoking this method when children.get(currentChildIndex).hasNext return false
    noMoreTsBlocks[currentChildIndex] = true;
    inputTsBlocks[currentChildIndex] = null;
    children.get(currentChildIndex).close();
    children.set(currentChildIndex, null);
  }

  @Override
  protected boolean canSkipCurrentChild(int currentChildIndex) {
    return noMoreTsBlocks[currentChildIndex]
        || !isEmpty(currentChildIndex)
        || children.get(currentChildIndex) == null;
  }

  @Override
  protected void processCurrentInputTsBlock(int currentInputIndex) {
    mergeSortHeap.push(new MergeSortKey(inputTsBlocks[currentInputIndex], 0, currentInputIndex));
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    // inputTsBlocks will cache all the tsBlocks returned by inputOperators
    for (Operator operator : children) {
      maxPeekMemory += operator.calculateMaxReturnSize();
      maxPeekMemory += operator.calculateRetainedSizeAfterCallingNext();
    }
    for (Operator operator : children) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemory());
    }
    return Math.max(maxPeekMemory, calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
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
        + RamUsageEstimator.sizeOf(noMoreTsBlocks)
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(canCallNext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }

  private boolean isInputNotEmpty(int index) {
    return inputTsBlocks[index] != null && !inputTsBlocks[index].isEmpty();
  }
}
