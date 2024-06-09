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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class ActiveRegionScanMergeOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ActiveRegionScanMergeOperator.class)
          + RamUsageEstimator.shallowSizeOfInstance(Set.class);

  private static final long REFERENCE_SIZE = 8;

  private final int[] inputIndex;
  private final boolean[] noMoreTsBlocks;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean outputCount;
  private final boolean needMergeBeforeCount;
  private boolean finished;
  private Set<String> deduplicatedSet;
  private long count = -1;

  private long estimatedSetSize = 0;

  public ActiveRegionScanMergeOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      boolean outputCount,
      boolean needMergeBeforeCount,
      long estimatedSize) {
    super(operatorContext, children);
    this.inputIndex = new int[this.inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.finished = false;
    this.outputCount = outputCount;
    this.needMergeBeforeCount = needMergeBeforeCount;
    if (!outputCount || needMergeBeforeCount) {
      this.deduplicatedSet = new HashSet<>();
      estimatedSetSize = estimatedSize * REFERENCE_SIZE;
    }
    if (outputCount) {
      count = 0;
    }
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

  @Override
  public TsBlock next() throws Exception {
    if (!prepareInput()) {
      return null;
    }
    tsBlockBuilder.reset();

    // Indicates how many rows can be built in this calculate
    int maxRowCanBuild = Integer.MAX_VALUE;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (inputTsBlocks[i] != null) {
        maxRowCanBuild =
            Math.min(maxRowCanBuild, inputTsBlocks[i].getPositionCount() - inputIndex[i]);
      }
    }

    if (!needMergeBeforeCount) {
      for (int i = 0; i < inputOperatorsCount; i++) {
        if (inputTsBlocks[i] == null) {
          continue;
        }
        for (int row = 0; row < maxRowCanBuild; row++) {
          long childCount = inputTsBlocks[i].getValueColumns()[0].getLong(inputIndex[i] + row);
          count += childCount;
          inputIndex[i] += maxRowCanBuild;
        }
      }
    } else {
      TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
      ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
      int curTsBlockRowIndex;
      for (int i = 0; i < inputOperatorsCount; i++) {
        if (inputTsBlocks[i] == null) {
          continue;
        }
        curTsBlockRowIndex = inputIndex[i];
        for (int row = 0; row < maxRowCanBuild; row++) {
          String id =
              inputTsBlocks[i].getValueColumns()[0].getBinary(curTsBlockRowIndex + row).toString();
          if (deduplicatedSet.contains(id)) {
            continue;
          }
          deduplicatedSet.add(id);
          buildOneRow(i, curTsBlockRowIndex + row, timeColumnBuilder, valueColumnBuilders);
        }
        inputIndex[i] += maxRowCanBuild;
      }
    }
    return outputCount ? returnResultIfNoMoreData() : tsBlockBuilder.build();
  }

  @Override
  protected TsBlock getNextTsBlock(int childIndex) throws Exception {
    inputIndex[childIndex] = 0;
    return children.get(childIndex).nextWithTimer();
  }

  @Override
  protected boolean canSkipCurrentChild(int currentChildIndex) {
    return noMoreTsBlocks[currentChildIndex]
        || !isEmpty(currentChildIndex)
        || children.get(currentChildIndex) == null;
  }

  @Override
  protected void handleFinishedChild(int currentChildIndex) throws Exception {
    noMoreTsBlocks[currentChildIndex] = true;
    inputTsBlocks[currentChildIndex] = null;
    children.get(currentChildIndex).close();
    children.set(currentChildIndex, null);
  }

  private TsBlock returnResultIfNoMoreData() throws Exception {
    if (isFinished() || finished) {
      tsBlockBuilder.reset();
      TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
      ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
      timeColumnBuilder.writeLong(-1);
      valueColumnBuilders[0].writeLong(count);
      tsBlockBuilder.declarePosition();
      count = -1;
      return tsBlockBuilder.build();
    }
    return null;
  }

  private void buildOneRow(
      int i,
      int curTsBlockRowIndex,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder[] valueColumnBuilders) {
    if (outputCount) {
      count++;
    } else {
      timeColumnBuilder.writeLong(-1);
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (inputTsBlocks[i].getValueColumns()[j].isNull(curTsBlockRowIndex)) {
          valueColumnBuilders[j].appendNull();
        } else {
          valueColumnBuilders[j].writeBinary(
              inputTsBlocks[i].getValueColumns()[j].getBinary(curTsBlockRowIndex));
        }
      }
      tsBlockBuilder.declarePosition();
    }
  }

  @Override
  protected boolean isEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == inputIndex[tsBlockIndex];
  }

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || (children.get(i) != null && children.get(i).hasNextWithTimer())) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return count != -1;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }
    finished = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      // has more tsBlock output from children[i] or has cached tsBlock in inputTsBlocks[i]
      if (!noMoreTsBlocks[i] || !isEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished && count == -1;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = estimatedSetSize;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(
              childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemoryWithCounter());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }

    maxPeekMemory += calculateMaxReturnSize();
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0;
    long minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock
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
        + RamUsageEstimator.sizeOf(inputIndex)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
