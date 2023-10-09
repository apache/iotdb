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

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class TopKOperator extends AbstractConsumeAllOperator {

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean[] noMoreTsBlocks;
  private final MergeSortHeap mergeSortHeap;
  private final Comparator<SortKey> comparator;

  private boolean finished;

  private final long topValue;

  private int readingValueCount = 0;

  public TopKOperator(
      OperatorContext operatorContext,
      List<Operator> inputOperators,
      List<TSDataType> dataTypes,
      Comparator<SortKey> comparator,
      long topValue) {
    super(operatorContext, inputOperators);
    this.dataTypes = dataTypes;
    // use MAX-HEAP
    this.mergeSortHeap = new MergeSortHeap(inputOperatorsCount, comparator.reversed());
    this.comparator = comparator;
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.topValue = topValue;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (!blocked.isDone()) {
        listenableFutures.add(blocked);
      }
    }
    return listenableFutures.isEmpty()
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }
    if (readingValueCount > topValue) {
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
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null) {
        continue;
      }

      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        if (children.get(i).hasNextWithTimer()) {
          inputTsBlocks[i] = getNextTsBlock(i);
        } else {
          children.get(i).close();
          children.set(i, null);
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      } else {

      }
    }

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
      readingValueCount += minMergeSortKey.tsBlock.getPositionCount() - minMergeSortKey.rowIndex;
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
      timeBuilder.writeLong(targetBlock.getTimeByIndex(rowIndex));
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
        if (!mergeSortHeap.isEmpty()
            && comparator.compare(mergeSortHeap.peek(), mergeSortKey) > 0) {
          break;
        }
      } else {
        mergeSortKey.rowIndex++;
        mergeSortHeap.push(mergeSortKey);
      }

      // break if time is out or tsBlockBuilder is full
      if (System.nanoTime() - startTime > maxRuntime
          || tsBlockBuilder.isFull()
          || tsBlockBuilder.getPositionCount() > topValue) {
        break;
      }
    }
    readingValueCount += tsBlockBuilder.getPositionCount();
    return tsBlockBuilder.build();
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
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return (1L + dataTypes.size()) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  protected boolean prepareInput() throws Exception {
    boolean allReady = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null) {
        continue;
      }
      if (canCallNext[i]) {
        if (children.get(i).hasNextWithTimer()) {
          inputTsBlocks[i] = getNextTsBlock(i);
          canCallNext[i] = false;
          if (isEmpty(i)) {
            allReady = false;
          } else {
            mergeSortHeap.push(new MergeSortKey(inputTsBlocks[i], 0, i));
          }
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      } else {
        allReady = false;
      }
    }
    return allReady;
  }
}
