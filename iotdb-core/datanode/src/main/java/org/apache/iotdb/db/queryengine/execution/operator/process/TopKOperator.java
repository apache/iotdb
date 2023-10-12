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

import org.apache.iotdb.commons.exception.runtime.UnSupportedDataTypeException;
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

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class TopKOperator extends AbstractConsumeAllOperator {

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean[] noMoreTsBlocks;
  private final MergeSortHeap mergeSortHeap;
  private final Comparator<SortKey> comparator;

  private boolean finished;

  private final long topValue;

  // all data from children have been loaded
  private boolean allChildrenFinished;

  // query result of final TopKOperator
  private MergeSortKey[] topKResult;

  // return size of topKResult
  private int resultReturnSize = 0;

  public TopKOperator(
      OperatorContext operatorContext,
      List<Operator> inputOperators,
      List<TSDataType> dataTypes,
      Comparator<SortKey> comparator,
      long topValue) {
    super(operatorContext, inputOperators);
    this.dataTypes = dataTypes;
    // use MAX-HEAP
    this.mergeSortHeap = new MergeSortHeap((int) topValue, comparator.reversed());
    this.comparator = comparator;
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder((int) topValue, dataTypes);
    this.topValue = topValue;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || children.get(i) == null) {
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
  public boolean hasNext() throws Exception {
    return !(allChildrenFinished && resultReturnSize == topKResult.length);
  }

  @Override
  public TsBlock next() throws Exception {
    if (allChildrenFinished && resultReturnSize < topKResult.length) {
      return getResultFromCachedTopKResult();
    }

    allChildrenFinished = true;
    for (int childIdx = 0; childIdx < inputOperatorsCount; childIdx++) {
      if (noMoreTsBlocks[childIdx] || children.get(childIdx) == null) {
        continue;
      }

      if (!children.get(childIdx).isBlocked().isDone()) {
        allChildrenFinished = false;
        continue;
      }

      if (!children.get(childIdx).hasNextWithTimer()) {
        noMoreTsBlocks[childIdx] = true;
        children.set(childIdx, null);
        continue;
      }

      TsBlock currentTsBlock = getNextTsBlock(childIdx);
      if (currentTsBlock == null || currentTsBlock.isEmpty()) {
        allChildrenFinished = false;
        continue;
      }

      boolean skipCurrentBatch = false;
      for (int idx = 0; idx < currentTsBlock.getPositionCount(); idx++) {
        if (mergeSortHeap.getHeapSize() < topValue) {
          mergeSortHeap.push(new MergeSortKey(buildOneEntryTsBlock(currentTsBlock, idx), 0));
        } else {
          if (comparator.compare(new MergeSortKey(currentTsBlock, idx), mergeSortHeap.peek()) < 0) {
            mergeSortHeap.poll();
            mergeSortHeap.push(new MergeSortKey(buildOneEntryTsBlock(currentTsBlock, idx), 0));
          } else {
            // TODO change this judgement, because topKOperator may also contains order expression
            skipCurrentBatch = true;
          }
        }
      }

      // if current childIdx tsblock has no value to put into heap
      // the remaining data will also have no value to put int heap
      if (skipCurrentBatch) {
        noMoreTsBlocks[childIdx] = true;
        children.set(childIdx, null);
      } else {
        allChildrenFinished = false;
      }
    }

    if (!allChildrenFinished) {
      return null;
    }

    finished = true;
    return getResultFromMaxHeap(mergeSortHeap);
  }

  @Override
  public long calculateMaxPeekMemory() {
    // traverse each child serial,
    // so no need to accumulate the returnSize and retainedSize of each child
    long maxPeekMemory = calculateMaxReturnSize();
    for (Operator operator : children) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemory());
    }
    return Math.max(maxPeekMemory, topValue * getMemoryUsageOfOneMergeSortKey());
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return (topValue - resultReturnSize) * getMemoryUsageOfOneMergeSortKey();
  }

  private TsBlock getResultFromMaxHeap(MergeSortHeap mergeSortHeap) {
    int cnt = mergeSortHeap.getHeapSize();
    topKResult = new MergeSortKey[cnt];
    while (!mergeSortHeap.isEmpty()) {
      topKResult[--cnt] = mergeSortHeap.poll();
    }

    return getResultFromCachedTopKResult();
  }

  private TsBlock getResultFromCachedTopKResult() {
    tsBlockBuilder.reset();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = resultReturnSize; i < topKResult.length; i++) {
      MergeSortKey mergeSortKey = topKResult[i];
      TsBlock targetBlock = mergeSortKey.tsBlock;
      tsBlockBuilder.getTimeColumnBuilder().writeLong(targetBlock.getTimeByIndex(0));
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (targetBlock.getColumn(j).isNull(0)) {
          valueColumnBuilders[j].appendNull();
          continue;
        }
        valueColumnBuilders[j].write(targetBlock.getColumn(j), 0);
      }
      resultReturnSize += 1;
      tsBlockBuilder.declarePosition();

      if (tsBlockBuilder.isFull()) {
        return tsBlockBuilder.build();
      }
    }

    return tsBlockBuilder.build();
  }

  private TsBlock buildOneEntryTsBlock(TsBlock tsBlock, int rowIndex) {
    TsBlockBuilder resultTsBlockBuilder = new TsBlockBuilder(1, dataTypes);
    resultTsBlockBuilder.getTimeColumnBuilder().writeLong(tsBlock.getTimeByIndex(rowIndex));

    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      if (tsBlock.getColumn(i).isNull(rowIndex)) {
        resultTsBlockBuilder.getValueColumnBuilders()[i].appendNull();
        continue;
      }
      resultTsBlockBuilder.getValueColumnBuilders()[i].write(tsBlock.getColumn(i), rowIndex);
    }

    resultTsBlockBuilder.declarePosition();

    return resultTsBlockBuilder.build();
  }

  private long getMemoryUsageOfOneMergeSortKey() {
    long memory = 0;
    for (TSDataType dataType : dataTypes) {
      switch (dataType) {
        case BOOLEAN:
          memory += 1;
          break;
        case INT32:
        case FLOAT:
          memory += 4;
          break;
        case INT64:
        case DOUBLE:
        case VECTOR:
          memory += 8;
          break;
        case TEXT:
          memory += 16;
          break;
        default:
          throw new UnSupportedDataTypeException("Unknown datatype: " + dataType);
      }
    }
    return memory;
  }
}
