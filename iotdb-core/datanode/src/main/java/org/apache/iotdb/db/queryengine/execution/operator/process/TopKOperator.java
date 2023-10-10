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

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class TopKOperator extends AbstractConsumeAllOperator {

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean[] noMoreTsBlocks;
  private final MergeSortHeap mergeSortHeap;
  private final Comparator<SortKey> comparator;

  private boolean finished;

  private final long topValue;

  boolean allBlocksRead;

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
      if (needCallNext(i)) {
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
  public boolean hasNext() throws Exception {
    return !allBlocksRead;
  }

  @Override
  public TsBlock next() throws Exception {
    allBlocksRead = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || children.get(i) == null) {
        continue;
      }

      if (!children.get(i).isBlocked().isDone()) {
        allBlocksRead = false;
        continue;
      }

      if (children.get(i).hasNextWithTimer()) {
        inputTsBlocks[i] = getNextTsBlock(i);
        if (inputTsBlocks[i] == null) {
          allBlocksRead = false;
          continue;
        }

        boolean getNewValue = false;
        for (int idx = 0; idx < inputTsBlocks[i].getPositionCount(); idx++) {
          if (mergeSortHeap.getHeapSize() < topValue) {
            mergeSortHeap.push(new MergeSortKey(inputTsBlocks[i], idx));
            getNewValue = true;
          } else if (comparator.compare(
                  new MergeSortKey(inputTsBlocks[i], idx), mergeSortHeap.peek())
              < 0) {
            mergeSortHeap.poll();
            mergeSortHeap.push(new MergeSortKey(inputTsBlocks[i], idx));
            getNewValue = true;
          }
        }

        if (!getNewValue) {
          inputTsBlocks[i] = null;
          noMoreTsBlocks[i] = true;
          children.set(i, null);
        } else {
          allBlocksRead = false;
        }
      } else {
        noMoreTsBlocks[i] = true;
        children.set(i, null);
      }
    }

    if (!allBlocksRead) {
      return null;
    }

    return getResultFromMaxHeap(mergeSortHeap, tsBlockBuilder);
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

  private TsBlock getResultFromMaxHeap(MergeSortHeap mergeSortHeap, TsBlockBuilder tsBlockBuilder) {
    int cnt = mergeSortHeap.getHeapSize();
    MergeSortKey[] mergeSortKeys = new MergeSortKey[cnt];
    while (!mergeSortHeap.isEmpty()) {
      mergeSortKeys[--cnt] = mergeSortHeap.poll();
    }

    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (MergeSortKey mergeSortKey : mergeSortKeys) {
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
    }

    allBlocksRead = true;
    finished = true;
    // TODO make all children null
    return tsBlockBuilder.build();
  }

  private boolean needCallNext(int i) {
    return noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null;
  }
}
