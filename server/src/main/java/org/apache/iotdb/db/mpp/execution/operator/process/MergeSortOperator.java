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
package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.MergeSortUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class MergeSortOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Operator> inputOperators;
  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  private final boolean[] noMoreTsBlocks;
  private final MergeSortUtils mergeSortToolKit;

  private boolean finished;

  public MergeSortOperator(
      OperatorContext operatorContext,
      List<Operator> inputOperators,
      List<TSDataType> dataTypes,
      MergeSortUtils mergeSortUtils) {
    this.operatorContext = operatorContext;
    this.inputOperators = inputOperators;
    this.dataTypes = dataTypes;
    this.inputOperatorsCount = inputOperators.size();
    this.mergeSortToolKit = mergeSortUtils;
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i)) {
        ListenableFuture<?> blocked = inputOperators.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
  }

  /** If the tsBlock is null or has no more data in the tsBlock, return true; else return false; */
  private boolean isTsBlockEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == 0;
  }

  @Override
  public TsBlock next() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i) && inputOperators.get(i).hasNext()) {
        inputTsBlocks[i] = inputOperators.get(i).next();
        if (inputTsBlocks[i] == null || inputTsBlocks[i].isEmpty()) {
          return null;
        }
        mergeSortToolKit.addTsBlock(inputTsBlocks[i], i);
      }
    }

    List<Integer> targetTsBlockIndex = mergeSortToolKit.getTargetTsBlockIndex();
    int targetTsBlockSize = targetTsBlockIndex.size();
    if (targetTsBlockSize == 1) {
      int index = targetTsBlockIndex.get(0);
      TsBlock resultTsBlock = inputTsBlocks[index];
      inputTsBlocks[index] = null;
      mergeSortToolKit.updateTsBlock(index, null);
      return resultTsBlock;
    }

    // get the row of tsBlock whose keyValue <= targetValue
    tsBlockBuilder.reset();

    TsBlockSingleColumnIterator[] tsBlockIterators =
        new TsBlockSingleColumnIterator[targetTsBlockSize];
    for (int i = 0; i < targetTsBlockSize; i++) {
      tsBlockIterators[i] =
          inputTsBlocks[targetTsBlockIndex.get(i)].getTsBlockSingleColumnIterator();
      mergeSortToolKit.keyValueSelector.updateTsBlock(i, tsBlockIterators[i]);
    }
    // use the min KeyValue of all TsBlock as the end keyValue of result tsBlock, it has already
    // been calculated through mergeSortToolKit.getTargetTsBlockIndex().
    // ps: max KeyValue if the ordering is desc, which is aimed to use up at least one tsBlock.
    boolean hasMatchKey = true;
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    // add to result TsBlock through a merge-sorting way
    while (hasMatchKey) {
      hasMatchKey = false;
      // find the targetTsBlock which has the min KeyValue to output one row
      int minIndex = mergeSortToolKit.keyValueSelector.poll();
      if (minIndex != -1) {
        hasMatchKey = true;
        int rowIndex = tsBlockIterators[minIndex].getRowIndex();
        TsBlock targetBlock = inputTsBlocks[targetTsBlockIndex.get(minIndex)];
        timeBuilder.writeLong(targetBlock.getTimeByIndex(rowIndex));
        for (int i = 0; i < valueColumnBuilders.length; i++) {
          if (targetBlock.getColumn(i).isNull(rowIndex)) {
            valueColumnBuilders[i].appendNull();
            continue;
          }
          valueColumnBuilders[i].write(targetBlock.getColumn(i), rowIndex);
        }
        tsBlockIterators[minIndex].next();
        tsBlockBuilder.declarePosition();
        if (!tsBlockIterators[minIndex].hasNext()) break;
        mergeSortToolKit.keyValueSelector.update();
      }
    }
    // update inputTsBlocks after consuming
    mergeSortToolKit.keyValueSelector.clear();
    for (int i = 0; i < targetTsBlockSize; i++) {
      if (tsBlockIterators[i].hasNext()) {
        int rowIndex = tsBlockIterators[i].getRowIndex();
        inputTsBlocks[targetTsBlockIndex.get(i)] =
            inputTsBlocks[targetTsBlockIndex.get(i)].subTsBlock(rowIndex);
        mergeSortToolKit.updateTsBlock(
            targetTsBlockIndex.get(i), inputTsBlocks[targetTsBlockIndex.get(i)]);
      } else {
        inputTsBlocks[targetTsBlockIndex.get(i)] = null;
        mergeSortToolKit.updateTsBlock(targetTsBlockIndex.get(i), null);
      }
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isTsBlockEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (inputOperators.get(i).hasNext()) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    for (Operator operator : inputOperators) {
      operator.close();
    }
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || !isTsBlockEmpty(i)) {
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
    for (Operator operator : inputOperators) {
      maxPeekMemory += operator.calculateMaxReturnSize();
      maxPeekMemory += operator.calculateRetainedSizeAfterCallingNext();
    }
    for (Operator operator : inputOperators) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemory());
    }
    return Math.max(maxPeekMemory, calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return (1L + dataTypes.size()) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0, minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : inputOperators) {
      long maxReturnSize = child.calculateMaxReturnSize();
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
    }
    return currentRetainedSize - minChildReturnSize;
  }
}
