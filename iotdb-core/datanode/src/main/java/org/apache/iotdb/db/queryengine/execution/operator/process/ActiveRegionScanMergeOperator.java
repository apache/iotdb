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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class ActiveRegionScanMergeOperator extends AbstractConsumeAllOperator {

  private final int[] inputIndex;
  private final boolean[] noMoreTsBlocks;
  private final TsBlockBuilder tsBlockBuilder;
  private final boolean outputCount;
  private final boolean needMergeBeforeCount;
  private boolean finished;
  private Set<String> deduplicatedSet;
  private long count = -1;

  public ActiveRegionScanMergeOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      boolean outputCount,
      boolean needMergeBeforeCount) {
    super(operatorContext, children);
    this.inputIndex = new int[this.inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.finished = false;
    this.outputCount = outputCount;
    this.needMergeBeforeCount = needMergeBeforeCount;
    if (!outputCount || needMergeBeforeCount) {
      this.deduplicatedSet = new HashSet<>();
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
        if (!outputCount || needMergeBeforeCount) {
          if (deduplicatedSet.contains(id)) {
            continue;
          }
          deduplicatedSet.add(id);
        }
        buildOneRow(i, curTsBlockRowIndex + row, timeColumnBuilder, valueColumnBuilders);
      }
      inputIndex[i] += maxRowCanBuild;
    }
    return outputCount ? returnResultIfNoMoreData() : tsBlockBuilder.build();
  }

  private TsBlock returnResultIfNoMoreData() throws Exception {
    if (isFinished()) {
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
      timeColumnBuilder.writeLong(inputTsBlocks[i].getTimeColumn().getLong(curTsBlockRowIndex));
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
    if (retainedTsBlock != null) {
      return true;
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
    return false;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }
    if (retainedTsBlock != null) {
      return false;
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
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
