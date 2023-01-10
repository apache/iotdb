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
package org.apache.iotdb.db.mpp.execution.operator.process.last;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil.compareTimeSeries;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

// collect all last query result in the same data region and sort them according to the
// time-series's alphabetical order
public class LastQuerySortOperator implements ProcessOperator {
  private static final int MAX_DETECT_COUNT =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

  // we must make sure that data in cachedTsBlock has already been sorted
  // values that have last cache
  private TsBlock cachedTsBlock;

  private final int cachedTsBlockSize;

  // read index for cachedTsBlock
  private int cachedTsBlockRowIndex;

  // we must make sure that Operator in children has already been sorted
  private final List<AbstractUpdateLastCacheOperator> children;

  private final OperatorContext operatorContext;

  private final int inputOperatorsCount;

  private int currentIndex;

  private final TsBlockBuilder tsBlockBuilder;

  private final Comparator<Binary> timeSeriesComparator;

  // used to cache previous TsBlock get from children
  private TsBlock previousTsBlock;

  private int previousTsBlockIndex = 0;

  public LastQuerySortOperator(
      OperatorContext operatorContext,
      TsBlock cachedTsBlock,
      List<AbstractUpdateLastCacheOperator> children,
      Comparator<Binary> timeSeriesComparator) {
    this.cachedTsBlock = cachedTsBlock;
    this.cachedTsBlockSize = cachedTsBlock.getPositionCount();
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.currentIndex = 0;
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder();
    this.timeSeriesComparator = timeSeriesComparator;
    this.previousTsBlock = null;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex < inputOperatorsCount) {
      int endIndex = getEndIndex();
      List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
      for (int i = currentIndex; i < endIndex; i++) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
      return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
    } else {
      return Futures.immediateVoidFuture();
    }
  }

  @Override
  public TsBlock next() {
    // we have consumed up data from children Operator, just return all remaining cached data in
    // cachedTsBlock, tsBlockBuilder and previousTsBlock
    if (currentIndex >= inputOperatorsCount) {
      if (previousTsBlock != null) {
        while (previousTsBlockIndex < previousTsBlock.getPositionCount()) {
          if (canUseDataFromCachedTsBlock(previousTsBlock, previousTsBlockIndex)) {
            LastQueryUtil.appendLastValue(tsBlockBuilder, cachedTsBlock, cachedTsBlockRowIndex++);
          } else {
            LastQueryUtil.appendLastValue(tsBlockBuilder, previousTsBlock, previousTsBlockIndex++);
          }
        }
      }
      TsBlock res = cachedTsBlock.subTsBlock(cachedTsBlockRowIndex);
      cachedTsBlockRowIndex = cachedTsBlockSize;
      if (!tsBlockBuilder.isEmpty()) {
        LastQueryUtil.appendLastValue(tsBlockBuilder, res);
        res = tsBlockBuilder.build();
        tsBlockBuilder.reset();
      }
      return res;
    }

    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    int endIndex = getEndIndex();

    while ((System.nanoTime() - start < maxRuntime)
        && (currentIndex < endIndex
            || (previousTsBlock != null
                && previousTsBlockIndex < previousTsBlock.getPositionCount()))
        && !tsBlockBuilder.isFull()) {
      if (previousTsBlock == null || previousTsBlock.getPositionCount() <= previousTsBlockIndex) {
        if (children.get(currentIndex).hasNextWithTimer()) {
          previousTsBlock = children.get(currentIndex).nextWithTimer();
          previousTsBlockIndex = 0;
          if (previousTsBlock == null) {
            return null;
          }
        }
        currentIndex++;
      }
      if (previousTsBlockIndex < previousTsBlock.getPositionCount()) {
        if (canUseDataFromCachedTsBlock(previousTsBlock, previousTsBlockIndex)) {
          LastQueryUtil.appendLastValue(tsBlockBuilder, cachedTsBlock, cachedTsBlockRowIndex++);
        } else {
          LastQueryUtil.appendLastValue(tsBlockBuilder, previousTsBlock, previousTsBlockIndex++);
        }
      }
    }

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() {
    return currentIndex < inputOperatorsCount
        || cachedTsBlockRowIndex < cachedTsBlockSize
        || !tsBlockBuilder.isEmpty()
        || (previousTsBlock != null && previousTsBlockIndex < previousTsBlock.getPositionCount());
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
    cachedTsBlock = null;
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES + cachedTsBlock.getRetainedSizeInBytes();
    long res = 0;
    for (Operator child : children) {
      res = Math.max(res, maxPeekMemory + child.calculateMaxPeekMemory());
    }
    return res;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long childrenMaxReturnSize = 0;
    long childrenSumRetainedSize = 0;
    for (Operator child : children) {
      childrenMaxReturnSize = Math.max(childrenMaxReturnSize, child.calculateMaxReturnSize());
      childrenSumRetainedSize += child.calculateRetainedSizeAfterCallingNext();
    }
    return cachedTsBlock.getRetainedSizeInBytes() + childrenMaxReturnSize + childrenSumRetainedSize;
  }

  private int getEndIndex() {
    return currentIndex + Math.min(MAX_DETECT_COUNT, inputOperatorsCount - currentIndex);
  }

  private boolean canUseDataFromCachedTsBlock(TsBlock tsBlock, int index) {
    return cachedTsBlockRowIndex < cachedTsBlockSize
        && compareTimeSeries(
                cachedTsBlock, cachedTsBlockRowIndex, tsBlock, index, timeSeriesComparator)
            < 0;
  }
}
