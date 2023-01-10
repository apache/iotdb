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

import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil.appendLastValue;
import static org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil.getTimeSeries;

// merge all last query result from different data regions, it will select max time for the same
// time-series
public class LastQueryMergeOperator implements ProcessOperator {

  public static final long MAP_NODE_RETRAINED_SIZE = 16L + Location.INSTANCE_SIZE;

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private final int inputOperatorsCount;

  private TsBlockBuilder tsBlockBuilder;

  /** TsBlock from child operator. Only one cache now. */
  private final TsBlock[] inputTsBlocks;

  /** start index for each input TsBlocks and size of it is equal to inputTsBlocks */
  private final int[] inputIndex;

  /**
   * Represent whether there are more tsBlocks from ith child operator. If all elements in
   * noMoreTsBlocks[] are true and inputTsBlocks[] are consumed completely, this operator is
   * finished.
   */
  private final boolean[] noMoreTsBlocks;

  private boolean finished = false;

  private final Comparator<Binary> comparator;

  private final TreeMap<Binary, Location> timeSeriesSelector;

  public LastQueryMergeOperator(
      OperatorContext operatorContext, List<Operator> children, Comparator<Binary> comparator) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.tsBlockBuilder = LastQueryUtil.createTsBlockBuilder();
    this.inputTsBlocks = new TsBlock[this.inputOperatorsCount];
    this.inputIndex = new int[this.inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.comparator = comparator;
    this.timeSeriesSelector = new TreeMap<>(comparator);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
  }

  @Override
  public TsBlock next() {

    // end time series for returned TsBlock this time, it's the min/max end time series among all
    // the children
    // TsBlocks order by asc/desc
    Binary currentEndTimeSeries = null;
    boolean init = false;
    // get TsBlock for each input, put their time series into TimeSeriesSelector and then use the
    // min/max TimeSeries
    // among all the input TsBlock as the current output TsBlock's endTimeSeries.
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        if (children.get(i).hasNextWithTimer()) {
          inputIndex[i] = 0;
          inputTsBlocks[i] = children.get(i).nextWithTimer();
          if (!empty(i)) {
            int rowSize = inputTsBlocks[i].getPositionCount();
            for (int row = 0; row < rowSize; row++) {
              Binary key = getTimeSeries(inputTsBlocks[i], row);
              Location location = timeSeriesSelector.get(key);
              if (location == null
                  || inputTsBlocks[i].getTimeByIndex(row)
                      > inputTsBlocks[location.tsBlockIndex].getTimeByIndex(location.rowIndex)) {
                timeSeriesSelector.put(key, new Location(i, row));
              }
            }
          } else {
            // child operator has next but return an empty TsBlock which means that it may not
            // finish calculation in given time slice.
            // In such case, LastQueryMergeOperator can't go on calculating, so we just return null.
            // We can also use the while loop here to continuously call the hasNext() and next()
            // methods of the child operator until its hasNext() returns false or the next() gets
            // the data that is not empty, but this will cause the execution time of the while loop
            // to be uncontrollable and may exceed all allocated time slice
            return null;
          }
        } else { // no more tsBlock
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
      // update the currentEndTimeSeries if the TsBlock is not empty
      if (!empty(i)) {
        Binary endTimeSeries =
            getTimeSeries(inputTsBlocks[i], inputTsBlocks[i].getPositionCount() - 1);
        currentEndTimeSeries =
            init
                ? (comparator.compare(currentEndTimeSeries, endTimeSeries) < 0
                    ? currentEndTimeSeries
                    : endTimeSeries)
                : endTimeSeries;
        init = true;
      }
    }

    if (timeSeriesSelector.isEmpty()) {
      TsBlock res = tsBlockBuilder.build();
      tsBlockBuilder.reset();
      return res;
    }

    while (!timeSeriesSelector.isEmpty()
        && (comparator.compare(timeSeriesSelector.firstKey(), currentEndTimeSeries) <= 0)) {
      Location location = timeSeriesSelector.pollFirstEntry().getValue();
      appendLastValue(tsBlockBuilder, inputTsBlocks[location.tsBlockIndex], location.rowIndex);
    }

    clearTsBlockCache(currentEndTimeSeries);

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!empty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (children.get(i).hasNextWithTimer()) {
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
    for (Operator child : children) {
      child.close();
    }
    tsBlockBuilder = null;
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      // has more tsBlock output from children[i] or has cached tsBlock in inputTsBlocks[i]
      if (!noMoreTsBlocks[i] || !empty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemory());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }
    // result size + cached TreeMap size
    maxPeekMemory +=
        (calculateMaxReturnSize()
            + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()
                * MAP_NODE_RETRAINED_SIZE);
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnSize = 0;
    for (Operator child : children) {
      maxReturnSize = Math.max(maxReturnSize, child.calculateMaxReturnSize());
    }
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long childrenSum = 0, minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      childrenSum += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock + cached TreeMap size
    return (childrenSum - minChildReturnSize)
        + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber()
            * MAP_NODE_RETRAINED_SIZE;
  }

  /**
   * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  private boolean empty(int columnIndex) {
    return inputTsBlocks[columnIndex] == null
        || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
  }

  private void clearTsBlockCache(Binary endTimeSeries) {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!empty(i)
          && comparator.compare(
                  getTimeSeries(inputTsBlocks[i], inputTsBlocks[i].getPositionCount() - 1),
                  endTimeSeries)
              == 0) {
        inputTsBlocks[i] = null;
      }
    }
  }

  private static class Location {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Location.class).instanceSize();
    int tsBlockIndex;
    int rowIndex;

    public Location(int tsBlockIndex, int rowIndex) {
      this.tsBlockIndex = tsBlockIndex;
      this.rowIndex = rowIndex;
    }
  }
}
