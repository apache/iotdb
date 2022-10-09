/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TagAggregationOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<List<String>> groups;
  private final List<List<Aggregator>> groupedAggregators;
  private final List<Operator> children;
  private final TsBlock[] inputTsBlocks;

  // These fields record the to be consumed index of each tsBlock.
  private final int[] consumedIndices;
  private final TsBlockBuilder tsBlockBuilder;
  private final long maxRetainedSize;
  private final long childrenRetainedSize;
  private final long maxReturnSize;

  public TagAggregationOperator(
      OperatorContext operatorContext,
      List<List<String>> groups,
      List<List<Aggregator>> groupedAggregators,
      List<Operator> children,
      long maxReturnSize) {
    this.operatorContext = Validate.notNull(operatorContext);
    this.groups = Validate.notNull(groups);
    this.groupedAggregators = Validate.notNull(groupedAggregators);
    this.children = Validate.notNull(children);
    List<TSDataType> actualOutputColumnTypes = new ArrayList<>();
    for (String ignored : groups.get(0)) {
      actualOutputColumnTypes.add(TSDataType.TEXT);
    }
    for (int outputColumnIdx = 0;
        outputColumnIdx < groupedAggregators.get(0).size();
        outputColumnIdx++) {
      for (List<Aggregator> aggregators : groupedAggregators) {
        Aggregator aggregator = aggregators.get(outputColumnIdx);
        if (aggregator != null) {
          actualOutputColumnTypes.addAll(Arrays.asList(aggregator.getOutputType()));
          break;
        }
      }
    }
    this.tsBlockBuilder = new TsBlockBuilder(actualOutputColumnTypes);
    // Initialize input tsblocks for each aggregator group.
    this.inputTsBlocks = new TsBlock[children.size()];
    this.consumedIndices = new int[children.size()];
    this.maxRetainedSize = children.stream().mapToLong(Operator::calculateMaxReturnSize).sum();
    this.childrenRetainedSize =
        children.stream().mapToLong(Operator::calculateRetainedSizeAfterCallingNext).sum();
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    boolean successful = true;
    while (System.nanoTime() - start < maxRuntime && !tsBlockBuilder.isFull() && successful) {
      successful = processOneRow();
    }
    TsBlock tsBlock = null;
    if (tsBlockBuilder.getPositionCount() > 0) {
      tsBlock = tsBlockBuilder.build();
    }
    tsBlockBuilder.reset();
    return tsBlock;
  }

  private boolean processOneRow() {
    for (int i = 0; i < children.size(); i++) {
      // If the data is unavailable first, try to find next tsblock of the child.
      if (dataUnavailable(i)) {
        inputTsBlocks[i] = children.get(i).next();
        consumedIndices[i] = 0;
      }

      // If it's still unavailable, then blocked by children i.
      if (dataUnavailable(i)) {
        return false;
      }
    }

    TsBlock[] rowBlocks = new TsBlock[children.size()];
    for (int i = 0; i < children.size(); i++) {
      rowBlocks[i] = inputTsBlocks[i].getRegion(consumedIndices[i], 1);
    }
    for (int groupIdx = 0; groupIdx < groups.size(); groupIdx++) {
      List<String> group = groups.get(groupIdx);
      List<Aggregator> aggregators = groupedAggregators.get(groupIdx);

      for (Aggregator aggregator : aggregators) {
        if (aggregator == null) {
          continue;
        }
        aggregator.reset();
        aggregator.processTsBlocks(rowBlocks);
      }

      TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
      timeColumnBuilder.writeLong(rowBlocks[0].getStartTime());
      ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

      for (int i = 0; i < group.size(); i++) {
        if (group.get(i) == null) {
          columnBuilders[i].writeBinary(new Binary("NULL"));
        } else {
          columnBuilders[i].writeBinary(new Binary(group.get(i)));
        }
      }
      for (int i = 0; i < aggregators.size(); i++) {
        Aggregator aggregator = aggregators.get(i);
        ColumnBuilder columnBuilder = columnBuilders[i + group.size()];
        if (aggregator == null) {
          columnBuilder.appendNull();
        } else {
          aggregator.outputResult(new ColumnBuilder[] {columnBuilder});
        }
      }
      tsBlockBuilder.declarePosition();
    }

    // Reset dataReady for next iteration
    for (int i = 0; i < children.size(); i++) {
      consumedIndices[i]++;
    }
    return true;
  }

  @Override
  public boolean hasNext() {
    for (int i = 0; i < children.size(); i++) {
      if (dataUnavailable(i) && !children.get(i).hasNext()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (!blocked.isDone() && dataUnavailable(i)) {
        listenableFutures.add(blocked);
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : Futures.successfulAsList(listenableFutures);
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + childrenRetainedSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + childrenRetainedSize;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  private boolean dataUnavailable(int index) {
    return inputTsBlocks[index] == null
        || consumedIndices[index] == inputTsBlocks[index].getPositionCount();
  }
}
