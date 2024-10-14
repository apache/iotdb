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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TagAggregationOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TagAggregationOperator.class);
  private final List<List<String>> groups;
  private final List<List<TreeAggregator>> groupedAggregators;

  // These fields record the to be consumed index of each tsBlock.
  private final int[] consumedIndices;
  private final TsBlockBuilder tsBlockBuilder;
  private final long maxRetainedSize;
  private final long childrenRetainedSize;

  public TagAggregationOperator(
      OperatorContext operatorContext,
      List<List<String>> groups,
      List<List<TreeAggregator>> groupedAggregators,
      List<Operator> children,
      long maxReturnSize) {
    super(operatorContext, children);
    this.groups = Validate.notNull(groups);
    this.groupedAggregators = Validate.notNull(groupedAggregators);
    List<TSDataType> actualOutputColumnTypes = new ArrayList<>();
    for (int i = 0; i < groups.get(0).size(); i++) {
      actualOutputColumnTypes.add(TSDataType.TEXT);
    }
    for (int outputColumnIdx = 0;
        outputColumnIdx < groupedAggregators.get(0).size();
        outputColumnIdx++) {
      for (List<TreeAggregator> aggregators : groupedAggregators) {
        TreeAggregator aggregator = aggregators.get(outputColumnIdx);
        if (aggregator != null) {
          actualOutputColumnTypes.addAll(Arrays.asList(aggregator.getOutputType()));
          break;
        }
      }
    }
    this.tsBlockBuilder = new TsBlockBuilder(actualOutputColumnTypes);
    Arrays.fill(canCallNext, false);
    this.consumedIndices = new int[children.size()];
    this.maxRetainedSize = children.stream().mapToLong(Operator::calculateMaxReturnSize).sum();
    this.childrenRetainedSize =
        children.stream().mapToLong(Operator::calculateRetainedSizeAfterCallingNext).sum();
    this.maxReturnSize = maxReturnSize;
  }

  @Override
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    while (System.nanoTime() - start < maxRuntime && !tsBlockBuilder.isFull()) {
      if (!prepareInput()) {
        break;
      }
      processOneRow();
    }
    TsBlock tsBlock = null;
    if (tsBlockBuilder.getPositionCount() > 0) {
      tsBlock = tsBlockBuilder.build();
    }
    tsBlockBuilder.reset();
    return tsBlock;
  }

  private void processOneRow() {
    TsBlock[] rowBlocks = new TsBlock[children.size()];
    for (int i = 0; i < children.size(); i++) {
      rowBlocks[i] = inputTsBlocks[i].getRegion(consumedIndices[i], 1);
    }
    for (int groupIdx = 0; groupIdx < groups.size(); groupIdx++) {
      List<TreeAggregator> aggregators = groupedAggregators.get(groupIdx);
      aggregate(aggregators, rowBlocks);
      List<String> group = groups.get(groupIdx);
      appendOneRow(rowBlocks, group, aggregators);
    }

    // Reset dataReady for next iteration
    for (int i = 0; i < children.size(); i++) {
      consumedIndices[i]++;
    }
  }

  private void aggregate(List<TreeAggregator> aggregators, TsBlock[] rowBlocks) {
    for (TreeAggregator aggregator : aggregators) {
      if (aggregator == null) {
        continue;
      }
      aggregator.reset();
      aggregator.processTsBlocks(rowBlocks);
    }
  }

  private void appendOneRow(
      TsBlock[] rowBlocks, List<String> group, List<TreeAggregator> aggregators) {
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(rowBlocks[0].getStartTime());
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    for (int i = 0; i < group.size(); i++) {
      if (group.get(i) == null) {
        columnBuilders[i].writeBinary(new Binary("NULL", TSFileConfig.STRING_CHARSET));
      } else {
        columnBuilders[i].writeBinary(new Binary(group.get(i), TSFileConfig.STRING_CHARSET));
      }
    }
    for (int i = 0; i < aggregators.size(); i++) {
      TreeAggregator aggregator = aggregators.get(i);
      ColumnBuilder columnBuilder = columnBuilders[i + group.size()];
      if (aggregator == null) {
        columnBuilder.appendNull();
      } else {
        aggregator.outputResult(new ColumnBuilder[] {columnBuilder});
      }
    }
    tsBlockBuilder.declarePosition();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isEmpty(readyChildIndex) || children.get(readyChildIndex).hasNextWithTimer();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
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
  protected boolean isEmpty(int index) {
    return inputTsBlocks[index] == null
        || consumedIndices[index] == inputTsBlocks[index].getPositionCount();
  }

  @Override
  protected TsBlock getNextTsBlock(int childIndex) throws Exception {
    consumedIndices[childIndex] = 0;
    return children.get(childIndex).nextWithTimer();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(canCallNext)
        + RamUsageEstimator.sizeOf(consumedIndices)
        + groups.stream()
            .mapToLong(group -> group.stream().mapToLong(RamUsageEstimator::sizeOf).sum())
            .sum()
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
