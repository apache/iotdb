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

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.createGroupByHash;

/**
 * Streaming operator optimized for {@code PARTITION BY device ORDER BY time} with a top-K filter.
 *
 * <p>Unlike {@link TopKRankingOperator} which buffers all input and uses heap-based selection, this
 * operator exploits the fact that input data is already sorted by (device, time). It simply counts
 * rows per partition and emits the first K rows for each, discarding the rest. This yields O(N)
 * time with minimal memory overhead, compared to O(N log K) for the general TopK path.
 */
public class LimitKRankingOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LimitKRankingOperator.class);

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;

  private final List<Integer> outputChannels;
  private final List<Integer> partitionChannels;
  private final int maxRowCountPerPartition;
  private final boolean produceRowNumber;

  private final Optional<GroupByHash> groupByHash;
  private final Map<Integer, Long> partitionRowCounts;
  private final TsBlockBuilder tsBlockBuilder;

  public LimitKRankingOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<Integer> outputChannels,
      List<Integer> partitionChannels,
      List<TSDataType> partitionTSDataTypes,
      int maxRowCountPerPartition,
      boolean produceRowNumber,
      int expectedPositions) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = inputDataTypes;
    this.outputChannels = ImmutableList.copyOf(outputChannels);
    this.partitionChannels = ImmutableList.copyOf(partitionChannels);
    this.maxRowCountPerPartition = maxRowCountPerPartition;
    this.produceRowNumber = produceRowNumber;

    List<TSDataType> outputDataTypes = new ArrayList<>();
    for (int channel : outputChannels) {
      outputDataTypes.add(inputDataTypes.get(channel));
    }
    if (produceRowNumber) {
      outputDataTypes.add(TSDataType.INT64);
    }
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    if (partitionChannels.isEmpty()) {
      this.groupByHash = Optional.empty();
    } else {
      List<Type> partitionTypes = new ArrayList<>(partitionTSDataTypes.size());
      for (TSDataType tsDataType : partitionTSDataTypes) {
        partitionTypes.add(InternalTypeManager.fromTSDataType(tsDataType));
      }
      this.groupByHash =
          Optional.of(
              createGroupByHash(partitionTypes, false, expectedPositions, UpdateMemory.NOOP));
    }

    this.partitionRowCounts = new HashMap<>(expectedPositions);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock input = inputOperator.nextWithTimer();
    if (input == null) {
      return null;
    }

    int positionCount = input.getPositionCount();
    int[] partitionIds = getPartitionIds(input);

    for (int position = 0; position < positionCount; position++) {
      int partitionId = groupByHash.isPresent() ? partitionIds[position] : 0;
      long rowCount = partitionRowCounts.getOrDefault(partitionId, 0L);

      if (rowCount < maxRowCountPerPartition) {
        emitRow(input, position, rowCount + 1);
      }

      partitionRowCounts.put(partitionId, rowCount + 1);
    }

    if (tsBlockBuilder.getPositionCount() == 0) {
      tsBlockBuilder.reset();
      return null;
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    tsBlockBuilder.reset();
    return result;
  }

  private void emitRow(TsBlock tsBlock, int position, long rowNumber) {
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = tsBlockBuilder.getColumnBuilder(i);
      if (column.isNull(position)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(column, position);
      }
    }

    if (produceRowNumber) {
      tsBlockBuilder.getColumnBuilder(outputChannels.size()).writeLong(rowNumber);
    }

    tsBlockBuilder.declarePosition();
  }

  private int[] getPartitionIds(TsBlock tsBlock) {
    if (groupByHash.isPresent()) {
      Column[] partitionColumns = new Column[partitionChannels.size()];
      for (int i = 0; i < partitionChannels.size(); i++) {
        partitionColumns[i] = tsBlock.getColumn(partitionChannels.get(i));
      }
      return groupByHash.get().getGroupIds(partitionColumns);
    }
    return new int[0];
  }

  @Override
  public boolean hasNext() throws Exception {
    return inputOperator.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    long maxTsBlockSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxTsBlockSize)
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
