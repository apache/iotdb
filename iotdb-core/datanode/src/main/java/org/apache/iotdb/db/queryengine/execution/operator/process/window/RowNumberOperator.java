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

public class RowNumberOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RowNumberOperator.class);

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<Integer> outputChannels;
  private final List<Integer> partitionChannels;
  private final TsBlockBuilder tsBlockBuilder;

  private final Optional<GroupByHash> groupByHash;
  private final Optional<Integer> maxRowsPerPartition;
  private final Map<Integer, Long> partitionRowCounts;

  public RowNumberOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<Integer> outputChannels,
      List<Integer> partitionChannels,
      Optional<Integer> maxRowsPerPartition,
      int expectedPositions) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.outputChannels = ImmutableList.copyOf(outputChannels);
    this.partitionChannels = ImmutableList.copyOf(partitionChannels);
    this.maxRowsPerPartition = maxRowsPerPartition;

    // Output data types
    // original output channels + row number column
    List<TSDataType> outputDataTypes = new ArrayList<>();
    for (int channel : outputChannels) {
      outputDataTypes.add(inputDataTypes.get(channel));
    }
    outputDataTypes.add(TSDataType.INT64);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    if (partitionChannels.isEmpty()) {
      this.groupByHash = Optional.empty();
    } else {
      // Partition data types
      List<Type> partitionDataTypes = new ArrayList<>();
      for (int channel : partitionChannels) {
        TSDataType tsDataType = inputDataTypes.get(channel);
        Type convertType = InternalTypeManager.fromTSDataType(tsDataType);
        partitionDataTypes.add(convertType);
      }
      this.groupByHash =
          Optional.of(
              createGroupByHash(partitionDataTypes, false, expectedPositions, UpdateMemory.NOOP));
    }

    this.partitionRowCounts = new HashMap<>(expectedPositions);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock tsBlock = inputOperator.nextWithTimer();
    if (tsBlock == null) {
      return null;
    }

    int[] partitionIds = getTsBlockPartitionIds(tsBlock);
    for (int position = 0; position < tsBlock.getPositionCount(); position++) {
      int partitionId = groupByHash.isPresent() ? partitionIds[position] : 0;
      long rowCount = partitionRowCounts.getOrDefault(partitionId, 0L);
      processRow(tsBlock, position, rowCount + 1);
      partitionRowCounts.put(partitionId, rowCount + 1);
    }

    TsBlock result =
        tsBlockBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    tsBlockBuilder.reset();
    return result;
  }

  private void processRow(TsBlock tsBlock, int position, long rowNumber) {
    // Check max rows per partition limit
    if (maxRowsPerPartition.isPresent() && rowNumber > maxRowsPerPartition.get()) {
      return; // Skip this row, partition has reached limit
    }

    // Copy origin values
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = tsBlockBuilder.getColumnBuilder(i);
      if (column.isNull(position)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(column, position);
      }
    }
    // Write row number
    int rowNumberChannel = outputChannels.size();
    ColumnBuilder columnBuilder = tsBlockBuilder.getColumnBuilder(rowNumberChannel);
    columnBuilder.writeLong(rowNumber);

    tsBlockBuilder.declarePosition();
  }

  private int[] getTsBlockPartitionIds(TsBlock tsBlock) {
    if (groupByHash.isPresent()) {
      Column[] partitionColumns = new Column[partitionChannels.size()];
      for (int i = 0; i < partitionChannels.size(); i++) {
        partitionColumns[i] = tsBlock.getColumn(partitionChannels.get(i));
      }
      return groupByHash.get().getGroupIds(partitionColumns);
    } else {
      return new int[] {0};
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return inputOperator.hasNext();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    long maxPeekMemoryFromCurrent =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxPeekMemoryFromCurrent)
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

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }
}
