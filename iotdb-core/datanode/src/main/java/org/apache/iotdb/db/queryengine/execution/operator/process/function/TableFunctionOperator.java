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

package org.apache.iotdb.db.queryengine.execution.operator.process.function;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.AggregationMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionCache;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

// only one input source is supported now
public class TableFunctionOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AggregationMergeSortOperator.class);

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TableFunctionProcessorProvider processorProvider;
  private final PartitionRecognizer partitionRecognizer;
  private final TsBlockBuilder properBlockBuilder;
  private final int properChannelCount;
  private final boolean needPassThrough;
  private final PartitionCache partitionCache;
  private final boolean requireRecordSnapshot;
  private final boolean isDeclaredAsPassThrough;

  private TableFunctionDataProcessor processor;
  private PartitionState partitionState;
  private ListenableFuture<?> isBlocked;
  private boolean finished = false;

  private final Queue<TsBlock> resultTsBlocks;

  public TableFunctionOperator(
      OperatorContext operatorContext,
      TableFunctionProcessorProvider processorProvider,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      int properChannelCount,
      List<Integer> requiredChannels,
      List<Integer> passThroughChannels,
      boolean isDeclaredAsPassThrough,
      List<Integer> partitionChannels,
      boolean requireRecordSnapshot) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.properChannelCount = properChannelCount;
    this.processorProvider = processorProvider;
    this.partitionRecognizer =
        new PartitionRecognizer(
            partitionChannels, requiredChannels, passThroughChannels, inputDataTypes);
    this.isDeclaredAsPassThrough = isDeclaredAsPassThrough;
    this.needPassThrough = properChannelCount != outputDataTypes.size();
    this.partitionState = null;
    this.properBlockBuilder = new TsBlockBuilder(outputDataTypes.subList(0, properChannelCount));
    this.partitionCache = new PartitionCache();
    this.resultTsBlocks = new LinkedList<>();
    this.requireRecordSnapshot = requireRecordSnapshot;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (isBlocked == null) {
      isBlocked = tryGetNextTsBlock();
    }
    return isBlocked;
  }

  private ListenableFuture<?> tryGetNextTsBlock() {
    try {
      if (inputOperator.isFinished()) {
        partitionRecognizer.noMoreData();
        return NOT_BLOCKED;
      }
      if (!inputOperator.isBlocked().isDone()) {
        return inputOperator.isBlocked();
      }
      if (inputOperator.hasNextWithTimer()) {
        partitionRecognizer.addTsBlock(inputOperator.nextWithTimer());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!resultTsBlocks.isEmpty()) {
      return resultTsBlocks.poll();
    }
    if (partitionState == null) {
      partitionState = partitionRecognizer.nextState();
    }
    PartitionState.StateType stateType = partitionState.getStateType();
    Slice slice = partitionState.getSlice();
    if (stateType == PartitionState.StateType.INIT
        || stateType == PartitionState.StateType.NEED_MORE_DATA) {
      consumeCurrentPartitionState();
      consumeCurrentSourceTsBlock();
      return null;
    } else {
      List<ColumnBuilder> properColumnBuilders = getProperColumnBuilders();
      ColumnBuilder passThroughIndexBuilder = getPassThroughIndexBuilder();
      if (stateType == PartitionState.StateType.FINISHED) {
        if (processor != null) {
          processor.finish(properColumnBuilders, passThroughIndexBuilder);
        }
        finished = true;
        resultTsBlocks.addAll(buildTsBlock(properColumnBuilders, passThroughIndexBuilder));
        partitionCache.clear();
        consumeCurrentPartitionState();
        return resultTsBlocks.poll();
      }
      if (stateType == PartitionState.StateType.NEW_PARTITION) {
        if (processor != null) {
          // previous partition state has not finished consuming yet
          processor.finish(properColumnBuilders, passThroughIndexBuilder);
          resultTsBlocks.addAll(buildTsBlock(properColumnBuilders, passThroughIndexBuilder));
          partitionCache.clear();
          processor.beforeDestroy();
          processor = null;
          return resultTsBlocks.poll();
        } else {
          processor = processorProvider.getDataProcessor();
          processor.beforeStart();
        }
      }
      partitionCache.addSlice(slice);
      Iterator<Record> recordIterator = slice.getRequiredRecordIterator(requireRecordSnapshot);
      while (recordIterator.hasNext()) {
        processor.process(recordIterator.next(), properColumnBuilders, passThroughIndexBuilder);
      }
      consumeCurrentPartitionState();
      resultTsBlocks.addAll(buildTsBlock(properColumnBuilders, passThroughIndexBuilder));
      return resultTsBlocks.poll();
    }
  }

  private List<ColumnBuilder> getProperColumnBuilders() {
    return Arrays.asList(properBlockBuilder.getValueColumnBuilders());
  }

  private ColumnBuilder getPassThroughIndexBuilder() {
    return isDeclaredAsPassThrough ? new LongColumnBuilder(null, 1) : null;
  }

  private List<TsBlock> buildTsBlock(
      List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
    int positionCount = 0;
    if (properChannelCount > 0) {
      // if there is proper column, use its position count
      positionCount = properColumnBuilders.get(0).getPositionCount();
    } else if (isDeclaredAsPassThrough) {
      // if there is no proper column, use pass through column's position count
      positionCount = passThroughIndexBuilder.getPositionCount();
    }
    if (positionCount == 0) {
      return Collections.emptyList();
    }
    properBlockBuilder.declarePositions(positionCount);
    TsBlock properBlock =
        properBlockBuilder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount));
    List<TsBlock> result = new ArrayList<>();
    if (needPassThrough) {
      // handle pass through column only if needed
      int builtCount = 0;
      Column passThroughIndex;
      if (isDeclaredAsPassThrough) {
        passThroughIndex = passThroughIndexBuilder.build();
      } else {
        passThroughIndex =
            new RunLengthEncodedColumn(
                new LongColumn(1, Optional.empty(), new long[] {0}), positionCount);
      }
      for (Column[] passThroughColumns : partitionCache.getPassThroughResult(passThroughIndex)) {
        int subBlockPositionCount = passThroughColumns[0].getPositionCount();
        TsBlock subProperBlock = properBlock.getRegion(builtCount, subBlockPositionCount);
        builtCount += subBlockPositionCount;
        result.add(subProperBlock.appendValueColumns(passThroughColumns));
      }
    } else {
      // split the proper block into smaller blocks
      result.add(properBlock);
    }
    properBlockBuilder.reset();
    return result;
  }

  private void consumeCurrentPartitionState() {
    partitionState = null;
  }

  private void consumeCurrentSourceTsBlock() {
    isBlocked = null;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished || !resultTsBlocks.isEmpty();
  }

  @Override
  public void close() throws Exception {
    partitionCache.close();
    inputOperator.close();
    if (processor != null) {
      processor.beforeDestroy();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return inputOperator.calculateMaxPeekMemory()
        + Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, properBlockBuilder.getRetainedSizeInBytes());
  }

  @Override
  public long calculateMaxReturnSize() {
    return Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, properBlockBuilder.getRetainedSizeInBytes());
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + properBlockBuilder.getRetainedSizeInBytes()
        + partitionCache.getEstimatedSize();
  }
}
