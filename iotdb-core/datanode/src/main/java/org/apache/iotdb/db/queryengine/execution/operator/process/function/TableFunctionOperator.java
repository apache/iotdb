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

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.PartitionState;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.SliceCache;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

// only one input source is supported now
public class TableFunctionOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TableFunctionProcessorProvider processorProvider;
  private final PartitionRecognizer partitionRecognizer;
  private final TsBlockBuilder blockBuilder;
  private final int properChannelCount;
  private final boolean needPassThrough;

  private TableFunctionDataProcessor processor;
  private PartitionState partitionState;
  private ListenableFuture<?> isBlocked;
  private boolean finished = false;

  private SliceCache sliceCache;

  public TableFunctionOperator(
      OperatorContext operatorContext,
      TableFunctionProcessorProvider processorProvider,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      int properChannelCount,
      List<Integer> requiredChannels,
      List<Integer> passThroughChannels,
      List<Integer> partitionChannels) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.properChannelCount = properChannelCount;
    this.processorProvider = processorProvider;
    this.partitionRecognizer =
        new PartitionRecognizer(
            partitionChannels, requiredChannels, passThroughChannels, inputDataTypes);
    this.needPassThrough = properChannelCount != outputDataTypes.size();
    this.partitionState = null;
    this.blockBuilder = new TsBlockBuilder(outputDataTypes);
    this.sliceCache = new SliceCache();
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
        TsBlock tsBlock = buildTsBlock(properColumnBuilders, passThroughIndexBuilder);
        sliceCache.clear();
        consumeCurrentPartitionState();
        return tsBlock;
      }
      if (stateType == PartitionState.StateType.NEW_PARTITION) {
        if (processor != null) {
          // previous partition state has not finished consuming yet
          processor.finish(properColumnBuilders, passThroughIndexBuilder);
          TsBlock tsBlock = buildTsBlock(properColumnBuilders, passThroughIndexBuilder);
          sliceCache.clear();
          processor = null;
          return tsBlock;
        } else {
          processor = processorProvider.getDataProcessor();
          processor.beforeStart();
        }
      }
      sliceCache.addSlice(slice);
      Iterator<Record> recordIterator = slice.getRequiredRecordIterator();
      while (recordIterator.hasNext()) {
        processor.process(recordIterator.next(), properColumnBuilders, passThroughIndexBuilder);
      }
      consumeCurrentPartitionState();
      return buildTsBlock(properColumnBuilders, passThroughIndexBuilder);
    }
  }

  private List<ColumnBuilder> getProperColumnBuilders() {
    blockBuilder.reset();
    return Arrays.asList(blockBuilder.getValueColumnBuilders()).subList(0, properChannelCount);
  }

  private ColumnBuilder getPassThroughIndexBuilder() {
    return new LongColumnBuilder(null, 1);
  }

  private TsBlock buildTsBlock(
      List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
    List<ColumnBuilder> passThroughColumnBuilders =
        Arrays.asList(blockBuilder.getValueColumnBuilders())
            .subList(properChannelCount, blockBuilder.getValueColumnBuilders().length);
    int positionCount = 0;
    if (properChannelCount > 0) {
      // if there is proper column, use its position count
      positionCount = properColumnBuilders.get(0).getPositionCount();
    } else if (needPassThrough) {
      // if there is no proper column, use pass through column's position count
      positionCount = passThroughIndexBuilder.getPositionCount();
    }
    if (positionCount == 0) {
      return null;
    }
    blockBuilder.declarePositions(positionCount);
    if (needPassThrough) {
      // handle pass through column only if needed
      Column passThroughIndex = passThroughIndexBuilder.build();
      for (Column[] passThroughColumns : sliceCache.getPassThroughResult(passThroughIndex)) {
        for (int i = 0; i < passThroughColumns.length; i++) {
          ColumnBuilder passThroughColumnBuilder = passThroughColumnBuilders.get(i);
          for (int j = 0; j < passThroughColumns[i].getPositionCount(); j++) {
            if (passThroughColumns[i].isNull(j)) {
              passThroughColumnBuilder.appendNull();
            } else {
              passThroughColumnBuilder.write(passThroughColumns[i], j);
            }
          }
        }
      }
    }
    return blockBuilder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount));
  }

  private void consumeCurrentPartitionState() {
    partitionState = null;
  }

  private void consumeCurrentSourceTsBlock() {
    isBlocked = null;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (partitionState == null) {
      isBlocked().get(); // wait for the next TsBlock
      partitionState = partitionRecognizer.nextState();
    }
    return !finished;
  }

  @Override
  public void close() throws Exception {
    sliceCache.close();
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
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
