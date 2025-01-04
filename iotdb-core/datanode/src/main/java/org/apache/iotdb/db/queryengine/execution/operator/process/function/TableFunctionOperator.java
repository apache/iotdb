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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

// only one input source is supported now
public class TableFunctionOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TableFunctionProcessorProvider processorProvider;
  private final List<TSDataType> outputDataTypes;
  private final int properChannelCount;
  private final List<Integer> requiredChannels;
  private final TableFunctionNode.PassThroughSpecification passThroughSpecifications;
  private final List<Integer> partitionChannels;
  private final PartitionRecognizer partitionRecognizer;
  private final TsBlockBuilder blockBuilder;

  private TableFunctionDataProcessor processor;
  private PartitionState partitionState;
  private ListenableFuture<?> isBlocked;
  private boolean finished = false;

  public TableFunctionOperator(
      OperatorContext operatorContext,
      TableFunctionProcessorProvider processorProvider,
      Operator inputOperator,
      List<TSDataType> outputDataTypes,
      int properChannelCount,
      List<Integer> requiredChannels,
      TableFunctionNode.PassThroughSpecification passThroughSpecifications,
      List<Integer> partitionChannels) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.processorProvider = processorProvider;
    this.outputDataTypes = outputDataTypes;
    this.properChannelCount = properChannelCount;
    this.requiredChannels = requiredChannels;
    this.passThroughSpecifications = passThroughSpecifications;
    this.partitionChannels = partitionChannels;
    this.partitionRecognizer = new PartitionRecognizer(partitionChannels);
    this.partitionState = PartitionState.INIT_STATE;
    this.blockBuilder = new TsBlockBuilder(outputDataTypes);
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
    if (stateType == PartitionState.StateType.INIT
        || stateType == PartitionState.StateType.NEED_MORE_DATA) {
      isBlocked = null;
      return null;
    } else {
      List<ColumnBuilder> columnBuilders = getOutputColumnBuilders();
      if (stateType == PartitionState.StateType.FINISHED) {
        if (processor != null) {
          processor.finish(columnBuilders);
        }
        finished = true;
        return buildTsBlock();
      }
      if (stateType == PartitionState.StateType.NEW_PARTITION) {
        if (processor != null) {
          processor.finish(columnBuilders);
        }
        processor = processorProvider.getDataProcessor();
      }
      Iterator<Record> recordIterator = partitionState.getRecordIterator();
      while (recordIterator.hasNext()) {
        processor.process(recordIterator.next(), columnBuilders);
      }
      return buildTsBlock();
    }
  }

  private List<ColumnBuilder> getOutputColumnBuilders() {
    blockBuilder.reset();
    return Arrays.asList(blockBuilder.getValueColumnBuilders());
  }

  private TsBlock buildTsBlock() {
    // TODO(UDF): it should be implemented in the other way
    int positionCount = getOutputColumnBuilders().get(0).build().getPositionCount();
    blockBuilder.declarePositions(positionCount);
    return blockBuilder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount));
  }

  @Override
  public boolean hasNext() throws Exception {
    isBlocked().get(); // wait for the next TsBlock
    partitionState = partitionRecognizer.getState();
    return !finished;
  }

  @Override
  public void close() throws Exception {}

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
