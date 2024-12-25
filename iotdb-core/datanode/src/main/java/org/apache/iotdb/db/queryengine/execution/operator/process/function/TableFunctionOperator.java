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

import org.apache.iotdb.db.queryengine.common.transformation.TableFunctionPartitionImpl;
import org.apache.iotdb.db.queryengine.common.transformation.TransformationState;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionProcessorState;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

// only one input source is supported now
public class TableFunctionOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TableFunctionDataProcessor processor;
  private final List<TSDataType> outputDataTypes;
  private final int properChannelCount;
  private final List<Integer> requiredChannels;
  private final TableFunctionNode.PassThroughSpecification passThroughSpecifications;
  private final List<Integer> partitionChannels;

  private final TableFunctionPartitionTransformation partitionTransformation;
  private TransformationState<TableFunctionPartitionImpl> partitionState =
      TransformationState.needsMoreData();
  private TableFunctionProcessorState processorState = null;

  private ListenableFuture<?> isBlocked;
  private Optional<TsBlock> currentInputBlock;

  public TableFunctionOperator(
      OperatorContext operatorContext,
      TableFunctionDataProcessor processor,
      Operator inputOperator,
      List<TSDataType> outputDataTypes,
      int properChannelCount,
      List<Integer> requiredChannels,
      TableFunctionNode.PassThroughSpecification passThroughSpecifications,
      List<Integer> partitionChannels) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.processor = processor;
    this.outputDataTypes = outputDataTypes;
    this.properChannelCount = properChannelCount;
    this.requiredChannels = requiredChannels;
    this.passThroughSpecifications = passThroughSpecifications;
    this.partitionChannels = partitionChannels;
    this.partitionTransformation =
        new TableFunctionPartitionTransformation(
            outputDataTypes, requiredChannels, partitionChannels, properChannelCount);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (isBlocked == null) {
      isBlocked = tryGetNextPartition();
    }
    return isBlocked;
  }

  private ListenableFuture<?> tryGetNextPartition() {
    try {
      while (!TransformationState.Type.FINISHED.equals(partitionState.getType())) {
        if (partitionState.isNeedsMoreData()) {
          if (!inputOperator.isBlocked().isDone()) {
            return inputOperator.isBlocked();
          }
          if (inputOperator.hasNextWithTimer()) {
            currentInputBlock = Optional.ofNullable(inputOperator.nextWithTimer());
          } else {
            currentInputBlock = null;
          }
        }
        partitionState = partitionTransformation.process(currentInputBlock);
        if (TransformationState.Type.RESULT.equals(partitionState.getType())) {
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final TableFunctionPartitionImpl partition = partitionState.getResult();
    if (partition == null) {
      // it should never happen
      throw new RuntimeException("TableFunctionPartitionImpl is null");
    }
    List<Column> columns = processor.process(Collections.singletonList(partition));
    isBlocked = null;
    return partition.constructResult(columns);
  }

  @Override
  public boolean hasNext() throws Exception {
    isBlocked().get(); // wait for the next TsBlock
    return TransformationState.Type.RESULT.equals(partitionState.getType());
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean isFinished() throws Exception {
    return TransformationState.Type.FINISHED.equals(partitionState.getType());
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
