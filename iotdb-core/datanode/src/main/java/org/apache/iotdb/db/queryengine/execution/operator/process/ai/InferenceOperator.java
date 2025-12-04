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

package org.apache.iotdb.db.queryengine.execution.operator.process.ai;

import org.apache.iotdb.ainode.rpc.thrift.TInferenceReq;
import org.apache.iotdb.ainode.rpc.thrift.TInferenceResp;
import org.apache.iotdb.db.exception.runtime.ModelInferenceProcessException;
import org.apache.iotdb.db.protocol.client.an.AINodeClient;
import org.apache.iotdb.db.protocol.client.an.AINodeClientManager;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class InferenceOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(InferenceOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;
  private final ModelInferenceDescriptor modelInferenceDescriptor;

  private final TsBlockBuilder inputTsBlockBuilder;

  private final ExecutorService modelInferenceExecutor;
  private ListenableFuture<TInferenceResp> inferenceExecutionFuture;

  private boolean finished = false;

  private final long maxRetainedSize;
  private final long maxReturnSize;
  private final int[] columnIndexes;
  private long totalRow;
  private int resultIndex = 0;
  private List<ByteBuffer> results;
  private final TsBlockSerde serde = new TsBlockSerde();

  private final boolean generateTimeColumn;
  private long maxTimestamp;
  private long minTimestamp;
  private long interval;
  private long currentRowIndex;

  public InferenceOperator(
      OperatorContext operatorContext,
      Operator child,
      ModelInferenceDescriptor modelInferenceDescriptor,
      ExecutorService modelInferenceExecutor,
      List<String> targetColumnNames,
      List<String> inputColumnNames,
      boolean generateTimeColumn,
      long maxRetainedSize,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.modelInferenceDescriptor = modelInferenceDescriptor;
    this.inputTsBlockBuilder =
        new TsBlockBuilder(
            Arrays.asList(modelInferenceDescriptor.getModelInformation().getInputDataType()));
    this.modelInferenceExecutor = modelInferenceExecutor;
    this.columnIndexes = new int[inputColumnNames.size()];
    for (int i = 0; i < inputColumnNames.size(); i++) {
      columnIndexes[i] = targetColumnNames.indexOf(inputColumnNames.get(i));
    }

    this.maxRetainedSize = maxRetainedSize;
    this.maxReturnSize = maxReturnSize;
    this.totalRow = 0;

    if (generateTimeColumn) {
      this.interval = 0;
      this.minTimestamp = Long.MAX_VALUE;
      this.maxTimestamp = Long.MIN_VALUE;
      this.currentRowIndex = 0;
    }
    this.generateTimeColumn = generateTimeColumn;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> childBlocked = child.isBlocked();
    boolean childDone = childBlocked.isDone();
    boolean executionDone = forecastExecutionDone();
    if (executionDone && childDone) {
      return NOT_BLOCKED;
    } else if (childDone) {
      return inferenceExecutionFuture;
    } else if (executionDone) {
      return childBlocked;
    } else {
      return successfulAsList(Arrays.asList(inferenceExecutionFuture, childBlocked));
    }
  }

  private boolean forecastExecutionDone() {
    if (inferenceExecutionFuture == null) {
      return true;
    }
    return inferenceExecutionFuture.isDone();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished || (results != null && results.size() != resultIndex);
  }

  private void fillTimeColumn(TsBlock tsBlock) {
    Column timeColumn = tsBlock.getTimeColumn();
    long[] time = timeColumn.getLongs();
    for (int i = 0; i < time.length; i++) {
      time[i] = maxTimestamp + interval * currentRowIndex;
      currentRowIndex++;
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (inferenceExecutionFuture == null) {
      if (child.hasNextWithTimer()) {
        TsBlock inputTsBlock = child.nextWithTimer();
        if (inputTsBlock != null) {
          appendTsBlockToBuilder(inputTsBlock);
        }
      } else {
        submitInferenceTask();
      }
      return null;
    } else {

      if (results != null && resultIndex != results.size()) {
        TsBlock tsBlock = serde.deserialize(results.get(resultIndex));
        if (generateTimeColumn) {
          fillTimeColumn(tsBlock);
        }
        resultIndex++;
        return tsBlock;
      }

      try {
        if (!inferenceExecutionFuture.isDone()) {
          throw new IllegalStateException(
              "The operator cannot continue until the forecast execution is done.");
        }

        TInferenceResp inferenceResp = inferenceExecutionFuture.get();
        if (inferenceResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          String message =
              String.format(
                  "Error occurred while executing inference:[%s]",
                  inferenceResp.getStatus().getMessage());
          throw new ModelInferenceProcessException(message);
        }

        finished = true;
        TsBlock resultTsBlock = serde.deserialize(inferenceResp.inferenceResult.get(0));
        if (generateTimeColumn) {
          fillTimeColumn(resultTsBlock);
        }
        results = inferenceResp.inferenceResult;
        resultIndex++;
        return resultTsBlock;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ModelInferenceProcessException(e.getMessage());
      } catch (ExecutionException e) {
        throw new ModelInferenceProcessException(e.getMessage());
      }
    }
  }

  private void appendTsBlockToBuilder(TsBlock inputTsBlock) {
    TimeColumnBuilder timeColumnBuilder = inputTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = inputTsBlockBuilder.getValueColumnBuilders();
    totalRow += inputTsBlock.getPositionCount();
    for (int i = 0; i < inputTsBlock.getPositionCount(); i++) {
      long timestamp = inputTsBlock.getTimeByIndex(i);
      if (generateTimeColumn) {
        minTimestamp = Math.min(minTimestamp, timestamp);
        maxTimestamp = Math.max(maxTimestamp, timestamp);
      }
      timeColumnBuilder.writeLong(timestamp);
      for (int columnIndex = 0; columnIndex < inputTsBlock.getValueColumnCount(); columnIndex++) {
        columnBuilders[columnIndexes[columnIndex]].write(inputTsBlock.getColumn(columnIndex), i);
      }
      inputTsBlockBuilder.declarePosition();
    }
  }

  private void submitInferenceTask() {

    if (generateTimeColumn) {
      interval = (maxTimestamp - minTimestamp) / totalRow;
    }

    TsBlock inputTsBlock = inputTsBlockBuilder.build();

    inferenceExecutionFuture =
        Futures.submit(
            () -> {
              try (AINodeClient client =
                  AINodeClientManager.getInstance()
                      .borrowClient(AINodeClientManager.AINODE_ID_PLACEHOLDER)) {
                return client.inference(
                    new TInferenceReq(
                            modelInferenceDescriptor.getModelId(), serde.serialize(inputTsBlock))
                        .setInferenceAttributes(modelInferenceDescriptor.getInferenceAttributes()));
              } catch (Exception e) {
                throw new ModelInferenceProcessException(e.getMessage());
              }
            },
            modelInferenceExecutor);
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished && !hasNext();
  }

  @Override
  public void close() throws Exception {
    if (inferenceExecutionFuture != null) {
      inferenceExecutionFuture.cancel(true);
    }
    child.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + inputTsBlockBuilder.getRetainedSizeInBytes()
        + (long) columnIndexes.length * Integer.BYTES;
  }
}
