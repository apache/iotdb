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

import org.apache.iotdb.ainode.rpc.thrift.TInferenceResp;
import org.apache.iotdb.ainode.rpc.thrift.TWindowParams;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.db.exception.runtime.ModelInferenceProcessException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.BottomInferenceWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.CountInferenceWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindowType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
  private final List<String> inputColumnNames;
  private final List<String> targetColumnNames;
  private long totalRow;
  private int resultIndex = 0;
  private List<ByteBuffer> results;
  private final TsBlockSerde serde = new TsBlockSerde();
  private InferenceWindowType windowType = null;

  public InferenceOperator(
      OperatorContext operatorContext,
      Operator child,
      ModelInferenceDescriptor modelInferenceDescriptor,
      ExecutorService modelInferenceExecutor,
      List<String> targetColumnNames,
      List<String> inputColumnNames,
      long maxRetainedSize,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.modelInferenceDescriptor = modelInferenceDescriptor;
    this.inputTsBlockBuilder =
        new TsBlockBuilder(
            Arrays.asList(modelInferenceDescriptor.getModelInformation().getInputDataType()));
    this.modelInferenceExecutor = modelInferenceExecutor;
    this.targetColumnNames = targetColumnNames;
    this.inputColumnNames = inputColumnNames;
    this.maxRetainedSize = maxRetainedSize;
    this.maxReturnSize = maxReturnSize;
    this.totalRow = 0;

    if (modelInferenceDescriptor.getInferenceWindowParameter() != null) {
      windowType = modelInferenceDescriptor.getInferenceWindowParameter().getWindowType();
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> childBlocked = child.isBlocked();
    boolean executionDone = forecastExecutionDone();
    if (executionDone && childBlocked.isDone()) {
      return NOT_BLOCKED;
    } else if (childBlocked.isDone()) {
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
      timeColumnBuilder.writeLong(inputTsBlock.getTimeByIndex(i));
      for (int columnIndex = 0; columnIndex < inputTsBlock.getValueColumnCount(); columnIndex++) {
        columnBuilders[columnIndex].write(inputTsBlock.getColumn(columnIndex), i);
      }
      inputTsBlockBuilder.declarePosition();
    }
  }

  private TWindowParams getWindowParams() {
    TWindowParams windowParams;
    if (windowType == null) {
      return null;
    }
    if (windowType == InferenceWindowType.COUNT) {
      CountInferenceWindowParameter countInferenceWindowParameter =
          (CountInferenceWindowParameter) modelInferenceDescriptor.getInferenceWindowParameter();
      windowParams = new TWindowParams();
      windowParams.setWindowInterval((int) countInferenceWindowParameter.getInterval());
      windowParams.setWindowStep((int) countInferenceWindowParameter.getStep());
    } else {
      windowParams = null;
    }
    return windowParams;
  }

  private TsBlock preProcess(TsBlock inputTsBlock) {
    boolean notBuiltIn = !modelInferenceDescriptor.getModelInformation().isBuiltIn();
    if (windowType == null || windowType == InferenceWindowType.HEAD) {
      if (notBuiltIn
          && totalRow != modelInferenceDescriptor.getModelInformation().getInputShape()[0]) {
        throw new ModelInferenceProcessException(
            String.format(
                "The number of rows %s in the input data does not match the model input %s. Try to use LIMIT in SQL or WINDOW in CALL INFERENCE",
                totalRow, modelInferenceDescriptor.getModelInformation().getInputShape()[0]));
      }
      return inputTsBlock;
    } else if (windowType == InferenceWindowType.COUNT) {
      if (notBuiltIn
          && totalRow < modelInferenceDescriptor.getModelInformation().getInputShape()[0]) {
        throw new ModelInferenceProcessException(
            String.format(
                "The number of rows %s in the input data is less than the model input %s. ",
                totalRow, modelInferenceDescriptor.getModelInformation().getInputShape()[0]));
      }
    } else if (windowType == InferenceWindowType.TAIL) {
      if (notBuiltIn
          && totalRow < modelInferenceDescriptor.getModelInformation().getInputShape()[0]) {
        throw new ModelInferenceProcessException(
            String.format(
                "The number of rows %s in the input data is less than the model input %s. ",
                totalRow, modelInferenceDescriptor.getModelInformation().getInputShape()[0]));
      }
      // Tail window logic: get the latest data for inference
      long windowSize =
          (int)
              ((BottomInferenceWindowParameter)
                      modelInferenceDescriptor.getInferenceWindowParameter())
                  .getWindowSize();
      return inputTsBlock.subTsBlock((int) (totalRow - windowSize));
    }
    return inputTsBlock;
  }

  private void submitInferenceTask() {

    TsBlock inputTsBlock = inputTsBlockBuilder.build();

    TsBlock finalInputTsBlock = preProcess(inputTsBlock);
    TWindowParams windowParams = getWindowParams();

    Map<String, Integer> columnNameIndexMap = new HashMap<>();

    for (int i = 0; i < inputColumnNames.size(); i++) {
      columnNameIndexMap.put(inputColumnNames.get(i), i);
    }

    inferenceExecutionFuture =
        Futures.submit(
            () -> {
              try (AINodeClient client =
                  AINodeClientManager.getInstance()
                      .borrowClient(modelInferenceDescriptor.getTargetAINode())) {
                return client.inference(
                    modelInferenceDescriptor.getModelName(),
                    targetColumnNames,
                    Arrays.stream(modelInferenceDescriptor.getModelInformation().getInputDataType())
                        .map(TSDataType::toString)
                        .collect(Collectors.toList()),
                    columnNameIndexMap,
                    finalInputTsBlock,
                    modelInferenceDescriptor.getInferenceAttributes(),
                    windowParams);
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
        + (inputColumnNames == null
            ? 0
            : inputColumnNames.stream().mapToLong(RamUsageEstimator::sizeOf).sum())
        + (targetColumnNames == null
            ? 0
            : targetColumnNames.stream().mapToLong(RamUsageEstimator::sizeOf).sum());
  }
}
