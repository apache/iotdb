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

package org.apache.iotdb.db.queryengine.execution.operator.process.ml;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.ModelInferenceProcessException;
import org.apache.iotdb.db.protocol.client.MLNodeClient;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.mlnode.rpc.thrift.TForecastResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class ForecastOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ForecastOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;

  private final String modelPath;
  private final List<TSDataType> inputTypeList;
  private final List<String> inputColumnNameList;
  private final int expectedPredictLength;

  private final TsBlockBuilder inputTsBlockBuilder;

  private MLNodeClient client;
  private final ExecutorService modelInferenceExecutor;
  private ListenableFuture<TForecastResp> forecastExecutionFuture;

  private boolean finished = false;

  private final long maxRetainedSize;
  private final long maxReturnSize;

  public ForecastOperator(
      OperatorContext operatorContext,
      Operator child,
      String modelPath,
      List<TSDataType> inputTypeList,
      List<String> inputColumnNameList,
      int expectedPredictLength,
      ExecutorService modelInferenceExecutor,
      long maxRetainedSize,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.modelPath = modelPath;
    this.inputTypeList = inputTypeList;
    this.inputColumnNameList = inputColumnNameList;
    this.expectedPredictLength = expectedPredictLength;
    this.inputTsBlockBuilder = new TsBlockBuilder(inputTypeList);
    this.modelInferenceExecutor = modelInferenceExecutor;
    this.maxRetainedSize = maxRetainedSize;
    this.maxReturnSize = maxReturnSize;
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
      return forecastExecutionFuture;
    } else if (executionDone) {
      return childBlocked;
    } else {
      return successfulAsList(Arrays.asList(forecastExecutionFuture, childBlocked));
    }
  }

  private boolean forecastExecutionDone() {
    if (forecastExecutionFuture == null) {
      return true;
    }
    return forecastExecutionFuture.isDone();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished;
  }

  @Override
  public TsBlock next() throws Exception {
    if (forecastExecutionFuture == null) {
      if (child.hasNextWithTimer()) {
        TsBlock inputTsBlock = child.nextWithTimer();
        if (inputTsBlock != null) {
          appendTsBlockToBuilder(inputTsBlock);
        }
      } else {
        submitForecastTask();
      }
      return null;
    } else {
      try {
        if (!forecastExecutionFuture.isDone()) {
          throw new IllegalStateException(
              "The operator cannot continue until the forecast execution is done.");
        }

        TForecastResp forecastResp = forecastExecutionFuture.get();
        if (forecastResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          String message =
              String.format(
                  "Error occurred while executing forecast: %s",
                  forecastResp.getStatus().getMessage());
          throw new ModelInferenceProcessException(message);
        }

        finished = true;
        TsBlock resultTsBlock =
            new TsBlockSerde().deserialize(forecastResp.bufferForForecastResult());
        resultTsBlock = modifyTimeColumn(resultTsBlock);
        return resultTsBlock;
      } catch (InterruptedException e) {
        LOGGER.warn(
            "{}: interrupted when processing write operation future with exception {}", this, e);
        Thread.currentThread().interrupt();
        throw new ModelInferenceProcessException(e.getMessage());
      } catch (ExecutionException e) {
        throw new ModelInferenceProcessException(e.getMessage());
      }
    }
  }

  private TsBlock modifyTimeColumn(TsBlock resultTsBlock) {
    long delta =
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision().equals("ms")
            ? 1_000_000L
            : 1_000L;

    TsBlockBuilder newTsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
    TimeColumnBuilder timeColumnBuilder = newTsBlockBuilder.getTimeColumnBuilder();
    for (int i = 0; i < resultTsBlock.getPositionCount(); i++) {
      timeColumnBuilder.writeLong(resultTsBlock.getTimeByIndex(i) / delta);
      newTsBlockBuilder.declarePosition();
    }
    return newTsBlockBuilder.build().appendValueColumns(resultTsBlock.getValueColumns());
  }

  private void appendTsBlockToBuilder(TsBlock inputTsBlock) {
    TimeColumnBuilder timeColumnBuilder = inputTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = inputTsBlockBuilder.getValueColumnBuilders();

    for (int i = 0; i < inputTsBlock.getPositionCount(); i++) {
      timeColumnBuilder.writeLong(inputTsBlock.getTimeByIndex(i));
      for (int columnIndex = 0; columnIndex < inputTsBlock.getValueColumnCount(); columnIndex++) {
        columnBuilders[columnIndex].write(inputTsBlock.getColumn(columnIndex), i);
      }
      inputTsBlockBuilder.declarePosition();
    }
  }

  private void submitForecastTask() {
    try {
      if (client == null) {
        client = new MLNodeClient();
      }
    } catch (TException e) {
      throw new ModelInferenceProcessException(e.getMessage());
    }

    TsBlock inputTsBlock = inputTsBlockBuilder.build();
    inputTsBlock.reverse();

    forecastExecutionFuture =
        Futures.submit(
            () ->
                client.forecast(
                    modelPath,
                    inputTsBlock,
                    inputTypeList,
                    inputColumnNameList,
                    expectedPredictLength),
            modelInferenceExecutor);
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public void close() throws Exception {
    client.close();
    if (forecastExecutionFuture != null) {
      forecastExecutionFuture.cancel(true);
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
}
