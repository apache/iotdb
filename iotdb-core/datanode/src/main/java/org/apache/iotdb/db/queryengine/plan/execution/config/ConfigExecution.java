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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class ConfigExecution implements IQueryExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigExecution.class);

  private static final TsBlockSerde serde = new TsBlockSerde();

  private final MPPQueryContext context;
  private final ExecutorService executor;

  private final QueryStateMachine stateMachine;
  private final SettableFuture<ConfigTaskResult> taskFuture;
  private TsBlock resultSet;
  private DatasetHeader datasetHeader;
  private boolean resultSetConsumed;
  private final IConfigTask task;
  private final IConfigTaskExecutor configTaskExecutor;

  private final StatementType statementType;
  private long totalExecutionTime;

  public ConfigExecution(
      MPPQueryContext context,
      StatementType statementType,
      ExecutorService executor,
      IConfigTask task) {
    this.context = context;
    this.statementType = statementType;
    this.executor = executor;
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);
    this.taskFuture = SettableFuture.create();
    this.task = task;
    this.resultSetConsumed = false;
    configTaskExecutor = ClusterConfigTaskExecutor.getInstance();
  }

  @TestOnly
  public ConfigExecution(MPPQueryContext context, ExecutorService executor, IConfigTask task) {
    this(context, StatementType.NULL, executor, task);
  }

  @Override
  public void start() {
    try {
      ListenableFuture<ConfigTaskResult> future = task.execute(configTaskExecutor);
      Futures.addCallback(
          future,
          new FutureCallback<ConfigTaskResult>() {
            @Override
            public void onSuccess(ConfigTaskResult taskRet) {
              stateMachine.transitionToFinished();
              taskFuture.set(taskRet);
            }

            @Override
            public void onFailure(@NotNull Throwable throwable) {
              fail(throwable);
            }
          },
          executor);
    } catch (Throwable e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      fail(e);
    }
  }

  private void fail(Throwable cause) {
    LOGGER.warn("Failures happened during running ConfigExecution.", cause);
    stateMachine.transitionToFailed(cause);
    ConfigTaskResult result;
    if (cause instanceof IoTDBException) {
      result =
          new ConfigTaskResult(TSStatusCode.representOf(((IoTDBException) cause).getErrorCode()));
    } else if (cause instanceof StatementExecutionException) {
      result =
          new ConfigTaskResult(
              TSStatusCode.representOf(((StatementExecutionException) cause).getStatusCode()));
    } else {
      result = new ConfigTaskResult(TSStatusCode.INTERNAL_SERVER_ERROR);
    }
    taskFuture.set(result);
  }

  @Override
  public void stop(Throwable t) {
    // do nothing
  }

  @Override
  public void stopAndCleanup() {
    // do nothing
  }

  @Override
  public void stopAndCleanup(Throwable t) {
    // do nothing
  }

  @Override
  public void cancel() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ExecutionResult getStatus() {
    try {
      ConfigTaskResult taskResult = taskFuture.get();
      TSStatusCode statusCode = taskResult.getStatusCode();
      resultSet = taskResult.getResultSet();
      datasetHeader = taskResult.getResultSetHeader();
      String message =
          statusCode == TSStatusCode.SUCCESS_STATUS ? "" : stateMachine.getFailureMessage();
      return new ExecutionResult(context.getQueryId(), RpcUtils.getStatus(statusCode, message));
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return new ExecutionResult(
          context.getQueryId(),
          RpcUtils.getStatus(TSStatusCode.QUERY_PROCESS_ERROR, e.getMessage()));
    }
  }

  @Override
  public Optional<TsBlock> getBatchResult() {
    if (!resultSetConsumed) {
      resultSetConsumed = true;
      return Optional.of(resultSet);
    }
    return Optional.empty();
  }

  @Override
  public Optional<ByteBuffer> getByteBufferBatchResult() throws IoTDBException {
    if (!resultSetConsumed) {
      resultSetConsumed = true;
      try {
        return Optional.of(serde.serialize(resultSet));
      } catch (IOException e) {
        throw new IoTDBException(e, TSStatusCode.TSBLOCK_SERIALIZE_ERROR.getStatusCode());
      }
    }
    return Optional.empty();
  }

  // According to the execution process of ConfigExecution, there is only one TsBlock for
  // this execution. Thus, the hasNextResult will be false once the TsBlock is consumed
  @Override
  public boolean hasNextResult() {
    return !resultSetConsumed && resultSet != null;
  }

  @Override
  public int getOutputValueColumnCount() {
    return datasetHeader.getOutputValueColumnCount();
  }

  @Override
  public DatasetHeader getDatasetHeader() {
    return datasetHeader;
  }

  @Override
  public boolean isQuery() {
    return context.getQueryType() == QueryType.READ;
  }

  @Override
  public String getQueryId() {
    return context.getQueryId().getId();
  }

  @Override
  public long getStartExecutionTime() {
    return context.getStartTime();
  }

  @Override
  public void recordExecutionTime(long executionTime) {
    totalExecutionTime += executionTime;
  }

  @Override
  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  @Override
  public Optional<String> getExecuteSQL() {
    return Optional.ofNullable(context.getSql());
  }

  @Override
  public String getStatementType() {
    return statementType == null ? null : statementType.name();
  }
}
