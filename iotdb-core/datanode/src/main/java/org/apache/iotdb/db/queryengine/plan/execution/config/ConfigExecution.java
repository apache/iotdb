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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.protocol.session.IClientSession;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class ConfigExecution implements IQueryExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigExecution.class);

  private static final TsBlockSerde serde = new TsBlockSerde();
  private static final Set<Integer> userExceptionCodes =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(),
                  TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode(),
                  TSStatusCode.DATABASE_CONFLICT.getStatusCode(),
                  TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode(),
                  TSStatusCode.PATH_NOT_EXIST.getStatusCode(),
                  TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode(),
                  TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode(),
                  TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode(),
                  TSStatusCode.TABLE_NOT_EXISTS.getStatusCode(),
                  TSStatusCode.COLUMN_ALREADY_EXISTS.getStatusCode(),
                  TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(),
                  TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode(),
                  TSStatusCode.DATABASE_MODEL.getStatusCode(),
                  TSStatusCode.DATABASE_CONFLICT.getStatusCode(),
                  TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode(),
                  TSStatusCode.TEMPLATE_NOT_SET.getStatusCode(),
                  TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode(),
                  TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
                  TSStatusCode.TEMPLATE_NOT_ACTIVATED.getStatusCode(),
                  TSStatusCode.USER_ALREADY_EXIST.getStatusCode(),
                  TSStatusCode.USER_NOT_EXIST.getStatusCode(),
                  TSStatusCode.NO_PERMISSION.getStatusCode(),
                  TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode(),
                  TSStatusCode.ROLE_ALREADY_EXIST.getStatusCode(),
                  TSStatusCode.ROLE_NOT_EXIST.getStatusCode(),
                  TSStatusCode.USER_ALREADY_HAS_ROLE.getStatusCode(),
                  TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode(),
                  TSStatusCode.NOT_HAS_PRIVILEGE_GRANTOPT.getStatusCode(),
                  TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
                  TSStatusCode.NO_SUCH_QUERY.getStatusCode())));

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

  private void fail(final Throwable cause) {
    int errorCode = TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
    TSStatus status = null;
    if (cause instanceof IoTDBException) {
      if (Objects.nonNull(((IoTDBException) cause).getStatus())) {
        status = ((IoTDBException) cause).getStatus();
        errorCode = status.getCode();
      } else {
        errorCode = ((IoTDBException) cause).getErrorCode();
      }
    } else if (cause instanceof IoTDBRuntimeException) {
      if (Objects.nonNull(((IoTDBRuntimeException) cause).getStatus())) {
        status = ((IoTDBRuntimeException) cause).getStatus();
        errorCode = status.getCode();
      } else {
        errorCode = ((IoTDBRuntimeException) cause).getErrorCode();
      }
    }
    if ((Objects.nonNull(status) && isUserException(status))
        || userExceptionCodes.contains(errorCode)) {
      LOGGER.info(
          "Failures happened during running ConfigExecution when executing {}, message: {}, status: {}",
          Objects.nonNull(task) ? task.getClass().getSimpleName() : null,
          cause.getMessage(),
          errorCode);
    } else {
      LOGGER.warn(
          "Failures happened during running ConfigExecution when executing {}.",
          Objects.nonNull(task) ? task.getClass().getSimpleName() : null,
          cause);
    }
    stateMachine.transitionToFailed(cause);
    final ConfigTaskResult result;
    if (Objects.nonNull(status)) {
      result = new ConfigTaskResult(status);
    } else {
      result =
          cause instanceof StatementExecutionException
              ? new ConfigTaskResult(
                  TSStatusCode.representOf(((StatementExecutionException) cause).getStatusCode()))
              : new ConfigTaskResult(TSStatusCode.representOf(errorCode));
    }

    taskFuture.set(result);
  }

  private boolean isUserException(final TSStatus status) {
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      return status.getSubStatus().stream()
          .allMatch(
              s ->
                  s.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                      || userExceptionCodes.contains(s.getCode()));
    }
    return userExceptionCodes.contains(status.getCode());
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
      final ConfigTaskResult taskResult = taskFuture.get();
      resultSet = taskResult.getResultSet();
      datasetHeader = taskResult.getResultSetHeader();
      if (Objects.nonNull(taskResult.getStatus())) {
        return new ExecutionResult(context.getQueryId(), taskResult.getStatus());
      }
      final TSStatusCode statusCode = taskResult.getStatusCode();
      final String message =
          statusCode == TSStatusCode.SUCCESS_STATUS ? "" : stateMachine.getFailureMessage();
      return new ExecutionResult(context.getQueryId(), RpcUtils.getStatus(statusCode, message));
    } catch (final InterruptedException | ExecutionException e) {
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
    return context.getQueryType() != QueryType.WRITE;
  }

  @Override
  public QueryType getQueryType() {
    return context.getQueryType();
  }

  @Override
  public boolean isUserQuery() {
    return context.isUserQuery();
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

  @Override
  public IClientSession.SqlDialect getSQLDialect() {
    return context.getSession().getSqlDialect();
  }

  @Override
  public String getUser() {
    return context.getSession().getUserName();
  }

  @Override
  public String getClientHostname() {
    return context.getCliHostname();
  }
}
