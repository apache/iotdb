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

package org.apache.iotdb.db.mpp.execution.config;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jersey.repackaged.com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class ConfigExecution implements IQueryExecution {

  private final MPPQueryContext context;
  private final Statement statement;
  private final ExecutorService executor;

  private final QueryStateMachine stateMachine;
  private final SettableFuture<Boolean> result;

  private final IConfigTask task;

  public ConfigExecution(MPPQueryContext context, Statement statement, ExecutorService executor) {
    this.context = context;
    this.statement = statement;
    this.executor = executor;
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);
    this.result = SettableFuture.create();
    this.task = statement.accept(new ConfigTaskVisitor(), new ConfigTaskVisitor.TaskContext());
  }

  @TestOnly
  public ConfigExecution(
      MPPQueryContext context, Statement statement, ExecutorService executor, IConfigTask task) {
    this.context = context;
    this.statement = statement;
    this.executor = executor;
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);
    this.result = SettableFuture.create();
    this.task = task;
  }

  @Override
  public void start() {
    try {
      ListenableFuture<Void> future = task.execute();
      Futures.addCallback(
          future,
          new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void success) {
              stateMachine.transitionToFinished();
              result.set(true);
            }

            @Override
            public void onFailure(@NotNull Throwable throwable) {
              fail(throwable);
            }
          },
          executor);
    } catch (Throwable e) {
      Thread.currentThread().interrupt();
      fail(e);
    }
  }

  public void fail(Throwable cause) {
    stateMachine.transitionToFailed(cause);
    result.set(false);
  }

  @Override
  public void stop() {}

  @Override
  public void stopAndCleanup() {}

  @Override
  public ExecutionResult getStatus() {
    try {
      Boolean success = result.get();
      TSStatusCode statusCode =
          success ? TSStatusCode.SUCCESS_STATUS : TSStatusCode.QUERY_PROCESS_ERROR;
      String message = success ? "" : stateMachine.getFailureMessage();
      return new ExecutionResult(context.getQueryId(), RpcUtils.getStatus(statusCode, message));
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      return new ExecutionResult(
          context.getQueryId(),
          RpcUtils.getStatus(TSStatusCode.QUERY_PROCESS_ERROR, e.getMessage()));
    }
  }

  @Override
  public TsBlock getBatchResult() {
    // TODO
    return null;
  }

  @Override
  public boolean hasNextResult() {
    return false;
  }

  @Override
  public int getOutputValueColumnCount() {
    // TODO
    return 0;
  }

  @Override
  public DatasetHeader getDatasetHeader() {
    // TODO
    return null;
  }

  @Override
  public boolean isQuery() {
    return context.getQueryType() == QueryType.READ;
  }
}
