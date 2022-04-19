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

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jersey.repackaged.com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.throwIfInstanceOf;

public class ConfigExecution implements IQueryExecution {

  private final MPPQueryContext context;
  private final Statement statement;
  private final ExecutorService executor;

  private final QueryStateMachine stateMachine;
  private final SettableFuture<Boolean> result;

  public ConfigExecution(MPPQueryContext context, Statement statement, ExecutorService executor) {
    this.context = context;
    this.statement = statement;
    this.executor = executor;
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);
    this.result = SettableFuture.create();
  }

  @Override
  public void start() {
    IConfigTask task = getTask(statement);
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
      fail(e);
      throwIfInstanceOf(e, Error.class);
    }
  }

  public void fail(Throwable cause) {
    stateMachine.transitionToFailed();
    result.cancel(false);
  }

  @Override
  public void stop() {}

  @Override
  public ExecutionResult getStatus() {
    try {
      if (result.isCancelled()) {
        return new ExecutionResult(
            context.getQueryId(), RpcUtils.getStatus(TSStatusCode.QUERY_PROCESS_ERROR));
      }
      Boolean success = result.get();
      TSStatusCode statusCode =
          success ? TSStatusCode.SUCCESS_STATUS : TSStatusCode.QUERY_PROCESS_ERROR;
      return new ExecutionResult(context.getQueryId(), RpcUtils.getStatus(statusCode));

    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      return new ExecutionResult(
          context.getQueryId(), RpcUtils.getStatus(TSStatusCode.QUERY_PROCESS_ERROR));
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

  // TODO: consider a more suitable implementation for it
  // Generate the corresponding IConfigTask by statement.
  // Each type of statement will has a ConfigTask
  private IConfigTask getTask(Statement statement) {
    try {
      switch (statement.getType()) {
        case SET_STORAGE_GROUP:
          return new SetStorageGroupTask((SetStorageGroupStatement) statement);
        case AUTHOR:
          return new AuthorizerConfigTask((AuthorStatement) statement);
        default:
          throw new NotImplementedException();
      }
    } catch (ClassCastException classCastException) {
      throw new NotImplementedException();
    }
  }
}
