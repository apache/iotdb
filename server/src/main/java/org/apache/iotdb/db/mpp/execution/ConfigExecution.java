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

package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jersey.repackaged.com.google.common.util.concurrent.SettableFuture;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.throwIfInstanceOf;

public class ConfigExecution implements IQueryExecution {

  private MPPQueryContext context;
  private Statement statement;
  private ExecutorService executor;

  private QueryStateMachine stateMachine;
  private SettableFuture<Boolean> result;

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
            public void onFailure(Throwable throwable) {
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

  // TODO: consider a more suitable implementation for it
  // Generate the corresponding IConfigTask by statement.
  // Each type of statement will has a ConfigTask
  private IConfigTask getTask(Statement statement) {
    throw new NotImplementedException();
  }
}
