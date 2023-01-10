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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.mpp.common.QueryId;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * State machine for a QueryExecution. It stores the states for the QueryExecution. Others can
 * register listeners when the state changes of the QueryExecution.
 */
public class QueryStateMachine {
  private final String name;
  private final StateMachine<QueryState> queryState;

  // The executor will be used in all the state machines belonged to this query.
  private Executor stateMachineExecutor;
  private Throwable failureException;
  private TSStatus failureStatus;

  public QueryStateMachine(QueryId queryId, ExecutorService executor) {
    this.name = String.format("QueryStateMachine[%s]", queryId);
    this.stateMachineExecutor = executor;
    this.queryState =
        new StateMachine<>(
            queryId.toString(),
            this.stateMachineExecutor,
            QueryState.QUEUED,
            QueryState.TERMINAL_INSTANCE_STATES);
  }

  public void addStateChangeListener(
      StateMachine.StateChangeListener<QueryState> stateChangeListener) {
    queryState.addStateChangeListener(stateChangeListener);
  }

  public ListenableFuture<QueryState> getStateChange(QueryState currentState) {
    return queryState.getStateChange(currentState);
  }

  private String getName() {
    return name;
  }

  public QueryState getState() {
    return queryState.get();
  }

  public void transitionToQueued() {
    queryState.set(QueryState.QUEUED);
  }

  public void transitionToPlanned() {
    queryState.set(QueryState.PLANNED);
  }

  public void transitionToDispatching() {
    queryState.set(QueryState.DISPATCHING);
  }

  public void transitionToPendingRetry(TSStatus failureStatus) {
    if (queryState.get().isDone()) {
      return;
    }
    this.failureStatus = failureStatus;
    queryState.set(QueryState.PENDING_RETRY);
  }

  public void transitionToRunning() {
    queryState.set(QueryState.RUNNING);
  }

  public void transitionToFinished() {
    if (queryState.get().isDone()) {
      return;
    }
    queryState.set(QueryState.FINISHED);
  }

  public void transitionToCanceled() {
    if (queryState.get().isDone()) {
      return;
    }
    queryState.set(QueryState.CANCELED);
  }

  public void transitionToCanceled(Throwable throwable, TSStatus failureStatus) {
    if (queryState.get().isDone()) {
      return;
    }
    this.failureException = throwable;
    this.failureStatus = failureStatus;
    queryState.set(QueryState.CANCELED);
  }

  public void transitionToAborted() {
    if (queryState.get().isDone()) {
      return;
    }
    queryState.set(QueryState.ABORTED);
  }

  public void transitionToFailed() {
    if (queryState.get().isDone()) {
      return;
    }
    queryState.set(QueryState.FAILED);
  }

  public void transitionToFailed(Throwable throwable) {
    if (queryState.get().isDone()) {
      return;
    }
    this.failureException = throwable;
    queryState.set(QueryState.FAILED);
  }

  public void transitionToFailed(TSStatus failureStatus) {
    if (queryState.get().isDone()) {
      return;
    }
    this.failureStatus = failureStatus;
    queryState.set(QueryState.FAILED);
  }

  public String getFailureMessage() {
    if (failureException != null) {
      return failureException.getMessage();
    }
    return "no detailed failure reason in QueryStateMachine";
  }

  public Throwable getFailureException() {
    if (failureException == null) {
      return new IoTDBException(getFailureStatus().getMessage(), getFailureStatus().code);
    } else {
      return failureException;
    }
  }

  public TSStatus getFailureStatus() {
    return failureStatus;
  }
}
