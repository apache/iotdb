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

package org.apache.iotdb.db.queryengine.execution;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.rpc.RpcUtils;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.QueryState.ABORTED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.CANCELED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.DISPATCHING;
import static org.apache.iotdb.db.queryengine.execution.QueryState.FAILED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.FINISHED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.PENDING_RETRY;
import static org.apache.iotdb.db.queryengine.execution.QueryState.PLANNED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.QUEUED;
import static org.apache.iotdb.db.queryengine.execution.QueryState.RUNNING;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.getRootCause;

/**
 * State machine for a {@link QueryExecution}. It stores the states for the {@link QueryExecution}.
 * Others can register listeners when the state changes of the {@link QueryExecution}.
 */
public class QueryStateMachine {
  private final StateMachine<QueryState> queryState;

  // The executor will be used in all the state machines belonged to this query.
  private Executor stateMachineExecutor;
  private Throwable failureException;
  private TSStatus failureStatus;

  public QueryStateMachine(QueryId queryId, ExecutorService executor) {
    this.stateMachineExecutor = executor;
    this.queryState =
        new StateMachine<>(
            queryId.toString(),
            this.stateMachineExecutor,
            QUEUED,
            QueryState.TERMINAL_INSTANCE_STATES);
  }

  public void addStateChangeListener(
      StateMachine.StateChangeListener<QueryState> stateChangeListener) {
    queryState.addStateChangeListener(stateChangeListener);
  }

  public ListenableFuture<QueryState> getStateChange(QueryState currentState) {
    return queryState.getStateChange(currentState);
  }

  public QueryState getState() {
    return queryState.get();
  }

  public void transitionToQueued() {
    queryState.set(QUEUED);
  }

  public void transitionToPlanned() {
    queryState.setIf(PLANNED, currentState -> currentState == QUEUED);
  }

  public void transitionToDispatching() {
    queryState.setIf(DISPATCHING, currentState -> currentState == PLANNED);
  }

  public void transitionToPendingRetry(TSStatus failureStatus) {
    this.failureStatus = failureStatus;
    queryState.setIf(PENDING_RETRY, currentState -> currentState == DISPATCHING);
  }

  public void transitionToRunning() {
    // if we can skipExecute in QueryExecution.start(), we will directly change from QUEUED to
    // RUNNING
    queryState.setIf(
        RUNNING, currentState -> currentState == DISPATCHING || currentState == QUEUED);
  }

  public void transitionToFinished() {
    transitionToDoneState(FINISHED);
  }

  public void transitionToCanceled() {
    transitionToDoneState(CANCELED);
  }

  public void transitionToCanceled(Throwable throwable, TSStatus failureStatus) {
    this.failureException = throwable;
    this.failureStatus = failureStatus;
    transitionToDoneState(CANCELED);
  }

  public void transitionToAborted() {
    transitionToDoneState(ABORTED);
  }

  public void transitionToFailed() {
    transitionToDoneState(FAILED);
  }

  public void transitionToFailed(Throwable throwable) {
    this.failureException = throwable;
    transitionToDoneState(FAILED);
  }

  public void transitionToFailed(TSStatus failureStatus) {
    this.failureStatus = failureStatus;
    transitionToDoneState(FAILED);
  }

  private void transitionToDoneState(QueryState doneState) {
    requireNonNull(doneState, "doneState is null");
    checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

    queryState.setIf(doneState, currentState -> !currentState.isDone());
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
    if (failureStatus != null) {
      return failureStatus;
    } else if (failureException != null) {
      Throwable t = getRootCause(failureException);
      if (t instanceof IoTDBRuntimeException) {
        return RpcUtils.getStatus(((IoTDBRuntimeException) t).getErrorCode(), t.getMessage());
      } else if (t instanceof IoTDBException) {
        return RpcUtils.getStatus(((IoTDBException) t).getErrorCode(), t.getMessage());
      }
    }
    return failureStatus;
  }
}
