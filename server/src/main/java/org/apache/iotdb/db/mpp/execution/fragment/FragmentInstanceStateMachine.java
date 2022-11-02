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
package org.apache.iotdb.db.mpp.execution.fragment;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.StateMachine;
import org.apache.iotdb.db.mpp.execution.StateMachine.StateChangeListener;
import org.apache.iotdb.db.utils.SetThreadName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.ABORTED;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.CANCELLED;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.FAILED;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.FINISHED;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.FLUSHING;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.RUNNING;
import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState.TERMINAL_INSTANCE_STATES;

@ThreadSafe
public class FragmentInstanceStateMachine {
  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceStateMachine.class);

  private final long createdTime = System.currentTimeMillis();

  private final FragmentInstanceId instanceId;
  private final Executor executor;
  private final StateMachine<FragmentInstanceState> instanceState;
  private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

  @GuardedBy("this")
  private final Map<FragmentInstanceId, Throwable> sourceInstanceFailures = new HashMap<>();

  @GuardedBy("this")
  private final List<FragmentInstanceFailureListener> sourceInstanceFailureListeners =
      new ArrayList<>();

  public FragmentInstanceStateMachine(FragmentInstanceId fragmentInstanceId, Executor executor) {
    this.instanceId = requireNonNull(fragmentInstanceId, "fragmentInstanceId is null");
    this.executor = requireNonNull(executor, "executor is null");
    instanceState =
        new StateMachine<>(
            "FragmentInstance " + fragmentInstanceId, executor, RUNNING, TERMINAL_INSTANCE_STATES);
    if (LOGGER.isDebugEnabled()) {
      instanceState.addStateChangeListener(
          newState -> {
            try (SetThreadName threadName = new SetThreadName(fragmentInstanceId.getFullId())) {
              LOGGER.debug("[StateChanged] To {}", newState);
            }
          });
    }
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public FragmentInstanceId getFragmentInstanceId() {
    return instanceId;
  }

  public FragmentInstanceState getState() {
    return instanceState.get();
  }

  public ListenableFuture<FragmentInstanceState> getStateChange(
      FragmentInstanceState currentState) {
    requireNonNull(currentState, "currentState is null");
    checkArgument(!currentState.isDone(), "Current state is already done");

    ListenableFuture<FragmentInstanceState> future = instanceState.getStateChange(currentState);
    FragmentInstanceState state = instanceState.get();
    if (state.isDone()) {
      return immediateFuture(state);
    }
    return future;
  }

  public LinkedBlockingQueue<Throwable> getFailureCauses() {
    return failureCauses;
  }

  public void transitionToFlushing() {
    instanceState.setIf(FLUSHING, currentState -> currentState == RUNNING);
  }

  public void finished() {
    transitionToDoneState(FINISHED);
  }

  public void cancel() {
    transitionToDoneState(CANCELLED);
  }

  public void abort() {
    transitionToDoneState(ABORTED);
  }

  public void failed(Throwable cause) {
    failureCauses.add(cause);
    transitionToDoneState(FAILED);
  }

  private void transitionToDoneState(FragmentInstanceState doneState) {
    requireNonNull(doneState, "doneState is null");
    checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

    instanceState.setIf(doneState, currentState -> !currentState.isDone());
  }

  /**
   * Listener is always notified asynchronously using a dedicated notification thread pool so, care
   * should be taken to avoid leaking {@code this} when adding a listener in a constructor.
   * Additionally, it is possible notifications are observed out of order due to the asynchronous
   * execution.
   */
  public void addStateChangeListener(
      StateChangeListener<FragmentInstanceState> stateChangeListener) {
    instanceState.addStateChangeListener(stateChangeListener);
  }

  public void addSourceTaskFailureListener(FragmentInstanceFailureListener listener) {
    Map<FragmentInstanceId, Throwable> failures;
    synchronized (this) {
      sourceInstanceFailureListeners.add(listener);
      failures = ImmutableMap.copyOf(sourceInstanceFailures);
    }
    executor.execute(
        () -> {
          failures.forEach(listener::onTaskFailed);
        });
  }

  public void sourceTaskFailed(FragmentInstanceId instanceId, Throwable failure) {
    List<FragmentInstanceFailureListener> listeners;
    synchronized (this) {
      sourceInstanceFailures.putIfAbsent(instanceId, failure);
      listeners = ImmutableList.copyOf(sourceInstanceFailureListeners);
    }
    executor.execute(
        () -> {
          for (FragmentInstanceFailureListener listener : listeners) {
            listener.onTaskFailed(instanceId, failure);
          }
        });
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("FragmentInstanceId", instanceId)
        .add("FragmentInstanceState", instanceState)
        .add("failureCauses", failureCauses)
        .toString();
  }
}
