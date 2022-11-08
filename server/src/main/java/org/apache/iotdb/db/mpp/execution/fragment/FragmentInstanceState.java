/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.fragment;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum FragmentInstanceState {
  /**
   * Instance is planned but has not been scheduled yet. An instance will be in the planned state
   * until, the dependencies of the instance have begun producing output.
   */
  PLANNED(false, false),
  /** Instance is running. */
  RUNNING(false, false),
  /**
   * Instance has finished executing and output is left to be consumed. In this state, there will be
   * no new drivers, the existing drivers have finished and the output buffer of the instance is
   * at-least in a 'no-more-tsBlocks' state.
   */
  FLUSHING(false, false),
  /** Instance has finished executing and all output has been consumed. */
  FINISHED(true, false),
  /** Instance was canceled by a user. */
  CANCELLED(true, true),
  /** Instance was aborted due to a failure in the query. The failure was not in this instance. */
  ABORTED(true, true),
  /** Instance execution failed. */
  FAILED(true, true),
  /** Instance is not found. */
  NO_SUCH_INSTANCE(false, false);

  public static final Set<FragmentInstanceState> TERMINAL_INSTANCE_STATES =
      Stream.of(FragmentInstanceState.values())
          .filter(FragmentInstanceState::isDone)
          .collect(toImmutableSet());

  public static final Set<FragmentInstanceState> FAILURE_INSTANCE_STATES =
      Stream.of(FragmentInstanceState.values())
          .filter(FragmentInstanceState::isFailed)
          .collect(toImmutableSet());

  /**
   * If doneState is true, indicating that it won't transfer to another state anymore, i.e. a
   * terminal state.
   */
  private final boolean doneState;

  private final boolean failureState;

  FragmentInstanceState(boolean doneState, boolean failureState) {
    this.doneState = doneState;
    this.failureState = failureState;
  }

  /** Is this a terminal state. */
  public boolean isDone() {
    return doneState;
  }

  public boolean isFailed() {
    return failureState;
  }
}
