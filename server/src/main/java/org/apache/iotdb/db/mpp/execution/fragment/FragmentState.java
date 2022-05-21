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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum FragmentState {

  /**
   * Fragment is planned but has not been scheduled yet. A Fragment will be in the planned state
   * until, the dependencies of the Fragment have begun producing output.
   */
  PLANNED(false, false),
  /** Fragment instances are being scheduled on nodes. */
  SCHEDULING(false, false),
  /** Fragment is running. */
  RUNNING(false, false),
  /**
   * Fragment has finished executing existing tasks but more instances could be scheduled in the
   * future.
   */
  PENDING(false, false),
  /** Fragment has finished executing and all output has been consumed. */
  FINISHED(true, false),
  /** Fragment was aborted due to a failure in the query. The failure was not in this Fragment. */
  ABORTED(true, true),
  /** Fragment execution failed. */
  FAILED(true, true);

  public static final Set<FragmentState> TERMINAL_FRAGMENT_STATES =
      Stream.of(FragmentState.values()).filter(FragmentState::isDone).collect(toImmutableSet());

  private final boolean doneState;
  private final boolean failureState;

  FragmentState(boolean doneState, boolean failureState) {
    checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
    this.doneState = doneState;
    this.failureState = failureState;
  }

  /** Is this a terminal state. */
  public boolean isDone() {
    return doneState;
  }

  /** Is this a non-success terminal state. */
  public boolean isFailure() {
    return failureState;
  }
}
