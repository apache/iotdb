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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Guards one processor subtask invocation against concurrent STOP/START operations.
 *
 * <p>An invocation captures the current execution epoch. STOP invalidates that epoch before START
 * can enable a new one, so an invocation started before STOP must yield even if the pipe is started
 * again immediately.
 */
public class PipeProcessorSubtaskExecutionGuard {

  private static final PipeProcessorSubtaskExecutionGuard DISABLED_GUARD =
      new PipeProcessorSubtaskExecutionGuard(false);

  private final boolean enabled;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicLong executionEpoch = new AtomicLong(0);
  private final ThreadLocal<Long> invocationEpoch = new ThreadLocal<>();

  public PipeProcessorSubtaskExecutionGuard() {
    this(true);
  }

  private PipeProcessorSubtaskExecutionGuard(final boolean enabled) {
    this.enabled = enabled;
  }

  public static PipeProcessorSubtaskExecutionGuard disabled() {
    return DISABLED_GUARD;
  }

  public boolean isEnabled() {
    return enabled;
  }

  void start() {
    if (enabled) {
      isRunning.set(true);
    }
  }

  void stop() {
    if (enabled) {
      isRunning.set(false);
      executionEpoch.incrementAndGet();
    }
  }

  void enter() {
    if (!enabled) {
      return;
    }

    final long currentEpoch = executionEpoch.get();
    invocationEpoch.set(currentEpoch);
    if (!isRunning.get() || currentEpoch != executionEpoch.get()) {
      invocationEpoch.remove();
      throw PipeProcessorSubtaskYieldException.pauseRequested();
    }
  }

  void exit() {
    if (enabled) {
      invocationEpoch.remove();
    }
  }

  public void check() {
    if (!isCurrentInvocationValid()) {
      throw PipeProcessorSubtaskYieldException.pauseRequested();
    }
  }

  public boolean isCurrentInvocationValid() {
    if (!enabled) {
      return true;
    }

    final Long currentInvocationEpoch = invocationEpoch.get();
    return currentInvocationEpoch != null
        && isRunning.get()
        && currentInvocationEpoch == executionEpoch.get();
  }

  public void yieldIfParserNotAdmitted() {
    throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
  }
}
