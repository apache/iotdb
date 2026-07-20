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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.state.ProcedureState;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class TimeoutExecutorThread<Env> extends StoppableThread {

  private static final int DELAY_QUEUE_TIMEOUT = 20;
  private final ProcedureExecutor<Env> executor;
  private final DelayQueue<ProcedureDelayContainer<Env>> queue = new DelayQueue<>();
  private final Set<Procedure<Env>> registeredInternalProcedures = ConcurrentHashMap.newKeySet();

  public TimeoutExecutorThread(
      ProcedureExecutor<Env> envProcedureExecutor, ThreadGroup threadGroup, String name) {
    super(threadGroup, name);
    setDaemon(true);
    this.executor = envProcedureExecutor;
  }

  public void add(Procedure<Env> procedure) {
    ProcedureDelayContainer<Env> delayTask = new ProcedureDelayContainer<>(procedure);
    if (procedure instanceof InternalProcedure) {
      if (!registeredInternalProcedures.add(procedure)) {
        return;
      }
      procedure.setState(ProcedureState.WAITING_TIMEOUT);
    }
    queue.remove(delayTask);
    queue.add(delayTask);
  }

  public boolean remove(Procedure<Env> procedure) {
    boolean unregistered =
        procedure instanceof InternalProcedure && registeredInternalProcedures.remove(procedure);
    boolean removed = queue.remove(new ProcedureDelayContainer<>(procedure));
    return unregistered || removed || procedure.isFinished();
  }

  private ProcedureDelayContainer<Env> takeQuietly() {
    try {
      return queue.poll(DELAY_QUEUE_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      currentThread().interrupt();
      return null;
    }
  }

  @Override
  public void run() {
    while (executor.isRunning()) {
      ProcedureDelayContainer<Env> delayTask = takeQuietly();
      if (delayTask == null) {
        continue;
      }
      Procedure<Env> procedure = delayTask.getProcedure();
      if (procedure instanceof InternalProcedure) {
        if (!registeredInternalProcedures.contains(procedure) || procedure.isFinished()) {
          continue;
        }
        InternalProcedure internal = (InternalProcedure) procedure;
        internal.periodicExecute(executor.getEnvironment());
        if (!procedure.isFinished() && registeredInternalProcedures.contains(procedure)) {
          procedure.updateTimestamp();
          queue.add(delayTask);
        }
      } else {
        if (procedure.setTimeoutFailure(executor.getEnvironment())) {
          long rootProcId = executor.getRootProcedureId(procedure);
          RootProcedureStack<Env> rollbackStack = executor.getRollbackStack(rootProcId);
          rollbackStack.abort();
          executor.getStore().update(procedure);
          executor.getScheduler().addFront(procedure);
        }
      }
    }
  }

  public void sendStopSignal() {}

  private static class ProcedureDelayContainer<Env> implements Delayed {

    private final Procedure<Env> procedure;

    public ProcedureDelayContainer(Procedure<Env> procedure) {
      this.procedure = procedure;
    }

    public Procedure<Env> getProcedure() {
      return procedure;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProcedureDelayContainer)) {
        return false;
      }
      ProcedureDelayContainer<?> that = (ProcedureDelayContainer<?>) o;
      return procedure == that.procedure;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(procedure);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long delay = procedure.getTimeoutTimestamp() - System.currentTimeMillis();
      return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.compare(
          this.getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
  }
}
