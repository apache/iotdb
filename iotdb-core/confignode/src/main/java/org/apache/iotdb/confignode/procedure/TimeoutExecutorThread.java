1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure;
1
1import java.util.concurrent.DelayQueue;
1import java.util.concurrent.Delayed;
1import java.util.concurrent.TimeUnit;
1
1public class TimeoutExecutorThread<Env> extends StoppableThread {
1
1  private static final int DELAY_QUEUE_TIMEOUT = 20;
1  private final ProcedureExecutor<Env> executor;
1  private final DelayQueue<ProcedureDelayContainer<Env>> queue = new DelayQueue<>();
1
1  public TimeoutExecutorThread(
1      ProcedureExecutor<Env> envProcedureExecutor, ThreadGroup threadGroup, String name) {
1    super(threadGroup, name);
1    setDaemon(true);
1    this.executor = envProcedureExecutor;
1  }
1
1  public void add(Procedure<Env> procedure) {
1    queue.add(new ProcedureDelayContainer<>(procedure));
1  }
1
1  public boolean remove(Procedure<Env> procedure) {
1    return queue.remove(new ProcedureDelayContainer<>(procedure));
1  }
1
1  private ProcedureDelayContainer<Env> takeQuietly() {
1    try {
1      return queue.poll(DELAY_QUEUE_TIMEOUT, TimeUnit.SECONDS);
1    } catch (InterruptedException e) {
1      currentThread().interrupt();
1      return null;
1    }
1  }
1
1  @Override
1  public void run() {
1    while (executor.isRunning()) {
1      ProcedureDelayContainer<Env> delayTask = takeQuietly();
1      if (delayTask == null) {
1        continue;
1      }
1      Procedure<Env> procedure = delayTask.getProcedure();
1      if (procedure instanceof InternalProcedure) {
1        InternalProcedure internal = (InternalProcedure) procedure;
1        internal.periodicExecute(executor.getEnvironment());
1        procedure.updateTimestamp();
1        queue.add(delayTask);
1      } else {
1        if (procedure.setTimeoutFailure(executor.getEnvironment())) {
1          long rootProcId = executor.getRootProcedureId(procedure);
1          RootProcedureStack<Env> rollbackStack = executor.getRollbackStack(rootProcId);
1          rollbackStack.abort();
1          executor.getStore().update(procedure);
1          executor.getScheduler().addFront(procedure);
1        }
1      }
1    }
1  }
1
1  public void sendStopSignal() {}
1
1  private static class ProcedureDelayContainer<Env> implements Delayed {
1
1    private final Procedure<Env> procedure;
1
1    public ProcedureDelayContainer(Procedure<Env> procedure) {
1      this.procedure = procedure;
1    }
1
1    public Procedure<Env> getProcedure() {
1      return procedure;
1    }
1
1    @Override
1    public long getDelay(TimeUnit unit) {
1      long delay = procedure.getTimeoutTimestamp() - System.currentTimeMillis();
1      return unit.convert(delay, TimeUnit.MILLISECONDS);
1    }
1
1    @Override
1    public int compareTo(Delayed other) {
1      return Long.compare(
1          this.getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
1    }
1  }
1}
1