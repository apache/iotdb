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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Limits whole deletion tasks, retaining their slots across state transitions and rollback. */
public class DeleteTimeSeriesProcedureScheduler implements ProcedureScheduler {
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private final Deque<Procedure> waiting = new ArrayDeque<>();
  private final Deque<Procedure> runnable = new ArrayDeque<>();
  private final Set<Long> active = new HashSet<>();
  private final int maxConcurrentTasks;
  private long executionIntervalNanos;
  private boolean running;
  private boolean taskStarted;
  private long lastStartNanos;

  public DeleteTimeSeriesProcedureScheduler(int maxConcurrentTasks, long executionIntervalMs) {
    this.maxConcurrentTasks = maxConcurrentTasks;
    this.executionIntervalNanos = TimeUnit.MILLISECONDS.toNanos(executionIntervalMs);
  }

  public void setExecutionIntervalMs(long executionIntervalMs) {
    lock.lock();
    try {
      executionIntervalNanos = TimeUnit.MILLISECONDS.toNanos(executionIntervalMs);
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void start() {
    lock.lock();
    try {
      running = true;
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void stop() {
    lock.lock();
    try {
      running = false;
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void signalAll() {
    lock.lock();
    try {
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addFront(Procedure procedure) {
    addFront(procedure, true);
  }

  @Override
  public void addFront(Procedure procedure, boolean notify) {
    enqueue(procedure, true, notify);
  }

  @Override
  public void addBack(Procedure procedure) {
    addBack(procedure, true);
  }

  @Override
  public void addBack(Procedure procedure, boolean notify) {
    enqueue(procedure, false, notify);
  }

  private void enqueue(Procedure procedure, boolean front, boolean notify) {
    if (procedure.isSuccess()) {
      return;
    }
    lock.lock();
    try {
      // Enqueue without rate limiting; only starting a new task is throttled in poll().
      // Subsequent states retain their concurrency slot.
      Deque<Procedure> queue = active.contains(procedure.getProcId()) ? runnable : waiting;
      if (front) {
        queue.addFirst(procedure);
      } else {
        queue.addLast(procedure);
      }
      if (notify) {
        changed.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void yield(Procedure procedure) {
    addBack(procedure);
  }

  @Override
  public Procedure poll() {
    return poll(-1, TimeUnit.NANOSECONDS);
  }

  @Override
  public Procedure poll(long timeout, TimeUnit unit) {
    final long deadline = timeout < 0 ? 0 : System.nanoTime() + unit.toNanos(timeout);
    lock.lock();
    try {
      while (running) {
        if (!runnable.isEmpty()) {
          return runnable.removeFirst();
        }
        long delay = Long.MAX_VALUE;
        if (!waiting.isEmpty() && active.size() < maxConcurrentTasks) {
          delay = taskStarted ? executionIntervalNanos - (System.nanoTime() - lastStartNanos) : 0;
          if (delay <= 0) {
            final Procedure procedure = waiting.removeFirst();
            active.add(procedure.getProcId());
            lastStartNanos = System.nanoTime();
            taskStarted = true;
            return procedure;
          }
        }
        if (timeout >= 0) {
          delay = Math.min(delay, deadline - System.nanoTime());
          if (delay <= 0) {
            return null;
          }
        }
        changed.awaitNanos(delay);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }
    return null;
  }

  /** Called only after the entire root procedure, including rollback, has completed. */
  public void completionCleanup(Procedure procedure) {
    lock.lock();
    try {
      active.remove(procedure.getProcId());
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public boolean isQueued(long procedureId) {
    lock.lock();
    try {
      return waiting.stream().anyMatch(procedure -> procedure.getProcId() == procedureId);
    } finally {
      lock.unlock();
    }
  }

  public int getWaitingTaskCount() {
    lock.lock();
    try {
      return waiting.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean hasRunnables() {
    return size() > 0;
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return waiting.size() + runnable.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    lock.lock();
    try {
      waiting.clear();
      runnable.clear();
      active.clear();
      taskStarted = false;
    } finally {
      lock.unlock();
    }
  }
}
