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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractProcedureScheduler implements ProcedureScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractProcedureScheduler.class);
  private final ReentrantLock schedulerLock = new ReentrantLock();
  private final Condition schedWaitCond = schedulerLock.newCondition();
  private boolean running = false;

  @Override
  public void start() {
    schedLock();
    try {
      running = true;
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void stop() {
    schedLock();
    try {
      running = false;
      schedWaitCond.signalAll();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void signalAll() {
    schedLock();
    try {
      schedWaitCond.signalAll();
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Add related
  // ==========================================================================
  /**
   * Add the procedure to the queue. NOTE: this method is called with the sched lock held.
   *
   * @param procedure the Procedure to add
   * @param addFront true if the item should be added to the front of the queue
   */
  protected abstract void enqueue(Procedure procedure, boolean addFront);

  @Override
  public void addFront(final Procedure procedure) {
    if (procedure.isSuccess()) {
      LOG.warn("Don't add a successful procedure back to the scheduler, it will be ignored");
      return;
    }
    push(procedure, true, true);
  }

  @Override
  public void addFront(final Procedure procedure, boolean notify) {
    push(procedure, true, notify);
  }

  @Override
  public void addBack(final Procedure procedure) {
    if (procedure.isSuccess()) {
      LOG.warn("Don't add a successful procedure back to the scheduler, it will be ignored");
      return;
    }
    push(procedure, false, true);
  }

  @Override
  public void addBack(final Procedure procedure, boolean notify) {
    push(procedure, false, notify);
  }

  protected void push(final Procedure procedure, final boolean addFront, final boolean notify) {
    schedLock();
    try {
      enqueue(procedure, addFront);
      if (notify) {
        schedWaitCond.signal();
      }
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Poll related
  // ==========================================================================
  /**
   * Fetch one Procedure from the queue NOTE: this method is called with the sched lock held.
   *
   * @return the Procedure to execute, or null if nothing is available.
   */
  protected abstract Procedure dequeue();

  @Override
  public Procedure poll() {
    return poll(-1);
  }

  @Override
  public Procedure poll(long timeout, TimeUnit unit) {
    return poll(unit.toNanos(timeout));
  }

  public Procedure poll(final long nanos) {
    schedLock();
    try {
      if (!running) {
        LOG.debug("the scheduler is not running");
        return null;
      }

      if (!queueHasRunnables()) {
        // WA_AWAIT_NOT_IN_LOOP: we are not in a loop because we want the caller
        // to take decisions after a wake/interruption.
        if (nanos < 0) {
          schedWaitCond.await();
        } else {
          long leftTime = schedWaitCond.awaitNanos(nanos);
          LOG.debug("the scheduler waiting time left {} nanos", leftTime);
        }
        if (!queueHasRunnables()) {
          return null;
        }
      }
      final Procedure pollResult = dequeue();

      return pollResult;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Utils
  // ==========================================================================
  /**
   * Returns the number of elements in this queue. NOTE: this method is called with the sched lock
   * held.
   *
   * @return the number of elements in this queue.
   */
  protected abstract int queueSize();

  /**
   * Returns true if there are procedures available to process. NOTE: this method is called with the
   * sched lock held.
   *
   * @return true if there are procedures available to process, otherwise false.
   */
  protected abstract boolean queueHasRunnables();

  @Override
  public int size() {
    schedLock();
    try {
      return queueSize();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public boolean hasRunnables() {
    schedLock();
    try {
      return queueHasRunnables();
    } finally {
      schedUnlock();
    }
  }

  // ==========================================================================
  //  Internal helpers
  // ==========================================================================
  protected void schedLock() {
    schedulerLock.lock();
  }

  protected void schedUnlock() {
    schedulerLock.unlock();
  }
}
