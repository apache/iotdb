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
1package org.apache.iotdb.confignode.procedure.scheduler;
1
1import org.apache.iotdb.confignode.procedure.Procedure;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.concurrent.TimeUnit;
1import java.util.concurrent.locks.Condition;
1import java.util.concurrent.locks.ReentrantLock;
1
1public abstract class AbstractProcedureScheduler implements ProcedureScheduler {
1  private static final Logger LOG = LoggerFactory.getLogger(AbstractProcedureScheduler.class);
1  private final ReentrantLock schedulerLock = new ReentrantLock();
1  private final Condition schedWaitCond = schedulerLock.newCondition();
1  private boolean running = false;
1
1  @Override
1  public void start() {
1    schedLock();
1    try {
1      running = true;
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  @Override
1  public void stop() {
1    schedLock();
1    try {
1      running = false;
1      schedWaitCond.signalAll();
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  @Override
1  public void signalAll() {
1    schedLock();
1    try {
1      schedWaitCond.signalAll();
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  // ==========================================================================
1  //  Add related
1  // ==========================================================================
1  /**
1   * Add the procedure to the queue. NOTE: this method is called with the sched lock held.
1   *
1   * @param procedure the Procedure to add
1   * @param addFront true if the item should be added to the front of the queue
1   */
1  protected abstract void enqueue(Procedure procedure, boolean addFront);
1
1  @Override
1  public void addFront(final Procedure procedure) {
1    if (procedure.isSuccess()) {
1      LOG.warn("Don't add a successful procedure back to the scheduler, it will be ignored");
1      return;
1    }
1    push(procedure, true, true);
1  }
1
1  @Override
1  public void addFront(final Procedure procedure, boolean notify) {
1    push(procedure, true, notify);
1  }
1
1  @Override
1  public void addBack(final Procedure procedure) {
1    if (procedure.isSuccess()) {
1      LOG.warn("Don't add a successful procedure back to the scheduler, it will be ignored");
1      return;
1    }
1    push(procedure, false, true);
1  }
1
1  @Override
1  public void addBack(final Procedure procedure, boolean notify) {
1    push(procedure, false, notify);
1  }
1
1  protected void push(final Procedure procedure, final boolean addFront, final boolean notify) {
1    schedLock();
1    try {
1      enqueue(procedure, addFront);
1      if (notify) {
1        schedWaitCond.signal();
1      }
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  // ==========================================================================
1  //  Poll related
1  // ==========================================================================
1  /**
1   * Fetch one Procedure from the queue NOTE: this method is called with the sched lock held.
1   *
1   * @return the Procedure to execute, or null if nothing is available.
1   */
1  protected abstract Procedure dequeue();
1
1  @Override
1  public Procedure poll() {
1    return poll(-1);
1  }
1
1  @Override
1  public Procedure poll(long timeout, TimeUnit unit) {
1    return poll(unit.toNanos(timeout));
1  }
1
1  public Procedure poll(final long nanos) {
1    schedLock();
1    try {
1      if (!running) {
1        LOG.debug("the scheduler is not running");
1        return null;
1      }
1
1      if (!queueHasRunnables()) {
1        // WA_AWAIT_NOT_IN_LOOP: we are not in a loop because we want the caller
1        // to take decisions after a wake/interruption.
1        if (nanos < 0) {
1          schedWaitCond.await();
1        } else {
1          long leftTime = schedWaitCond.awaitNanos(nanos);
1          LOG.debug("the scheduler waiting time left {} nanos", leftTime);
1        }
1        if (!queueHasRunnables()) {
1          return null;
1        }
1      }
1      final Procedure pollResult = dequeue();
1
1      return pollResult;
1    } catch (InterruptedException e) {
1      Thread.currentThread().interrupt();
1      return null;
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  // ==========================================================================
1  //  Utils
1  // ==========================================================================
1  /**
1   * Returns the number of elements in this queue. NOTE: this method is called with the sched lock
1   * held.
1   *
1   * @return the number of elements in this queue.
1   */
1  protected abstract int queueSize();
1
1  /**
1   * Returns true if there are procedures available to process. NOTE: this method is called with the
1   * sched lock held.
1   *
1   * @return true if there are procedures available to process, otherwise false.
1   */
1  protected abstract boolean queueHasRunnables();
1
1  @Override
1  public int size() {
1    schedLock();
1    try {
1      return queueSize();
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  @Override
1  public boolean hasRunnables() {
1    schedLock();
1    try {
1      return queueHasRunnables();
1    } finally {
1      schedUnlock();
1    }
1  }
1
1  // ==========================================================================
1  //  Internal helpers
1  // ==========================================================================
1  protected void schedLock() {
1    schedulerLock.lock();
1  }
1
1  protected void schedUnlock() {
1    schedulerLock.unlock();
1  }
1}
1