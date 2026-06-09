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

package org.apache.iotdb.confignode.manager.pipe.coordinator.task;

import org.apache.iotdb.confignode.i18n.ManagerMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * {@link PipeTaskCoordinatorLock} is a cross-thread lock for pipe task coordinator. It is used to
 * ensure that only one thread can execute the pipe task coordinator at the same time.
 *
 * <p>Supports cross-thread acquire/release, which is required by the procedure recovery mechanism:
 * locks may be acquired on the StateMachineUpdater thread during {@code restoreLock()} and released
 * on a ProcedureCoreWorker thread after execution.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private boolean locked = false;

  public void lock() {
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD,
        Thread.currentThread().getName());

    boolean interrupted = false;
    synchronized (this) {
      while (locked) {
        try {
          wait();
        } catch (final InterruptedException e) {
          interrupted = true;
          LOGGER.error(
              ManagerMessages.INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD,
              Thread.currentThread().getName());
        }
      }
      locked = true;
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD,
        Thread.currentThread().getName());
  }

  public boolean tryLock() {
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD,
        Thread.currentThread().getName());
    if (Thread.currentThread().isInterrupted()) {
      LOGGER.error(
          ManagerMessages.INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD,
          Thread.currentThread().getName());
      return false;
    }

    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    synchronized (this) {
      while (locked) {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          LOGGER.info(
              ManagerMessages
                  .PIPETASKCOORDINATOR_LOCK_FAILED_TO_ACQUIRE_BY_THREAD_BECAUSE_OF_TIMEOUT,
              Thread.currentThread().getName());
          return false;
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error(
              ManagerMessages.INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD,
              Thread.currentThread().getName());
          return false;
        }
      }
      locked = true;
    }

    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD,
        Thread.currentThread().getName());
    return true;
  }

  public void unlock() {
    synchronized (this) {
      if (!locked) {
        return;
      }
      locked = false;
      notifyAll();
    }
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_RELEASED_BY_THREAD,
        Thread.currentThread().getName());
  }

  public synchronized boolean isLocked() {
    return locked;
  }
}
