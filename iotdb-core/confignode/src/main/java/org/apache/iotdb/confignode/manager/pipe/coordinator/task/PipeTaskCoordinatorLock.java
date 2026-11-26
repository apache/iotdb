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

import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.i18n.ManagerMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * {@link PipeTaskCoordinatorLock} is a cross-thread lock for pipe task coordinator. It is used to
 * ensure that only one thread can execute the pipe task coordinator at the same time.
 *
 * <p>Uses {@link Semaphore} instead of {@link java.util.concurrent.locks.ReentrantLock} to support
 * cross-thread acquire/release, which is required by the procedure recovery mechanism: locks may be
 * acquired on the StateMachineUpdater thread during {@code restoreLock()} and released on a
 * ProcedureCoreWorker thread after execution.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private final Semaphore semaphore = new Semaphore(1);

  public void lock() {
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD,
        Thread.currentThread().getName());
    semaphore.acquireUninterruptibly();
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD,
        Thread.currentThread().getName());
  }

  public boolean tryLock() {
    try {
      LOGGER.debug(
          ManagerMessages.PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD,
          Thread.currentThread().getName());
      if (semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
        LOGGER.debug(
            ManagerMessages.PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD,
            Thread.currentThread().getName());
        return true;
      } else {
        PipeLogger.log(
            LOGGER::info,
            ManagerMessages.PIPETASKCOORDINATOR_LOCK_FAILED_TO_ACQUIRE_BY_THREAD_BECAUSE_OF_TIMEOUT,
            Thread.currentThread().getName());
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      PipeLogger.log(
          LOGGER::error,
          ManagerMessages.INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD,
          Thread.currentThread().getName());
      return false;
    }
  }

  public void unlock() {
    semaphore.release();
    LOGGER.debug(
        ManagerMessages.PIPETASKCOORDINATOR_LOCK_RELEASED_BY_THREAD,
        Thread.currentThread().getName());
  }

  public boolean isLocked() {
    return semaphore.availablePermits() == 0;
  }
}
