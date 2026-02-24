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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link PipeTaskCoordinatorLock} is a cross thread lock for pipe task coordinator. It is used to
 * ensure that only one thread can execute the pipe task coordinator at the same time.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private final ReentrantLock lock = new ReentrantLock();

  public void lock() {
    LOGGER.debug(
        "PipeTaskCoordinator lock waiting for thread {}", Thread.currentThread().getName());
    try {
      lock.lockInterruptibly();
      LOGGER.debug(
          "PipeTaskCoordinator lock acquired by thread {}", Thread.currentThread().getName());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error(
          "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}",
          Thread.currentThread().getName());
    }
  }

  public boolean tryLock() {
    try {
      LOGGER.debug(
          "PipeTaskCoordinator lock waiting for thread {}", Thread.currentThread().getName());
      if (lock.tryLock(10, TimeUnit.SECONDS)) {
        LOGGER.debug(
            "PipeTaskCoordinator lock acquired by thread {}", Thread.currentThread().getName());
        return true;
      } else {
        LOGGER.info(
            "PipeTaskCoordinator lock failed to acquire by thread {} because of timeout",
            Thread.currentThread().getName());
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error(
          "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}",
          Thread.currentThread().getName());
      return false;
    }
  }

  public void unlock() {
    lock.unlock();
    LOGGER.debug(
        "PipeTaskCoordinator lock released by thread {}", Thread.currentThread().getName());
  }

  public boolean isLocked() {
    return lock.isLocked();
  }
}
