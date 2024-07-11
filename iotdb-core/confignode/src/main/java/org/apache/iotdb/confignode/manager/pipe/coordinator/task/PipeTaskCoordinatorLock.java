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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link PipeTaskCoordinatorLock} is a cross thread lock for pipe task coordinator. It is used to
 * ensure that only one thread can execute the pipe task coordinator at the same time.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private final BlockingDeque<Long> deque = new LinkedBlockingDeque<>(1);
  private final AtomicLong idGenerator = new AtomicLong(0);

  public void lock() {
    try {
      final long id = idGenerator.incrementAndGet();
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}) waiting for thread {}",
          id,
          Thread.currentThread().getName());
      deque.put(id);
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}) acquired by thread {}",
          id,
          Thread.currentThread().getName());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error(
          "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}",
          Thread.currentThread().getName());
    }
  }

  public boolean tryLock() {
    try {
      final long id = idGenerator.incrementAndGet();
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}) waiting for thread {}",
          id,
          Thread.currentThread().getName());
      if (deque.offer(id, 10, TimeUnit.SECONDS)) {
        LOGGER.info(
            "PipeTaskCoordinator lock (id: {}) acquired by thread {}",
            id,
            Thread.currentThread().getName());
        return true;
      } else {
        LOGGER.info(
            "PipeTaskCoordinator lock (id: {}) failed to acquire by thread {} because of timeout",
            id,
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
    final Long id = deque.poll();
    if (id == null) {
      LOGGER.error(
          "PipeTaskCoordinator lock released by thread {} but the lock is not acquired by any thread",
          Thread.currentThread().getName());
    } else {
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}) released by thread {}",
          id,
          Thread.currentThread().getName());
    }
  }

  public boolean isLocked() {
    return !deque.isEmpty();
  }
}
