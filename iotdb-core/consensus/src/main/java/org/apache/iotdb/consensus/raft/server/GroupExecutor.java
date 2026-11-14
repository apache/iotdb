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

package org.apache.iotdb.consensus.raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serial executor for one group on a shared pool: tasks of the same group never run concurrently,
 * so the kernel needs no locks; different groups run in parallel.
 */
public final class GroupExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupExecutor.class);
  private static final int MAX_PER_DRAIN = 256;

  private final String name;
  private final ExecutorService pool;
  private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private volatile boolean closed;

  public GroupExecutor(String name, ExecutorService pool) {
    this.name = name;
    this.pool = pool;
  }

  public void execute(Runnable task) {
    if (closed) {
      return;
    }
    queue.add(task);
    trySchedule();
  }

  private void trySchedule() {
    if (!queue.isEmpty() && scheduled.compareAndSet(false, true)) {
      try {
        pool.execute(this::drain);
      } catch (RuntimeException e) {
        scheduled.set(false);
        if (!closed) {
          LOGGER.error("{}: cannot schedule", name, e);
        }
      }
    }
  }

  private void drain() {
    try {
      Runnable task;
      int n = 0;
      while (!closed && (task = queue.poll()) != null) {
        try {
          task.run();
        } catch (Throwable t) {
          LOGGER.error("{}: task failed", name, t);
        }
        if (++n >= MAX_PER_DRAIN) {
          break;
        }
      }
    } finally {
      scheduled.set(false);
      trySchedule();
    }
  }

  public boolean isIdle() {
    return queue.isEmpty() && !scheduled.get();
  }

  public void close() {
    closed = true;
    queue.clear();
  }
}
