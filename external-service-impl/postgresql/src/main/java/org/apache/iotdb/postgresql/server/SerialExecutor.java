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

package org.apache.iotdb.postgresql.server;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * Runs one connection's tasks one after another on a shared pool, so blocking work (logins,
 * queries) leaves Netty's event loop free while the connection's messages keep their order.
 */
final class SerialExecutor implements Executor {

  private final Executor pool;
  private final Queue<Runnable> tasks = new ArrayDeque<>();
  private boolean running;

  SerialExecutor(Executor pool) {
    this.pool = pool;
  }

  @Override
  public void execute(Runnable task) {
    synchronized (tasks) {
      tasks.add(task);
      if (running) {
        return;
      }
      running = true;
    }
    pool.execute(this::drain);
  }

  private void drain() {
    while (true) {
      Runnable next;
      synchronized (tasks) {
        next = tasks.poll();
        if (next == null) {
          running = false;
          return;
        }
      }
      try {
        next.run();
      } catch (RuntimeException e) {
        // a task reports its own errors to the client; never stop the queue
      }
    }
  }
}
