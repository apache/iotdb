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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;

import io.airlift.concurrent.SetThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/** an abstract executor for {@link DriverTask} */
public abstract class AbstractDriverThread extends Thread implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDriverThread.class);
  private final IndexedBlockingQueue<DriverTask> queue;
  protected final ITaskScheduler scheduler;
  private volatile boolean closed;

  public AbstractDriverThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<DriverTask> queue,
      ITaskScheduler scheduler) {
    super(tg, workerId);
    this.queue = queue;
    this.scheduler = scheduler;
    this.closed = false;
  }

  @Override
  public void run() {
    DriverTask next;
    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        next = queue.poll();
      } catch (InterruptedException e) {
        logger.error("Executor " + this.getName() + "failed to poll driver task from queue");
        Thread.currentThread().interrupt();
        break;
      }
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(next.getFragmentInstance().getInfo().getFullId())) {
        execute(next);
      } catch (Throwable t) {
        logger.error("execute failed", t);
        if (next != null) {
          next.setAbortCause(FragmentInstanceAbortedException.BY_INTERNAL_ERROR_SCHEDULED);
          scheduler.toAborted(next);
        }
      }
    }
  }

  /** Processing a task. */
  protected abstract void execute(DriverTask task) throws InterruptedException, ExecutionException;

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
