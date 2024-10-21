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

package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.db.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/** An abstract executor for {@link DriverTask}. */
public abstract class AbstractDriverThread extends Thread implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDriverThread.class);
  private final IndexedBlockingQueue<DriverTask> queue;
  private final ThreadProducer producer;
  protected final ITaskScheduler scheduler;
  private volatile boolean closed;

  protected AbstractDriverThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<DriverTask> queue,
      ITaskScheduler scheduler,
      ThreadProducer producer) {
    super(tg, workerId);
    this.queue = queue;
    this.scheduler = scheduler;
    this.closed = false;
    this.producer = producer;
  }

  @Override
  public void run() {
    DriverTask next;
    try {
      while (!closed && !Thread.currentThread().isInterrupted()) {
        try {
          next = queue.poll();
        } catch (InterruptedException e) {
          logger.warn("Executor {} failed to poll driver task from queue", this.getName());
          Thread.currentThread().interrupt();
          break;
        }

        if (next == null) {
          logger.error("DriverTask should never be null");
          continue;
        }

        try (SetThreadName driverTaskName = new SetThreadName(next.getDriverTaskId().getFullId())) {
          execute(next);
        } catch (Exception e) {
          // Try-with-resource syntax will call close once after try block is done, so we need to
          // reset the thread name here
          try (SetThreadName driverTaskName =
              new SetThreadName(next.getDriver().getDriverTaskId().getFullId())) {
            Throwable rootCause = ErrorHandlingUtils.getRootCause(e);
            if (rootCause instanceof IoTDBRuntimeException) {
              next.setAbortCause(e.getMessage());
            } else {
              logger.warn("[ExecuteFailed]", e);
              next.setAbortCause(getAbortCause(e));
            }
            scheduler.toAborted(next);
          }
        } finally {
          // Clear the interrupted flag on the current thread, driver cancellation may have
          // triggered an interrupt
          if (Thread.interrupted() && closed) {
            // Reset interrupted flag if closed before interrupt
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      // Unless we have been closed, we need to replace this thread
      if (!closed) {
        logger.warn(
            "Executor {} exits because it's interrupted. We will produce another thread to replace.",
            this.getName());
        producer.produce(getName(), getThreadGroup(), queue, producer);
      } else {
        logger.info("Executor {} exits because it is closed.", this.getName());
      }
    }
  }

  /**
   * Processing a task.
   *
   * @throws InterruptedException if the task processing is interrupted
   */
  protected abstract void execute(DriverTask task) throws InterruptedException;

  @Override
  public void close() throws IOException {
    closed = true;
  }

  private String getAbortCause(final Exception e) {
    Throwable rootCause = ErrorHandlingUtils.getRootCause(e);
    if (rootCause instanceof MemoryNotEnoughException) {
      return DriverTaskAbortedException.BY_MEMORY_NOT_ENOUGH;
    }
    return DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED;
  }
}
