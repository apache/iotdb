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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** the thread for watching the timeout of {@link DriverTask} */
public class DriverTaskTimeoutSentinelThread extends AbstractDriverThread {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DriverTaskTimeoutSentinelThread.class);

  public DriverTaskTimeoutSentinelThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<DriverTask> queue,
      ITaskScheduler scheduler,
      ThreadProducer producer) {
    super(workerId, tg, queue, scheduler, producer);
  }

  @Override
  public void execute(DriverTask task) throws InterruptedException {
    task.lock();
    try {
      // if this task is already in an end state, it means that the resource releasing will be
      // handled by other threads, we don't care anymore.
      if (task.isEndState()) {
        return;
      }
    } finally {
      task.unlock();
    }
    // if this task is not timeout, we can wait it to timeout.
    long waitTime = task.getDDL() - System.currentTimeMillis();
    while (waitTime > 0L) {
      // After this time, the task must be timeout.
      Thread.sleep(waitTime);
      waitTime = task.getDDL() - System.currentTimeMillis();
    }

    task.lock();
    try {
      // if this task is already in an end state, it means that the resource releasing will be
      // handled by other threads, we don't care anymore.
      if (task.isEndState()) {
        return;
      }
    } finally {
      task.unlock();
    }
    LOGGER.warn(
        "[DriverTaskTimeout] Current time is {}, ddl of task is {}",
        System.currentTimeMillis(),
        task.getDDL());
    task.setAbortCause(DriverTaskAbortedException.BY_TIMEOUT);
    scheduler.toAborted(task);
  }
}
