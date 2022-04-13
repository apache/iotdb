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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;

/** the thread for watching the timeout of {@link FragmentInstanceTask} */
public class FragmentInstanceTimeoutSentinel extends AbstractExecutor {

  public FragmentInstanceTimeoutSentinel(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<FragmentInstanceTask> queue,
      ITaskScheduler scheduler) {
    super(workerId, tg, queue, scheduler);
  }

  @Override
  public void execute(FragmentInstanceTask task) throws InterruptedException {
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
    if (waitTime > 0L) {
      // After this time, the task must be timeout.
      Thread.sleep(waitTime);
    }
    scheduler.toAborted(task);
  }
}
