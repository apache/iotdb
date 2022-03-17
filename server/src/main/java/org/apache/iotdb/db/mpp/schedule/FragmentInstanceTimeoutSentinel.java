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
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** the thread for watching the timeout of {@link FragmentInstanceTask} */
public class FragmentInstanceTimeoutSentinel extends Thread {

  private static final Logger logger =
      LoggerFactory.getLogger(FragmentInstanceTimeoutSentinel.class);

  private final IndexedBlockingQueue<FragmentInstanceTask> queue;
  private final FragmentInstanceTaskCallback timeoutCallback;
  // the check interval in milliseconds if the queue head remains the same.
  private static final int CHECK_INTERVAL = 100;

  public FragmentInstanceTimeoutSentinel(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<FragmentInstanceTask> queue,
      FragmentInstanceTaskCallback timeoutCallback) {
    super(tg, workerId);
    this.queue = queue;
    this.timeoutCallback = timeoutCallback;
  }

  @Override
  public void run() {
    while (true) {
      try {
        FragmentInstanceTask next = queue.poll();
        next.lock();
        try {
          // if this task is already in an end state, it means that the resource releasing will be
          // handled by other threads, we don't care anymore.
          if (next.isEndState()) {
            continue;
          }
          // if this task is not in end state and not timeout, we should push it back to the queue.
          if (next.getDDL() > System.currentTimeMillis()) {
            queue.push(next);
            Thread.sleep(CHECK_INTERVAL);
            continue;
          }
          next.setStatus(FragmentInstanceTaskStatus.ABORTED);
        } finally {
          next.unlock();
        }
        try {
          // Or we should do something to abort
          timeoutCallback.call(next);
        } catch (Exception e) {
          logger.error("Abort instance " + next.getId() + " failed", e);
        }
      } catch (InterruptedException e) {
        logger.info("{} is interrupted.", this.getName());
        break;
      }
    }
  }
}
