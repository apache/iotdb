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
package org.apache.iotdb.mpp.execution;

import org.apache.iotdb.mpp.execution.queue.IndexedBlockingQueue;
import org.apache.iotdb.mpp.execution.task.FragmentInstanceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** the thread for watching the timeout of {@link FragmentInstanceTask} */
public class FragmentInstanceTimeoutSentinel extends Thread {

  private static final Logger logger =
      LoggerFactory.getLogger(FragmentInstanceTimeoutSentinel.class);

  private final IndexedBlockingQueue<FragmentInstanceTask> queue;

  public FragmentInstanceTimeoutSentinel(
      String workerId, ThreadGroup tg, IndexedBlockingQueue<FragmentInstanceTask> queue) {
    super(tg, workerId);
    this.queue = queue;
  }

  @Override
  public void run() {
    try {
      while (true) {
        FragmentInstanceTask next = queue.poll();
        // do logic here
      }
    } catch (InterruptedException e) {
      logger.info("{} is interrupted.", this.getName());
    }
  }
}
