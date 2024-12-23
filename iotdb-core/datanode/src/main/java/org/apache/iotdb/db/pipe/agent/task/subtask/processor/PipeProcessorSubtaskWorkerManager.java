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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.atomic.AtomicLong;

public class PipeProcessorSubtaskWorkerManager {

  private static final int MAX_THREAD_NUM =
      PipeConfig.getInstance().getPipeSubtaskExecutorMaxThreadNum();

  private final PipeProcessorSubtaskWorker[] workers;

  private final AtomicLong scheduledTaskNumber;

  public PipeProcessorSubtaskWorkerManager(ListeningExecutorService workerThreadPoolExecutor) {
    workers = new PipeProcessorSubtaskWorker[MAX_THREAD_NUM];
    for (int i = 0; i < MAX_THREAD_NUM; i++) {
      workers[i] = new PipeProcessorSubtaskWorker();
      workerThreadPoolExecutor.submit(workers[i]);
    }

    scheduledTaskNumber = new AtomicLong(0);
  }

  public void schedule(PipeProcessorSubtask pipeProcessorSubtask) {
    workers[(int) (scheduledTaskNumber.getAndIncrement() % MAX_THREAD_NUM)].schedule(
        pipeProcessorSubtask);
  }
}
