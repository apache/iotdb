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
package org.apache.iotdb.commons.concurrent.pipeline;

import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;

import java.util.concurrent.BlockingQueue;

/**
 * A thread that will continuously take tasks from an input queue, run the task, and insert the next
 * task to the output queue. By connecting multiple TaskRunners with input queues and output queues,
 * a pipeline is formed.
 */
public class TaskRunner extends DynamicThread {

  private Runnable cleanUp;
  private BlockingQueue<Task> input;
  private BlockingQueue<Task> output;
  private String taskName;

  public TaskRunner(
      DynamicThreadGroup threadGroup,
      Runnable cleanUp,
      BlockingQueue<Task> input,
      BlockingQueue<Task> output,
      String taskName) {
    super(threadGroup);
    this.cleanUp = cleanUp;
    this.input = input;
    this.output = output;
    this.taskName = taskName;
  }

  @Override
  public void runInternal() {
    Thread.currentThread().setName(Thread.currentThread().getName() + "-" + taskName);
    while (!Thread.interrupted()) {
      Task task;
      try {
        task = input.take();
        idleToRunning();
        task.run();
        Task nextTask = task.nextTask();
        if (nextTask != null) {
          output.put(nextTask);
        }
        runningToIdle();

        if (shouldExit()) {
          break;
        }
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    cleanUp.run();
  }
}
