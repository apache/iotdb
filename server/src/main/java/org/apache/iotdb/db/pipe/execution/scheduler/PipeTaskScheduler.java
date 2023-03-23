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

package org.apache.iotdb.db.pipe.execution.scheduler;

import org.apache.iotdb.db.pipe.task.PipeTask;

/**
 * PipeTaskScheduler is responsible for scheduling the pipe tasks. It takes the pipe tasks and
 * executes them in the PipeTaskExecutor. It is a singleton class.
 */
public class PipeTaskScheduler {

  private final PipeSubtaskScheduler assignerSubtaskScheduler;
  private final PipeSubtaskScheduler processorSubtaskScheduler;
  private final PipeSubtaskScheduler connectorSubtaskScheduler;

  public void createPipeTask(PipeTask pipeTask) {}

  public void dropPipeTask(String pipeName) {}

  public void startPipeTask(String pipeName) {}

  public void stopPipeTask(String pipeName) {}

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeTaskScheduler() {
    assignerSubtaskScheduler = new PipeAssignerSubtaskScheduler();
    processorSubtaskScheduler = new PipeProcessorSubtaskScheduler();
    connectorSubtaskScheduler = new PipeConnectorSubtaskScheduler();
  }

  private static class PipeTaskSchedulerHolder {
    private static PipeTaskScheduler instance = null;
  }

  public static PipeTaskScheduler setupAndGetInstance() {
    if (PipeTaskSchedulerHolder.instance == null) {
      PipeTaskSchedulerHolder.instance = new PipeTaskScheduler();
    }
    return PipeTaskSchedulerHolder.instance;
  }
}
