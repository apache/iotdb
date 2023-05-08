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

import org.apache.iotdb.db.pipe.execution.executor.PipeTaskExecutorManager;

/**
 * PipeTaskScheduler is a singleton class that manages the numbers of threads used by
 * PipeTaskExecutors dynamically.
 */
public class PipeTaskScheduler {

  private final PipeTaskExecutorManager pipeTaskExecutorManager =
      PipeTaskExecutorManager.setupAndGetInstance();

  public void adjustAssignerSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeTaskExecutorManager.getAssignerSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getAssignerSubtaskExecutorThreadNum() {
    return pipeTaskExecutorManager.getAssignerSubtaskExecutor().getExecutorThreadNumber();
  }

  public void adjustConnectorSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeTaskExecutorManager.getConnectorSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getConnectorSubtaskExecutorThreadNum() {
    return pipeTaskExecutorManager.getConnectorSubtaskExecutor().getExecutorThreadNumber();
  }

  public void adjustProcessorSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeTaskExecutorManager.getProcessorSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getProcessorSubtaskExecutorThreadNum() {
    return pipeTaskExecutorManager.getProcessorSubtaskExecutor().getExecutorThreadNumber();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeTaskScheduler() {}

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
