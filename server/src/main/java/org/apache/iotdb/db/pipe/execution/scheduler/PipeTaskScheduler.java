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

import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutorManager;

/**
 * PipeTaskScheduler is a singleton class that manages the numbers of threads used by
 * PipeTaskExecutors dynamically.
 */
public class PipeTaskScheduler {

  private final PipeSubtaskExecutorManager pipeSubtaskExecutorManager =
      PipeSubtaskExecutorManager.getInstance();

  public void adjustAssignerSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeSubtaskExecutorManager.getAssignerSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getAssignerSubtaskExecutorThreadNum() {
    return pipeSubtaskExecutorManager.getAssignerSubtaskExecutor().getExecutorThreadNumber();
  }

  public void adjustConnectorSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeSubtaskExecutorManager.getConnectorSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getConnectorSubtaskExecutorThreadNum() {
    return pipeSubtaskExecutorManager.getConnectorSubtaskExecutor().getExecutorThreadNumber();
  }

  public void adjustProcessorSubtaskExecutorThreadNum(int threadNum) {
    // TODO: make it configurable by setting different parameters
    pipeSubtaskExecutorManager.getProcessorSubtaskExecutor().adjustExecutorThreadNumber(threadNum);
  }

  public int getProcessorSubtaskExecutorThreadNum() {
    return pipeSubtaskExecutorManager.getProcessorSubtaskExecutor().getExecutorThreadNumber();
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
