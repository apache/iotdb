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

import org.apache.iotdb.db.pipe.execution.executor.PipeTaskExecutor;

/**
 * PipeTaskScheduler is a singleton class that manages the numbers of threads used by
 * PipeTaskExecutors dynamically.
 */
public class PipeTaskScheduler {

  private PipeTaskExecutor pipeTaskExecutor;

  void setPipeTaskExecutor(PipeTaskExecutor pipeTaskExecutor) {
    this.pipeTaskExecutor = pipeTaskExecutor;
  }

  public void setAssignerSubtaskExecutorThreadNum(int threadNum) {
    pipeTaskExecutor.getAssignerSubtaskExecutor().setExecutorThreadNum(threadNum);
  }

  public int getAssignerSubtaskExecutorThreadNum() {
    return pipeTaskExecutor.getAssignerSubtaskExecutor().getExecutorThreadNum();
  }

  public void setConnectorSubtaskExecutorThreadNum(int threadNum) {
    pipeTaskExecutor.getConnectorSubtaskExecutor().setExecutorThreadNum(threadNum);
  }

  public int getConnectorSubtaskExecutorThreadNum() {
    return pipeTaskExecutor.getConnectorSubtaskExecutor().getExecutorThreadNum();
  }

  public void setProcessorSubtaskExecutorThreadNum(int threadNum) {
    pipeTaskExecutor.getProcessorSubtaskExecutor().setExecutorThreadNum(threadNum);
  }

  public int getProcessorSubtaskExecutorThreadNum() {
    return pipeTaskExecutor.getProcessorSubtaskExecutor().getExecutorThreadNum();
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
