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

package org.apache.iotdb.db.pipe.execution.executor;

/**
 * PipeTaskExecutor is responsible for executing the pipe tasks, and it is scheduled by the
 * PipeTaskScheduler. It is a singleton class.
 */
public class PipeTaskExecutor {

  private final PipeAssignerSubtaskExecutor assignerSubtaskExecutor =
      new PipeAssignerSubtaskExecutor();
  private final PipeProcessorSubtaskExecutor processorSubtaskExecutor =
      new PipeProcessorSubtaskExecutor();
  private final PipeConnectorSubtaskExecutor connectorSubtaskExecutor =
      new PipeConnectorSubtaskExecutor();

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeTaskExecutor() {}

  private static class PipeTaskExecutorHolder {
    private static PipeTaskExecutor instance = null;
  }

  public static PipeTaskExecutor setupAndGetInstance() {
    if (PipeTaskExecutorHolder.instance == null) {
      PipeTaskExecutorHolder.instance = new PipeTaskExecutor();
    }
    return PipeTaskExecutorHolder.instance;
  }
}
