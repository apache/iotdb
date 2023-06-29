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
@SuppressWarnings("unused") // assignerSubtaskExecutor is for future use
public class PipeSubtaskExecutorManager {

  private final PipeAssignerSubtaskExecutor assignerSubtaskExecutor;
  private final PipeProcessorSubtaskExecutor processorSubtaskExecutor;
  private final PipeConnectorSubtaskExecutor connectorSubtaskExecutor;

  public PipeAssignerSubtaskExecutor getAssignerSubtaskExecutor() {
    throw new UnsupportedOperationException("currently not needed");
  }

  public PipeProcessorSubtaskExecutor getProcessorSubtaskExecutor() {
    return processorSubtaskExecutor;
  }

  public PipeConnectorSubtaskExecutor getConnectorSubtaskExecutor() {
    return connectorSubtaskExecutor;
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeSubtaskExecutorManager() {
    assignerSubtaskExecutor = null;
    processorSubtaskExecutor = new PipeProcessorSubtaskExecutor();
    connectorSubtaskExecutor = new PipeConnectorSubtaskExecutor();
  }

  private static class PipeTaskExecutorHolder {
    private static PipeSubtaskExecutorManager instance = null;
  }

  public static synchronized PipeSubtaskExecutorManager getInstance() {
    if (PipeTaskExecutorHolder.instance == null) {
      PipeTaskExecutorHolder.instance = new PipeSubtaskExecutorManager();
    }
    return PipeTaskExecutorHolder.instance;
  }
}
