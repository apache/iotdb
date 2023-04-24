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

package org.apache.iotdb.db.pipe.task.stage;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.PipeSubtaskManager;
import org.apache.iotdb.db.pipe.task.callable.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.TreeMap;

public class PipeTaskConnectorStage implements PipeTaskStage {

  protected final PipeSubtaskExecutor executor;
  protected final PipeSubtask subtask;
  private final PipeSubtaskManager subtaskManager;

  private final String pipeConnectorAttributes;

  protected PipeTaskConnectorStage(
      PipeConnectorSubtaskExecutor executor, PipeConnectorSubtask subtask, String pipeName) {
    this.executor = executor;
    this.subtask = subtask;
    // TODO: get the connector attributes from the constructor's parameter
    this.pipeConnectorAttributes =
        new TreeMap<>(
                PipeAgent.task().getPipeMeta(pipeName).getStaticMeta().getConnectorAttributes())
            .toString();

    subtaskManager = PipeSubtaskManager.setupAndGetInstance();
  }

  @Override
  public void create() throws PipeException {
    if (subtaskManager.increaseAlivePipePluginRef(pipeConnectorAttributes) == 1) {
      executor.register(subtask);
    }
  }

  @Override
  public void start() throws PipeException {
    if (subtaskManager.increaseRuntimePipePluginRef(pipeConnectorAttributes) == 1) {
      executor.start(subtask.getTaskID());
    }
  }

  @Override
  public void stop() throws PipeException {
    if (!subtaskManager.decreaseRuntimePipePluginRef(pipeConnectorAttributes)) {
      executor.stop(subtask.getTaskID());
    }
  }

  @Override
  public void drop() throws PipeException {
    if (!subtaskManager.decreaseAlivePipePluginRef(pipeConnectorAttributes)) {
      executor.deregister(subtask.getTaskID());
    }
  }

  @Override
  public PipeSubtask getSubtask() {
    return subtask;
  }
}
