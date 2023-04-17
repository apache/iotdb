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

import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.PipeSubtaskManager;
import org.apache.iotdb.db.pipe.task.callable.PipeConnectorSubtask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskConnectorStage extends PipeTaskAbstractStage {

  private final PipeSubtaskManager subtaskManager;

  protected PipeTaskConnectorStage(
      PipeConnectorSubtaskExecutor executor, PipeConnectorSubtask subtask) {
    super(executor, subtask);
    subtaskManager = PipeSubtaskManager.setupAndGetInstance();
  }

  @Override
  public void create() throws PipeException {
    if (subtaskManager.increaseAlivePipePluginRef(subtask.getPipePluginName()) == 1) {
      super.create();
    }
  }

  @Override
  public void start() throws PipeException {
    if (!((PipeConnectorSubtask) subtask).isPendingQueueEmpty()
        && subtaskManager.increaseRuntimePipePluginRef(subtask.getPipePluginName()) == 1) {
      super.start();
    }
  }

  @Override
  public void stop() throws PipeException {
    if (!subtaskManager.decreaseRuntimePipePluginRef(subtask.getPipePluginName())) {
      super.stop();
    }
  }

  @Override
  public void drop() throws PipeException {
    if (!subtaskManager.decreaseAlivePipePluginRef(subtask.getPipePluginName())) {
      super.drop();
    }
  }

  public boolean consumeEvent(Event event) {
    return ((PipeConnectorSubtask) subtask).offerEvent(event);
  }
}
