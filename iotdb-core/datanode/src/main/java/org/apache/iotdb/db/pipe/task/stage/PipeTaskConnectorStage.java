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

import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtaskManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskConnectorStage extends PipeTaskStage {

  protected final PipeParameters pipeConnectorParameters;

  protected String connectorSubtaskId;

  public PipeTaskConnectorStage(
      String pipeName, long creationTime, PipeParameters pipeConnectorParameters) {
    this.pipeConnectorParameters = pipeConnectorParameters;

    connectorSubtaskId =
        PipeConnectorSubtaskManager.instance()
            .register(
                PipeSubtaskExecutorManager.getInstance().getConnectorSubtaskExecutor(),
                pipeConnectorParameters,
                new PipeTaskRuntimeEnvironment(pipeName, creationTime));
  }

  @Override
  public void createSubtask() throws PipeException {
    // Do nothing
  }

  @Override
  public void startSubtask() throws PipeException {
    PipeConnectorSubtaskManager.instance().start(connectorSubtaskId);
  }

  @Override
  public void stopSubtask() throws PipeException {
    PipeConnectorSubtaskManager.instance().stop(connectorSubtaskId);
  }

  @Override
  public void dropSubtask() throws PipeException {
    PipeConnectorSubtaskManager.instance().deregister(connectorSubtaskId);
  }

  public BoundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue() {
    return PipeConnectorSubtaskManager.instance().getPipeConnectorPendingQueue(connectorSubtaskId);
  }
}
