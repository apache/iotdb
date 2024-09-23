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

package org.apache.iotdb.db.pipe.agent.task.stage;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.stage.PipeTaskStage;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskConnectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeConnectorSubtaskManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskConnectorStage extends PipeTaskStage {

  protected final String pipeName;
  protected final long creationTime;
  protected final PipeParameters pipeConnectorParameters;
  protected final int regionId;
  protected final PipeConnectorSubtaskExecutor executor;

  protected String connectorSubtaskId;

  public PipeTaskConnectorStage(
      String pipeName,
      long creationTime,
      PipeParameters pipeConnectorParameters,
      int regionId,
      PipeConnectorSubtaskExecutor executor) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.pipeConnectorParameters = pipeConnectorParameters;
    this.regionId = regionId;
    this.executor = executor;

    registerSubtask();
  }

  protected void registerSubtask() {
    this.connectorSubtaskId =
        PipeConnectorSubtaskManager.instance()
            .register(
                executor,
                pipeConnectorParameters,
                new PipeTaskConnectorRuntimeEnvironment(pipeName, creationTime, regionId));
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
    PipeConnectorSubtaskManager.instance()
        .deregister(pipeName, creationTime, regionId, connectorSubtaskId);
  }

  public UnboundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue() {
    return PipeConnectorSubtaskManager.instance().getPipeConnectorPendingQueue(connectorSubtaskId);
  }
}
