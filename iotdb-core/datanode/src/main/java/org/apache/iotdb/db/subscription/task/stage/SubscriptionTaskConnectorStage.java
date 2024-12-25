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

package org.apache.iotdb.db.subscription.task.stage;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskConnectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionConnectorSubtaskManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class SubscriptionTaskConnectorStage extends PipeTaskConnectorStage {

  public SubscriptionTaskConnectorStage(
      String pipeName,
      long creationTime,
      PipeParameters pipeConnectorParameters,
      int regionId,
      PipeConnectorSubtaskExecutor executor) {
    super(pipeName, creationTime, pipeConnectorParameters, regionId, executor);
  }

  @Override
  protected void registerSubtask() {
    this.connectorSubtaskId =
        SubscriptionConnectorSubtaskManager.instance()
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
    SubscriptionConnectorSubtaskManager.instance().start(connectorSubtaskId);
  }

  @Override
  public void stopSubtask() throws PipeException {
    SubscriptionConnectorSubtaskManager.instance().stop(connectorSubtaskId);
  }

  @Override
  public void dropSubtask() throws PipeException {
    SubscriptionConnectorSubtaskManager.instance()
        .deregister(pipeName, creationTime, regionId, connectorSubtaskId);
  }

  public UnboundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue() {
    return SubscriptionConnectorSubtaskManager.instance()
        .getPipeConnectorPendingQueue(connectorSubtaskId);
  }
}
