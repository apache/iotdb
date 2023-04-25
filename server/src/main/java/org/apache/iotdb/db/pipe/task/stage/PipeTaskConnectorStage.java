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

import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.db.pipe.core.connector.PipeConnectorSubtaskManager;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskConnectorStage implements PipeTaskStage {

  protected final PipeConnectorSubtaskExecutor executor;
  protected final PipeParameters connectorAttributes;

  protected PipeStatus status = null;
  protected String connectorSubtaskId = null;
  protected boolean hasBeenExternallyStopped = false;

  protected PipeTaskConnectorStage(
      PipeConnectorSubtaskExecutor executor, PipeParameters connectorAttributes) {
    this.executor = executor;
    this.connectorAttributes = connectorAttributes;
  }

  @Override
  public synchronized void create() throws PipeException {
    if (status != null) {
      if (status == PipeStatus.RUNNING) {
        throw new PipeException(
            String.format("The PipeConnectorSubtask %s has been started", connectorSubtaskId));
      }
      if (status == PipeStatus.DROPPED) {
        throw new PipeException(
            String.format("The PipeConnectorSubtask %s has been dropped", connectorSubtaskId));
      }
      // status == PipeStatus.STOPPED
      if (hasBeenExternallyStopped) {
        throw new PipeException(
            String.format(
                "The PipeConnectorSubtask %s has been externally stopped", connectorSubtaskId));
      }
      // otherwise, do nothing to allow retry strategy
      return;
    }

    // status == null, register the connector
    connectorSubtaskId =
        PipeConnectorSubtaskManager.instance().register(executor, connectorAttributes);
    status = PipeStatus.STOPPED;
  }

  @Override
  public synchronized void start() throws PipeException {
    if (status == null) {
      throw new PipeException(
          String.format("The PipeConnectorSubtask %s has not been created", connectorSubtaskId));
    }
    if (status == PipeStatus.RUNNING) {
      // do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException(
          String.format("The PipeConnectorSubtask %s has been dropped", connectorSubtaskId));
    }

    // status == PipeStatus.STOPPED, start the connector
    PipeConnectorSubtaskManager.instance().start(connectorSubtaskId);
    status = PipeStatus.RUNNING;
  }

  @Override
  public synchronized void stop() throws PipeException {
    if (status == null) {
      throw new PipeException(
          String.format("The PipeConnectorSubtask %s has not been created", connectorSubtaskId));
    }
    if (status == PipeStatus.STOPPED) {
      // do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException(
          String.format("The PipeConnectorSubtask %s has been dropped", connectorSubtaskId));
    }

    // status == PipeStatus.RUNNING, stop the connector
    PipeConnectorSubtaskManager.instance().stop(connectorSubtaskId);
    status = PipeStatus.STOPPED;
    hasBeenExternallyStopped = true;
  }

  @Override
  public synchronized void drop() throws PipeException {
    if (status == null) {
      throw new PipeException(
          String.format("The PipeConnectorSubtask %s has not been created", connectorSubtaskId));
    }
    if (status == PipeStatus.DROPPED) {
      // do nothing to allow retry strategy
      return;
    }

    // status == PipeStatus.RUNNING or PipeStatus.STOPPED, drop the connector
    PipeConnectorSubtaskManager.instance().deregister(connectorSubtaskId);
    status = PipeStatus.DROPPED;
  }

  @Override
  public PipeSubtask getSubtask() {
    return PipeConnectorSubtaskManager.instance().getPipeConnectorSubtask(connectorSubtaskId);
  }
}
