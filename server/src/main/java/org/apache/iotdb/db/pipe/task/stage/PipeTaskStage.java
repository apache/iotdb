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
import org.apache.iotdb.pipe.api.exception.PipeException;

public abstract class PipeTaskStage {

  protected PipeStatus status = null;
  protected boolean hasBeenExternallyStopped = false;

  /**
   * Create a pipe task stage.
   *
   * @throws PipeException if failed to create a pipe task stage.
   */
  public synchronized void create() {
    if (status != null) {
      if (status == PipeStatus.RUNNING) {
        throw new PipeException("The PipeTaskStage has been started");
      }
      if (status == PipeStatus.DROPPED) {
        throw new PipeException("The PipeTaskStage has been dropped");
      }
      // status == PipeStatus.STOPPED
      if (hasBeenExternallyStopped) {
        throw new PipeException("The PipeTaskStage has been externally stopped");
      }
      // otherwise, do nothing to allow retry strategy
      return;
    }

    // status == null, register the subtask
    createSubtask();

    status = PipeStatus.STOPPED;
  }

  protected abstract void createSubtask() throws PipeException;

  /**
   * Start a pipe task stage.
   *
   * @throws PipeException if failed to start a pipe task stage.
   */
  public synchronized void start() {
    if (status == null) {
      throw new PipeException("The PipeTaskStage has not been created");
    }
    if (status == PipeStatus.RUNNING) {
      // do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException("The PipeTaskStage has been dropped");
    }

    // status == PipeStatus.STOPPED, start the subtask
    startSubtask();

    status = PipeStatus.RUNNING;
  }

  protected abstract void startSubtask() throws PipeException;

  /**
   * Stop a pipe task stage.
   *
   * @throws PipeException if failed to stop a pipe task stage.
   */
  public synchronized void stop() {
    if (status == null) {
      throw new PipeException("The PipeTaskStage has not been created");
    }
    if (status == PipeStatus.STOPPED) {
      // do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException("The PipeTaskStage has been dropped");
    }

    // status == PipeStatus.RUNNING, stop the connector
    stopSubtask();

    status = PipeStatus.STOPPED;
    hasBeenExternallyStopped = true;
  }

  protected abstract void stopSubtask() throws PipeException;

  /**
   * Drop a pipe task stage.
   *
   * @throws PipeException if failed to drop a pipe task stage.
   */
  public synchronized void drop() {
    if (status == null) {
      throw new PipeException("The PipeTaskStage has not been created");
    }
    if (status == PipeStatus.DROPPED) {
      // do nothing to allow retry strategy
      return;
    }

    // status == PipeStatus.RUNNING or PipeStatus.STOPPED, drop the connector
    dropSubtask();

    status = PipeStatus.DROPPED;
  }

  protected abstract void dropSubtask() throws PipeException;
}
