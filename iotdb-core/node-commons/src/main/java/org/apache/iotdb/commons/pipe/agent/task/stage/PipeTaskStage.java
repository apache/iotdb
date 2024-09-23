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

package org.apache.iotdb.commons.pipe.agent.task.stage;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.pipe.api.exception.PipeException;

public abstract class PipeTaskStage {

  private static final String MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_STARTED =
      "The PipeTaskStage has been started";
  private static final String MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_DROPPED =
      "The PipeTaskStage has been dropped";
  private static final String MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_STOPPED =
      "The PipeTaskStage has been externally stopped";
  private static final String MESSAGE_PIPE_TASK_STAGE_HAS_NOT_BEEN_CREATED =
      "The PipeTaskStage has not been created";

  protected PipeStatus status = null;
  protected boolean hasBeenExternallyStopped = false;

  /**
   * Create a {@link PipeTaskStage}.
   *
   * @throws PipeException if failed to create a {@link PipeTaskStage}.
   */
  public synchronized void create() {
    if (status != null) {
      if (status == PipeStatus.RUNNING) {
        throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_STARTED);
      }
      if (status == PipeStatus.DROPPED) {
        throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_DROPPED);
      }
      // status == PipeStatus.STOPPED
      if (hasBeenExternallyStopped) {
        throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_STOPPED);
      }
      // Otherwise, do nothing to allow retry strategy
      return;
    }

    // Status == null, register the subtask
    createSubtask();

    status = PipeStatus.STOPPED;
  }

  protected abstract void createSubtask() throws PipeException;

  /**
   * Start a {@link PipeTaskStage}.
   *
   * @throws PipeException if failed to start a {@link PipeTaskStage}.
   */
  public synchronized void start() {
    if (status == null) {
      throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_NOT_BEEN_CREATED);
    }
    if (status == PipeStatus.RUNNING) {
      // Do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_DROPPED);
    }

    // status == PipeStatus.STOPPED, start the subtask
    startSubtask();

    status = PipeStatus.RUNNING;
  }

  protected abstract void startSubtask() throws PipeException;

  /**
   * Stop a {@link PipeTaskStage}.
   *
   * @throws PipeException if failed to stop a {@link PipeTaskStage}.
   */
  public synchronized void stop() {
    if (status == null) {
      throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_NOT_BEEN_CREATED);
    }
    if (status == PipeStatus.STOPPED) {
      // Do nothing to allow retry strategy
      return;
    }
    if (status == PipeStatus.DROPPED) {
      throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_BEEN_DROPPED);
    }

    // status == PipeStatus.RUNNING, stop the connector
    stopSubtask();

    status = PipeStatus.STOPPED;
    hasBeenExternallyStopped = true;
  }

  protected abstract void stopSubtask() throws PipeException;

  /**
   * Drop a {@link PipeTaskStage}.
   *
   * @throws PipeException if failed to drop a {@link PipeTaskStage}.
   */
  public synchronized void drop() {
    if (status == null) {
      throw new PipeException(MESSAGE_PIPE_TASK_STAGE_HAS_NOT_BEEN_CREATED);
    }
    if (status == PipeStatus.DROPPED) {
      // Do nothing to allow retry strategy
      return;
    }

    // MUST stop the subtask before dropping it, otherwise
    // the subtask might be in an inconsistent state!
    stop();

    // status == PipeStatus.STOPPED, drop the connector
    dropSubtask();

    status = PipeStatus.DROPPED;
  }

  protected abstract void dropSubtask() throws PipeException;
}
