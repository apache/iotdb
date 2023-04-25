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

import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;
import org.apache.iotdb.pipe.api.exception.PipeException;

public abstract class PipeTaskStage {

  protected final PipeSubtaskExecutor executor;
  protected final PipeSubtask subtask;

  protected PipeTaskStage(PipeSubtaskExecutor executor, PipeSubtask subtask) {
    this.executor = executor;
    this.subtask = subtask;
  }

  /**
   * Create a pipe task stage.
   *
   * @throws PipeException if failed to create a pipe task stage.
   */
  public final void create() throws PipeException {
    executor.register(subtask);
  }

  /**
   * Start a pipe task stage.
   *
   * @throws PipeException if failed to start a pipe task stage.
   */
  public final void start() throws PipeException {
    executor.start(subtask.getTaskID());
  }

  /**
   * Stop a pipe task stage.
   *
   * @throws PipeException if failed to stop a pipe task stage.
   */
  public final void stop() throws PipeException {
    executor.stop(subtask.getTaskID());
  }

  /**
   * Drop a pipe task stage.
   *
   * @throws PipeException if failed to drop a pipe task stage.
   */
  public final void drop() throws PipeException {
    executor.deregister(subtask.getTaskID());
  }

  /**
   * Get the pipe subtask.
   *
   * @return the pipe subtask.
   */
  public final PipeSubtask getSubtask() {
    return subtask;
  }
}
