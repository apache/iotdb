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

import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;
import org.apache.iotdb.pipe.api.exception.PipeException;

public interface PipeTaskStage {

  /**
   * Create a pipe task stage.
   *
   * @throws PipeException if failed to create a pipe task stage.
   */
  void create() throws PipeException;
  /**
   * Start a pipe task stage.
   *
   * @throws PipeException if failed to start a pipe task stage.
   */
  void start() throws PipeException;

  /**
   * Stop a pipe task stage.
   *
   * @throws PipeException if failed to stop a pipe task stage.
   */
  void stop() throws PipeException;

  /**
   * Drop a pipe task stage.
   *
   * @throws PipeException if failed to drop a pipe task stage.
   */
  void drop() throws PipeException;

  /**
   * Get the pipe subtask.
   *
   * @return the pipe subtask.
   */
  PipeSubtask getSubtask();
}
