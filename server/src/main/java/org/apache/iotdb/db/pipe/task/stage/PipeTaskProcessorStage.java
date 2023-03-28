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

import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.callable.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskProcessorStage implements PipeTaskStage {

  private PipeProcessorSubtaskExecutor executor;
  private PipeProcessorSubtask subtask;

  @Override
  public void create() throws PipeException {
    executor.putSubtask(subtask);
  }

  @Override
  public void start() throws PipeException {
    executor.submit(subtask);
  }

  @Override
  public void stop() throws PipeException {
    executor.stop();
  }

  @Override
  public void drop() throws PipeException {
    executor.removeSubtask(subtask.getTaskID());
  }

  @Override
  public PipeSubtask getSubtask() {
    return subtask;
  }

  @Override
  public void bind(PipeSubtaskExecutor executor) {
    this.executor = (PipeProcessorSubtaskExecutor) executor;
  }
}
