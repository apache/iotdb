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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.commons.pipe.task.stage.PipeTaskStage;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtaskExecutor;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Map;

public class PipeConfigNodeTaskStage extends PipeTaskStage {

  private final PipeConfigNodeSubtask subtask;

  public PipeConfigNodeTaskStage(
      String pipeName,
      long creationTime,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes) {

    try {
      subtask =
          new PipeConfigNodeSubtask(
              pipeName,
              creationTime,
              extractorAttributes,
              processorAttributes,
              connectorAttributes);
    } catch (Exception e) {
      throw new PipeException(
          String.format(
              "Failed to create subtask for pipe %s, creation time %d", pipeName, creationTime),
          e);
    }
  }

  @Override
  public void createSubtask() throws PipeException {
    PipeConfigNodeSubtaskExecutor.getInstance().register(subtask);
  }

  @Override
  public void startSubtask() throws PipeException {
    PipeConfigNodeSubtaskExecutor.getInstance().start(subtask.getTaskID());
  }

  @Override
  public void stopSubtask() throws PipeException {
    PipeConfigNodeSubtaskExecutor.getInstance().stop(subtask.getTaskID());
  }

  @Override
  public void dropSubtask() throws PipeException {
    PipeConfigNodeSubtaskExecutor.getInstance().deregister(subtask.getTaskID());
  }
}
