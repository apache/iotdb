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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeRuntimeAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeAgent.class);

  public void report(PipeSubtask subtask) {
    // TODO: terminate the task by the given taskID
    LOGGER.warn(
        "Failed to execute task {} after many retries, last failed cause by {}",
        subtask.getTaskID(),
        subtask.getLastFailedCause());
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeRuntimeAgent() {}

  private static class PipeRuntimeAgentHolder {
    private static PipeRuntimeAgent INSTANCE = null;
  }

  public static PipeRuntimeAgent setupAndGetInstance() {
    if (PipeRuntimeAgentHolder.INSTANCE == null) {
      PipeRuntimeAgentHolder.INSTANCE = new PipeRuntimeAgent();
    }
    return PipeRuntimeAgentHolder.INSTANCE;
  }
}
