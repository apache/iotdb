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

package org.apache.iotdb.db.pipe.agent;

import org.apache.iotdb.db.pipe.agent.plugin.PipePluginAgent;
import org.apache.iotdb.db.pipe.agent.runtime.PipeRuntimeAgent;
import org.apache.iotdb.db.pipe.agent.task.PipeTaskAgent;

/** PipeAgent is the entry point of the pipe module in DatNode. */
public class PipeAgent {

  private final PipePluginAgent pipePluginAgent;
  private final PipeTaskAgent pipeTaskAgent;
  private final PipeRuntimeAgent pipeRuntimeAgent;

  /** Private constructor to prevent users from creating a new instance. */
  private PipeAgent() {
    pipePluginAgent = new PipePluginAgent();
    pipeTaskAgent = new PipeTaskAgent();
    pipeRuntimeAgent = new PipeRuntimeAgent();
  }

  /** The singleton holder of PipeAgent. */
  private static class PipeAgentHolder {
    private static final PipeAgent HANDLE = new PipeAgent();
  }

  /**
   * Get the singleton instance of PipeTaskAgent.
   *
   * @return the singleton instance of PipeTaskAgent
   */
  public static PipeTaskAgent task() {
    return PipeAgentHolder.HANDLE.pipeTaskAgent;
  }

  /**
   * Get the singleton instance of PipePluginAgent.
   *
   * @return the singleton instance of PipePluginAgent
   */
  public static PipePluginAgent plugin() {
    return PipeAgentHolder.HANDLE.pipePluginAgent;
  }

  /**
   * Get the singleton instance of PipeRuntimeAgent.
   *
   * @return the singleton instance of PipeRuntimeAgent
   */
  public static PipeRuntimeAgent runtime() {
    return PipeAgentHolder.HANDLE.pipeRuntimeAgent;
  }
}
