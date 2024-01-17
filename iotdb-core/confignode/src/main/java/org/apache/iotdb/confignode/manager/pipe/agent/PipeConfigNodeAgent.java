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

package org.apache.iotdb.confignode.manager.pipe.agent;

import org.apache.iotdb.confignode.manager.pipe.agent.plugin.PipePluginConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeTaskConfigNodeAgent;

/** PipeAgent is the entry point of the pipe module in DataNode. */
public class PipeConfigNodeAgent {

  private final PipeTaskConfigNodeAgent pipeConfigNodeTaskAgent;
  private final PipePluginConfigNodeAgent pipePluginConfigNodeAgent;

  /** Private constructor to prevent users from creating a new instance. */
  private PipeConfigNodeAgent() {
    pipeConfigNodeTaskAgent = new PipeTaskConfigNodeAgent();
    pipePluginConfigNodeAgent = new PipePluginConfigNodeAgent(null);
  }

  /** The singleton holder of PipeAgent. */
  private static class PipeConfigNodeAgentHolder {
    private static final PipeConfigNodeAgent HANDLE = new PipeConfigNodeAgent();
  }

  /**
   * Get the singleton instance of PipeTaskAgent.
   *
   * @return the singleton instance of PipeTaskAgent
   */
  public static PipeTaskConfigNodeAgent task() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodeTaskAgent;
  }

  public static PipePluginConfigNodeAgent plugin() {
    return PipeConfigNodeAgentHolder.HANDLE.pipePluginConfigNodeAgent;
  }
}
