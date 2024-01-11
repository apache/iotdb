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

package org.apache.iotdb.confignode.manager.pipe.transfer.agent;

import org.apache.iotdb.confignode.manager.pipe.transfer.agent.plugin.PipePluginConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.receiver.PipeReceiverConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.runtime.PipeRuntimeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.task.PipeTaskConfigNodeAgent;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.agent.plugin.PipePluginDataNodeAgent;

/** {@link PipeConfigNodeAgent} is the entry point of the pipe module in {@link ConfigNode}. */
public class PipeConfigNodeAgent {

  private final PipeTaskConfigNodeAgent pipeConfigNodeTaskAgent;
  private final PipePluginConfigNodeAgent pipePluginConfigNodeAgent;
  private final PipeRuntimeConfigNodeAgent pipeRuntimeConfigNodeAgent;
  private final PipeReceiverConfigNodeAgent pipeReceiverConfigNodeAgent;

  /** Private constructor to prevent users from creating a new instance. */
  private PipeConfigNodeAgent() {
    pipeConfigNodeTaskAgent = new PipeTaskConfigNodeAgent();
    pipePluginConfigNodeAgent = new PipePluginConfigNodeAgent(null);
    pipeRuntimeConfigNodeAgent = new PipeRuntimeConfigNodeAgent();
    pipeReceiverConfigNodeAgent = new PipeReceiverConfigNodeAgent();
  }

  /** The singleton holder of {@link PipeConfigNodeAgent}. */
  private static class PipeConfigNodeAgentHolder {
    private static final PipeConfigNodeAgent HANDLE = new PipeConfigNodeAgent();
  }

  /**
   * Get the singleton instance of {@link PipeTaskConfigNodeAgent}.
   *
   * @return the singleton instance of {@link PipeTaskConfigNodeAgent}
   */
  public static PipeTaskConfigNodeAgent task() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodeTaskAgent;
  }

  /**
   * Get the singleton instance of {@link PipePluginDataNodeAgent}.
   *
   * @return the singleton instance of {@link PipePluginDataNodeAgent}
   */
  public static PipePluginConfigNodeAgent plugin() {
    return PipeConfigNodeAgentHolder.HANDLE.pipePluginConfigNodeAgent;
  }

  /**
   * Get the singleton instance of {@link PipeRuntimeConfigNodeAgent}.
   *
   * @return the singleton instance of {@link PipeRuntimeConfigNodeAgent}
   */
  public static PipeRuntimeConfigNodeAgent runtime() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeRuntimeConfigNodeAgent;
  }

  /**
   * Get the singleton instance of {@link PipeReceiverConfigNodeAgent}.
   *
   * @return the singleton instance of {@link PipeReceiverConfigNodeAgent}
   */
  public static PipeReceiverConfigNodeAgent receiver() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeReceiverConfigNodeAgent;
  }
}
