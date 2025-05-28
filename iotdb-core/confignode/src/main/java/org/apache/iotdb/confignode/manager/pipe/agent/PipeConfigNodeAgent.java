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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.manager.pipe.agent.plugin.PipeConfigNodePluginAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.receiver.IoTDBConfigNodeReceiverAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.runtime.PipeConfigNodeRuntimeAgent;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeTaskAgent;
import org.apache.iotdb.confignode.service.ConfigNode;

/** {@link PipeConfigNodeAgent} is the entry point of the pipe module in {@link ConfigNode}. */
public class PipeConfigNodeAgent {

  private final PipeConfigNodeTaskAgent pipeConfigNodeTaskAgent;
  private final PipeConfigNodePluginAgent pipeConfigNodePluginAgent;
  private final PipeConfigNodeRuntimeAgent pipeConfigNodeRuntimeAgent;
  private final IoTDBConfigNodeReceiverAgent pipeConfigNodeReceiverAgent;

  /** Private constructor to prevent users from creating a new instance. */
  private PipeConfigNodeAgent() {
    pipeConfigNodeTaskAgent = new PipeConfigNodeTaskAgent();
    pipeConfigNodePluginAgent = new PipeConfigNodePluginAgent(null);
    pipeConfigNodeRuntimeAgent = new PipeConfigNodeRuntimeAgent();
    pipeConfigNodeReceiverAgent = new IoTDBConfigNodeReceiverAgent();

    // bind runtime agent's period executor to pipe config for pipeTaskMeta persist progressIndex,
    // due to project structure reasons.
    PipeConfig.getInstance().setPipePeriodicalJobExecutor(runtime().getPipePeriodicalJobExecutor());
  }

  /** The singleton holder of {@link PipeConfigNodeAgent}. */
  private static class PipeConfigNodeAgentHolder {
    private static final PipeConfigNodeAgent HANDLE = new PipeConfigNodeAgent();
  }

  /**
   * Get the singleton instance of {@link PipeConfigNodeTaskAgent}.
   *
   * @return the singleton instance of {@link PipeConfigNodeTaskAgent}
   */
  public static PipeConfigNodeTaskAgent task() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodeTaskAgent;
  }

  /**
   * Get the singleton instance of {@link PipeConfigNodePluginAgent}.
   *
   * @return the singleton instance of {@link PipeConfigNodePluginAgent}
   */
  public static PipeConfigNodePluginAgent plugin() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodePluginAgent;
  }

  /**
   * Get the singleton instance of {@link PipeConfigNodeRuntimeAgent}.
   *
   * @return the singleton instance of {@link PipeConfigNodeRuntimeAgent}
   */
  public static PipeConfigNodeRuntimeAgent runtime() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodeRuntimeAgent;
  }

  /**
   * Get the singleton instance of {@link IoTDBConfigNodeReceiverAgent}.
   *
   * @return the singleton instance of {@link IoTDBConfigNodeReceiverAgent}
   */
  public static IoTDBConfigNodeReceiverAgent receiver() {
    return PipeConfigNodeAgentHolder.HANDLE.pipeConfigNodeReceiverAgent;
  }
}
