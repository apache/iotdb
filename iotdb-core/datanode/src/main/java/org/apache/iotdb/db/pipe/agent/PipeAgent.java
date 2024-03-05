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

import org.apache.iotdb.db.pipe.agent.plugin.PipeDataNodePluginAgent;
import org.apache.iotdb.db.pipe.agent.receiver.PipeDataNodeReceiverAgent;
import org.apache.iotdb.db.pipe.agent.runtime.PipeDataNodeRuntimeAgent;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTaskAgent;
import org.apache.iotdb.db.service.DataNode;

/** {@link PipeAgent} is the entry point of the pipe module in {@link DataNode}. */
public class PipeAgent {

  private final PipeDataNodePluginAgent pipeDataNodePluginAgent;
  private final PipeDataNodeTaskAgent pipeDataNodeTaskAgent;
  private final PipeDataNodeRuntimeAgent pipeDataNodeRuntimeAgent;
  private final PipeDataNodeReceiverAgent pipeDataNodeReceiverAgent;

  /** Private constructor to prevent users from creating a new instance. */
  private PipeAgent() {
    pipeDataNodePluginAgent = new PipeDataNodePluginAgent();
    pipeDataNodeTaskAgent = new PipeDataNodeTaskAgent();
    pipeDataNodeRuntimeAgent = new PipeDataNodeRuntimeAgent();
    pipeDataNodeReceiverAgent = new PipeDataNodeReceiverAgent();
  }

  /** The singleton holder of {@link PipeAgent}. */
  private static class PipeAgentHolder {
    private static final PipeAgent HANDLE = new PipeAgent();
  }

  /**
   * Get the singleton instance of {@link PipeDataNodeTaskAgent}.
   *
   * @return the singleton instance of {@link PipeDataNodeTaskAgent}
   */
  public static PipeDataNodeTaskAgent task() {
    return PipeAgentHolder.HANDLE.pipeDataNodeTaskAgent;
  }

  /**
   * Get the singleton instance of {@link PipeDataNodePluginAgent}.
   *
   * @return the singleton instance of {@link PipeDataNodePluginAgent}
   */
  public static PipeDataNodePluginAgent plugin() {
    return PipeAgentHolder.HANDLE.pipeDataNodePluginAgent;
  }

  /**
   * Get the singleton instance of {@link PipeDataNodeRuntimeAgent}.
   *
   * @return the singleton instance of {@link PipeDataNodeRuntimeAgent}
   */
  public static PipeDataNodeRuntimeAgent runtime() {
    return PipeAgentHolder.HANDLE.pipeDataNodeRuntimeAgent;
  }

  /**
   * Get the singleton instance of {@link PipeDataNodeReceiverAgent}.
   *
   * @return the singleton instance of {@link PipeDataNodeReceiverAgent}
   */
  public static PipeDataNodeReceiverAgent receiver() {
    return PipeAgentHolder.HANDLE.pipeDataNodeReceiverAgent;
  }
}
