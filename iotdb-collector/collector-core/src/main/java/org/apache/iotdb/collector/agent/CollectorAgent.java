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

package org.apache.iotdb.collector.agent;

import org.apache.iotdb.collector.agent.executor.CollectorTaskExecutorAgent;
import org.apache.iotdb.collector.agent.plugin.CollectorPluginAgent;
import org.apache.iotdb.collector.agent.task.CollectorTaskAgent;

public class CollectorAgent {

  private final CollectorTaskAgent collectorTaskAgent = CollectorTaskAgent.instance();
  private final CollectorTaskExecutorAgent collectorTaskExecutorAgent =
      CollectorTaskExecutorAgent.instance();
  private final CollectorPluginAgent collectorPluginAgent = CollectorPluginAgent.instance();

  private CollectorAgent() {}

  public static CollectorTaskAgent task() {
    return CollectorAgentHolder.INSTANCE.collectorTaskAgent;
  }

  public static CollectorTaskExecutorAgent executor() {
    return CollectorAgentHolder.INSTANCE.collectorTaskExecutorAgent;
  }

  public static CollectorPluginAgent plugin() {
    return CollectorAgentHolder.INSTANCE.collectorPluginAgent;
  }

  private static class CollectorAgentHolder {
    private static final CollectorAgent INSTANCE = new CollectorAgent();
  }
}
