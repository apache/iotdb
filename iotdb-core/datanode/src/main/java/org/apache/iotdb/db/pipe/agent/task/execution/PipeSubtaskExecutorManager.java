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

package org.apache.iotdb.db.pipe.agent.task.execution;

import org.apache.iotdb.db.pipe.consensus.PipeConsensusSubtaskExecutor;
import org.apache.iotdb.db.subscription.task.execution.SubscriptionSubtaskExecutor;

/**
 * PipeTaskExecutor is responsible for executing the pipe tasks, and it is scheduled by the
 * PipeTaskScheduler. It is a singleton class.
 */
public class PipeSubtaskExecutorManager {
  private final PipeProcessorSubtaskExecutor processorExecutor;
  private final PipeConnectorSubtaskExecutor connectorExecutor;
  private final SubscriptionSubtaskExecutor subscriptionExecutor;
  private final PipeConsensusSubtaskExecutor consensusExecutor;

  public PipeProcessorSubtaskExecutor getProcessorExecutor() {
    return processorExecutor;
  }

  public PipeConnectorSubtaskExecutor getConnectorExecutor() {
    return connectorExecutor;
  }

  public SubscriptionSubtaskExecutor getSubscriptionExecutor() {
    return subscriptionExecutor;
  }

  public PipeConsensusSubtaskExecutor getConsensusExecutor() {
    return consensusExecutor;
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeSubtaskExecutorManager() {
    processorExecutor = new PipeProcessorSubtaskExecutor();
    connectorExecutor = new PipeConnectorSubtaskExecutor();
    subscriptionExecutor = new SubscriptionSubtaskExecutor();
    consensusExecutor = new PipeConsensusSubtaskExecutor();
  }

  private static class PipeTaskExecutorHolder {
    private static PipeSubtaskExecutorManager instance = null;
  }

  public static synchronized PipeSubtaskExecutorManager getInstance() {
    if (PipeTaskExecutorHolder.instance == null) {
      PipeTaskExecutorHolder.instance = new PipeSubtaskExecutorManager();
    }
    return PipeTaskExecutorHolder.instance;
  }
}
