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

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.pipe.consensus.PipeConsensusSubtaskExecutor;
import org.apache.iotdb.db.subscription.task.execution.SubscriptionSubtaskExecutor;

import java.util.function.Supplier;

/**
 * PipeTaskExecutor is responsible for executing the pipe tasks, and it is scheduled by the
 * PipeTaskScheduler. It is a singleton class.
 */
public class PipeSubtaskExecutorManager {
  private final PipeProcessorSubtaskExecutor processorExecutor;
  private final Supplier<PipeConnectorSubtaskExecutor> connectorExecutorSupplier;
  private final SubscriptionSubtaskExecutor subscriptionExecutor;
  private final Supplier<PipeConsensusSubtaskExecutor> consensusExecutorSupplier;

  public PipeProcessorSubtaskExecutor getProcessorExecutor() {
    return processorExecutor;
  }

  public Supplier<PipeConnectorSubtaskExecutor> getConnectorExecutorSupplier() {
    return connectorExecutorSupplier;
  }

  public SubscriptionSubtaskExecutor getSubscriptionExecutor() {
    return subscriptionExecutor;
  }

  public Supplier<PipeConsensusSubtaskExecutor> getConsensusExecutorSupplier() {
    return consensusExecutorSupplier;
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeSubtaskExecutorManager() {
    processorExecutor = new PipeProcessorSubtaskExecutor();
    connectorExecutorSupplier = PipeConnectorSubtaskExecutor::new;
    subscriptionExecutor =
        SubscriptionConfig.getInstance().getSubscriptionEnabled()
            ? new SubscriptionSubtaskExecutor()
            : null;
    consensusExecutorSupplier = PipeConsensusSubtaskExecutor::new;
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
