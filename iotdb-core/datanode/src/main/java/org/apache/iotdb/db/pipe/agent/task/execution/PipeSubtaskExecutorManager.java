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

import org.apache.iotdb.commons.consensus.iotv2.container.IoTV2GlobalComponentContainer;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.IoTConsensusV2SubtaskExecutor;
import org.apache.iotdb.db.subscription.task.execution.SubscriptionSubtaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * PipeTaskExecutor is responsible for executing the pipe tasks, and it is scheduled by the
 * PipeTaskScheduler. It is a singleton class.
 */
public class PipeSubtaskExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtaskExecutorManager.class);

  private final PipeProcessorSubtaskExecutor processorExecutor;
  private final Supplier<PipeSinkSubtaskExecutor> connectorExecutorSupplier;
  private volatile SubscriptionSubtaskExecutor subscriptionExecutor;

  public PipeProcessorSubtaskExecutor getProcessorExecutor() {
    return processorExecutor;
  }

  public Supplier<PipeSinkSubtaskExecutor> getConnectorExecutorSupplier() {
    return connectorExecutorSupplier;
  }

  public synchronized IoTConsensusV2SubtaskExecutor getConsensusExecutor() {
    final IoTV2GlobalComponentContainer container = IoTV2GlobalComponentContainer.getInstance();
    final PipeSubtaskExecutor consensusExecutor = container.getConsensusExecutor();
    if (consensusExecutor instanceof IoTConsensusV2SubtaskExecutor
        && !consensusExecutor.isShutdown()) {
      return (IoTConsensusV2SubtaskExecutor) consensusExecutor;
    }

    // The consensus executor is shared by all consensus pipes. Recreate it if it has been shut
    // down, otherwise the following consensus pipes can never start their sink subtasks.
    LOGGER.warn(
        DataNodePipeMessages
            .LOG_THE_SHARED_CONSENSUS_EXECUTOR_HAS_BEEN_SHUT_DOWN_RECREATING_IT_C14080E3);
    final IoTConsensusV2SubtaskExecutor newConsensusExecutor = new IoTConsensusV2SubtaskExecutor();
    container.setConsensusExecutor(newConsensusExecutor);
    return newConsensusExecutor;
  }

  public SubscriptionSubtaskExecutor getSubscriptionExecutor() {
    ensureSubscriptionExecutors();
    return subscriptionExecutor;
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeSubtaskExecutorManager() {
    processorExecutor = new PipeProcessorSubtaskExecutor();
    connectorExecutorSupplier = PipeSinkSubtaskExecutor::new;
    ensureSubscriptionExecutors();
    // IoTV2 uses global singleton executor pool.
    IoTV2GlobalComponentContainer.getInstance()
        .setConsensusExecutor(new IoTConsensusV2SubtaskExecutor());
  }

  public synchronized void ensureSubscriptionExecutors() {
    if (!SubscriptionConfig.getInstance().getSubscriptionEnabled()
        || !PipeDataNodeAgent.isSubscriptionFeatureEnabled()) {
      return;
    }
    if (subscriptionExecutor == null || subscriptionExecutor.isShutdown()) {
      subscriptionExecutor = new SubscriptionSubtaskExecutor();
    }
  }

  public synchronized void shutdownSubscriptionExecutors() {
    if (subscriptionExecutor != null) {
      subscriptionExecutor.shutdown();
      subscriptionExecutor = null;
    }
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
