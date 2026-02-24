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

package org.apache.iotdb.commons.consensus.iotv2.container;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory.AsyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.commons.client.ClientPoolFactory.SyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.client.property.PipeConsensusClientProperty;
import org.apache.iotdb.commons.client.sync.SyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to hold the global component such as syncClientManager and asyncClientManager
 * used by pipeConsensus. The purpose of designing this class is that both the consensus layer and
 * the datanode layer of pipeConsensus use clientManager.
 *
 * <p>Note: we hope to create the corresponding clientManager only when the consensus is
 * pipeConsensus to avoid unnecessary overhead.
 */
public class IoTV2GlobalComponentContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTV2GlobalComponentContainer.class);
  private static final CommonConfig CONF = CommonDescriptor.getInstance().getConfig();
  private final PipeConsensusClientProperty config;
  private final IClientManager<TEndPoint, AsyncPipeConsensusServiceClient> asyncClientManager;
  private final IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager;
  private final ScheduledExecutorService backgroundTaskService;
  private PipeSubtaskExecutor consensusExecutor;

  private IoTV2GlobalComponentContainer() {
    // load rpc client config
    this.config =
        PipeConsensusClientProperty.newBuilder()
            .setIsRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnabled())
            .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
            .setSelectorNumOfClientManager(Math.max(3, CONF.getSelectorNumOfClientManager()))
            .build();
    this.asyncClientManager =
        new IClientManager.Factory<TEndPoint, AsyncPipeConsensusServiceClient>()
            .createClientManager(new AsyncPipeConsensusServiceClientPoolFactory(config));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncPipeConsensusServiceClient>()
            .createClientManager(new SyncPipeConsensusServiceClientPoolFactory(config));
    this.backgroundTaskService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.PIPE_CONSENSUS_BACKGROUND_TASK_EXECUTOR.getName());
  }

  public IClientManager<TEndPoint, AsyncPipeConsensusServiceClient> getGlobalAsyncClientManager() {
    return this.asyncClientManager;
  }

  public IClientManager<TEndPoint, SyncPipeConsensusServiceClient> getGlobalSyncClientManager() {
    return this.syncClientManager;
  }

  public ScheduledExecutorService getBackgroundTaskService() {
    return this.backgroundTaskService;
  }

  public void stopBackgroundTaskService() {
    backgroundTaskService.shutdownNow();
    try {
      if (!backgroundTaskService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("IoTV2 background service did not terminate within {}s", 30);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("IoTV2 background Thread still doesn't exit after 30s");
      Thread.currentThread().interrupt();
    }
  }

  public PipeSubtaskExecutor getConsensusExecutor() {
    return consensusExecutor;
  }

  public void setConsensusExecutor(PipeSubtaskExecutor consensusExecutor) {
    this.consensusExecutor = consensusExecutor;
  }

  private static class IoTV2GlobalComponentContainerHolder {
    private static IoTV2GlobalComponentContainer INSTANCE;

    private IoTV2GlobalComponentContainerHolder() {}

    public static void build() {
      if (INSTANCE == null) {
        INSTANCE = new IoTV2GlobalComponentContainer();
      }
    }
  }

  public static IoTV2GlobalComponentContainer getInstance() {
    if (IoTV2GlobalComponentContainerHolder.INSTANCE == null) {
      IoTV2GlobalComponentContainer.build();
    }
    return IoTV2GlobalComponentContainerHolder.INSTANCE;
  }

  // Only when consensus protocol is PipeConsensus, this method will be called once when construct
  // consensus class.
  public static void build() {
    IoTV2GlobalComponentContainerHolder.build();
  }
}
