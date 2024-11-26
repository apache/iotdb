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

package org.apache.iotdb.commons.client.container;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory.AsyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.commons.client.ClientPoolFactory.SyncPipeConsensusServiceClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.client.property.PipeConsensusClientProperty;
import org.apache.iotdb.commons.client.sync.SyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

/**
 * This class is used to hold the syncClientManager and asyncClientManager used by pipeConsensus.
 * The purpose of designing this class is that both the consensus layer and the datanode layer of
 * pipeConsensus use clientManager.
 *
 * <p>Note: we hope to create the corresponding clientManager only when the consensus is
 * pipeConsensus to avoid unnecessary overhead.
 */
public class PipeConsensusClientMgrContainer {
  private static final CommonConfig CONF = CommonDescriptor.getInstance().getConfig();
  private final PipeConsensusClientProperty config;

  private PipeConsensusClientMgrContainer() {
    // load rpc client config
    this.config =
        PipeConsensusClientProperty.newBuilder()
            .setIsRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnabled())
            .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
            .setSelectorNumOfClientManager(CONF.getSelectorNumOfClientManager())
            .build();
  }

  public IClientManager<TEndPoint, AsyncPipeConsensusServiceClient> newAsyncClientManager() {
    return new IClientManager.Factory<TEndPoint, AsyncPipeConsensusServiceClient>()
        .createClientManager(new AsyncPipeConsensusServiceClientPoolFactory(config));
  }

  public IClientManager<TEndPoint, SyncPipeConsensusServiceClient> newSyncClientManager() {
    return new IClientManager.Factory<TEndPoint, SyncPipeConsensusServiceClient>()
        .createClientManager(new SyncPipeConsensusServiceClientPoolFactory(config));
  }

  private static class PipeConsensusClientMgrContainerHolder {
    private static PipeConsensusClientMgrContainer INSTANCE;

    private PipeConsensusClientMgrContainerHolder() {}

    public static void build() {
      if (INSTANCE == null) {
        INSTANCE = new PipeConsensusClientMgrContainer();
      }
    }
  }

  public static PipeConsensusClientMgrContainer getInstance() {
    return PipeConsensusClientMgrContainerHolder.INSTANCE;
  }

  // Only when consensus protocol is PipeConsensus, this method will be called once when construct
  // consensus class.
  public static void build() {
    PipeConsensusClientMgrContainerHolder.build();
  }
}
