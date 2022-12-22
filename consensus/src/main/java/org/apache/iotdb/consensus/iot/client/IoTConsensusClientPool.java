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

package org.apache.iotdb.consensus.iot.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class IoTConsensusClientPool {

  private IoTConsensusClientPool() {}

  public static class SyncIoTConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncIoTConsensusServiceClient> {

    private final IoTConsensusConfig config;

    public SyncIoTConsensusServiceClientPoolFactory(IoTConsensusConfig config) {
      this.config = config;
    }

    @Override
    public KeyedObjectPool<TEndPoint, SyncIoTConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncIoTConsensusServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncIoTConsensusServiceClient.Factory(
              manager,
              new ClientFactoryProperty.Builder()
                  .setConnectionTimeoutMs(config.getRpc().getConnectionTimeoutInMs())
                  .setRpcThriftCompressionEnabled(config.getRpc().isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(
                      config.getRpc().getSelectorNumOfClientManager())
                  .build()),
          new ClientPoolProperty.Builder<SyncIoTConsensusServiceClient>().build().getConfig());
    }
  }

  public static class AsyncIoTConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncIoTConsensusServiceClient> {

    private final IoTConsensusConfig config;
    private static final String IOT_CONSENSUS_CLIENT_POOL_THREAD_NAME = "IoTConsensusClientPool";

    public AsyncIoTConsensusServiceClientPoolFactory(IoTConsensusConfig config) {
      this.config = config;
    }

    @Override
    public KeyedObjectPool<TEndPoint, AsyncIoTConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncIoTConsensusServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncIoTConsensusServiceClient.Factory(
              manager,
              new ClientFactoryProperty.Builder()
                  .setConnectionTimeoutMs(config.getRpc().getConnectionTimeoutInMs())
                  .setRpcThriftCompressionEnabled(config.getRpc().isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(
                      config.getRpc().getSelectorNumOfClientManager())
                  .build(),
              IOT_CONSENSUS_CLIENT_POOL_THREAD_NAME),
          new ClientPoolProperty.Builder<AsyncIoTConsensusServiceClient>()
              .setMaxIdleClientForEachNode(config.getRpc().getMaxConnectionForInternalService())
              .setMaxTotalClientForEachNode(config.getRpc().getMaxConnectionForInternalService())
              .build()
              .getConfig());
    }
  }
}
