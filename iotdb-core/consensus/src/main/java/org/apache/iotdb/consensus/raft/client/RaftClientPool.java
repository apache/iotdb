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

package org.apache.iotdb.consensus.raft.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty.DefaultProperty;
import org.apache.iotdb.consensus.raft.config.RaftConfig;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

/** Pool factories for the async and sync Raft clients (mirrors iot/client). */
public final class RaftClientPool {

  public static final String ASYNC_POOL_THREADS = "AsyncRaftConsensusClientPool";

  private RaftClientPool() {}

  public static IClientManager<TEndPoint, AsyncRaftServiceClient> newAsyncManager(
      RaftConfig.Rpc rpc) {
    return new IClientManager.Factory<TEndPoint, AsyncRaftServiceClient>()
        .createClientManager(new AsyncFactory(rpc));
  }

  public static IClientManager<TEndPoint, SyncRaftServiceClient> newSyncManager(
      RaftConfig.Rpc rpc) {
    return new IClientManager.Factory<TEndPoint, SyncRaftServiceClient>()
        .createClientManager(new SyncFactory(rpc));
  }

  public static final class SyncFactory
      implements IClientPoolFactory<TEndPoint, SyncRaftServiceClient> {

    private final RaftConfig.Rpc rpc;

    public SyncFactory(RaftConfig.Rpc rpc) {
      this.rpc = rpc;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncRaftServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncRaftServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncRaftServiceClient> pool =
          new GenericKeyedObjectPool<>(
              new SyncRaftServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(rpc.rpcThriftCompressionEnabled)
                      .setPrintLogWhenEncounterException(
                          rpc.printLogWhenThriftClientEncounterException)
                      .build()),
              new ClientPoolProperty.Builder<SyncRaftServiceClient>()
                  .setMaxClientNumForEachNode(rpc.maxClientNumForEachNode)
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance().registerClientManager(getClass().getSimpleName(), pool);
      return pool;
    }
  }

  public static final class AsyncFactory
      implements IClientPoolFactory<TEndPoint, AsyncRaftServiceClient> {

    private final RaftConfig.Rpc rpc;

    public AsyncFactory(RaftConfig.Rpc rpc) {
      this.rpc = rpc;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncRaftServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncRaftServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncRaftServiceClient> pool =
          new GenericKeyedObjectPool<>(
              new AsyncRaftServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(rpc.connectionTimeoutInMs)
                      .setRpcThriftCompressionEnabled(rpc.rpcThriftCompressionEnabled)
                      .setSelectorNumOfAsyncClientManager(rpc.selectorNumOfClientManager)
                      .setPrintLogWhenEncounterException(
                          rpc.printLogWhenThriftClientEncounterException)
                      .build(),
                  ASYNC_POOL_THREADS),
              new ClientPoolProperty.Builder<AsyncRaftServiceClient>()
                  .setMaxClientNumForEachNode(rpc.maxClientNumForEachNode)
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance().registerClientManager(getClass().getSimpleName(), pool);
      return pool;
    }
  }
}
