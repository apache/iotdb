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

package org.apache.iotdb.consensus.pipe.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty.DefaultProperty;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class PipeConsensusClientPool {
  private PipeConsensusClientPool() {
    // do nothing
  }

  public static class SyncPipeConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncPipeConsensusServiceClient> {

    private final PipeConsensusConfig config;

    public SyncPipeConsensusServiceClientPoolFactory(PipeConsensusConfig config) {
      this.config = config;
    }

    @Override
    public KeyedObjectPool<TEndPoint, SyncPipeConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncPipeConsensusServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncPipeConsensusServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncPipeConsensusServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // TODO: consider timeout and evict strategy.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(
                          config.getRpc().isRpcThriftCompressionEnabled())
                      .setPrintLogWhenEncounterException(
                          config.getRpc().isPrintLogWhenThriftClientEncounterException())
                      .build()),
              new ClientPoolProperty.Builder<SyncPipeConsensusServiceClient>()
                  .setMaxClientNumForEachNode(config.getRpc().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncPipeConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncPipeConsensusServiceClient> {

    private final PipeConsensusConfig config;

    public AsyncPipeConsensusServiceClientPoolFactory(PipeConsensusConfig config) {
      this.config = config;
    }

    @Override
    public KeyedObjectPool<TEndPoint, AsyncPipeConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncPipeConsensusServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncPipeConsensusServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncPipeConsensusServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // TODO: consider timeout and evict strategy.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(
                          config.getRpc().isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(
                          config.getRpc().getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(
                          config.getRpc().isPrintLogWhenThriftClientEncounterException())
                      .build(),
                  ThreadName.ASYNC_DATANODE_PIPE_CONSENSUS_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncPipeConsensusServiceClient>()
                  .setMaxClientNumForEachNode(config.getRpc().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
