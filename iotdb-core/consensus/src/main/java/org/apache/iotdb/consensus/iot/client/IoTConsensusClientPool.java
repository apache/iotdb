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
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty.DefaultProperty;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class IoTConsensusClientPool {

  private IoTConsensusClientPool() {
    // empty constructor
  }

  public static class SyncIoTConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncIoTConsensusServiceClient> {

    private final IoTConsensusConfig config;

    public SyncIoTConsensusServiceClientPoolFactory(IoTConsensusConfig config) {
      this.config = config;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncIoTConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncIoTConsensusServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncIoTConsensusServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncIoTConsensusServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // We never let it time out, because the logic behind a timeout is also to
                      // retry, which might actually worsen the situation. For example, resulting in
                      // a significant increase in the number of file handles.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(
                          config.getRpc().isRpcThriftCompressionEnabled())
                      .setPrintLogWhenEncounterException(
                          config.getRpc().isPrintLogWhenThriftClientEncounterException())
                      .build()),
              new ClientPoolProperty.Builder<SyncIoTConsensusServiceClient>()
                  .setMaxClientNumForEachNode(config.getRpc().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncIoTConsensusServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncIoTConsensusServiceClient> {

    private final IoTConsensusConfig config;

    public AsyncIoTConsensusServiceClientPoolFactory(IoTConsensusConfig config) {
      this.config = config;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncIoTConsensusServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncIoTConsensusServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncIoTConsensusServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncIoTConsensusServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // We never let it time out, because the logic behind a timeout is also to
                      // retry, which might actually worsen the situation. For example, resulting in
                      // a significant increase in the number of file handles.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(
                          config.getRpc().isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(
                          config.getRpc().getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(
                          config.getRpc().isPrintLogWhenThriftClientEncounterException())
                      .build(),
                  ThreadName.ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncIoTConsensusServiceClient>()
                  .setMaxClientNumForEachNode(config.getRpc().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
