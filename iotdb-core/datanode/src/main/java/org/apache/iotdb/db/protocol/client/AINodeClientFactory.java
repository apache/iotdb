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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.protocol.client.ainode.AINodeClient;
import org.apache.iotdb.db.protocol.client.ainode.AsyncAINodeServiceClient;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.Optional;

/** Dedicated factory for AINodeClient + AINodeClientPoolFactory. */
public class AINodeClientFactory extends ThriftClientFactory<TEndPoint, AINodeClient> {

  private static final int connectionTimeout =
      CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS();

  public AINodeClientFactory(
      ClientManager<TEndPoint, AINodeClient> manager, ThriftClientProperty thriftProperty) {
    super(manager, thriftProperty);
  }

  @Override
  public PooledObject<AINodeClient> makeObject(TEndPoint endPoint) throws Exception {
    return new DefaultPooledObject<>(
        new AINodeClient(thriftClientProperty, endPoint, clientManager));
  }

  @Override
  public void destroyObject(TEndPoint key, PooledObject<AINodeClient> pooled) throws Exception {
    pooled.getObject().invalidate();
  }

  @Override
  public boolean validateObject(TEndPoint key, PooledObject<AINodeClient> pooledObject) {
    return Optional.ofNullable(pooledObject.getObject().getTransport())
        .map(org.apache.thrift.transport.TTransport::isOpen)
        .orElse(false);
  }

  /** The PoolFactory originally inside ClientPoolFactory — now moved here. */
  public static class AINodeClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AINodeClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AINodeClient> createClientPool(
        ClientManager<TEndPoint, AINodeClient> manager) {

      // Build thrift client properties
      ThriftClientProperty thriftProperty =
          new ThriftClientProperty.Builder()
              .setConnectionTimeoutMs(connectionTimeout)
              .setRpcThriftCompressionEnabled(
                  CommonDescriptor.getInstance().getConfig().isRpcThriftCompressionEnabled())
              .build();

      GenericKeyedObjectPool<TEndPoint, AINodeClient> pool =
          new GenericKeyedObjectPool<>(
              new AINodeClientFactory(manager, thriftProperty),
              new ClientPoolProperty.Builder<AINodeClient>()
                  .setMaxClientNumForEachNode(
                      CommonDescriptor.getInstance().getConfig().getMaxClientNumForEachNode())
                  .build()
                  .getConfig());

      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), pool);

      return pool;
    }
  }

  public static class AINodeHeartbeatClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncAINodeServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncAINodeServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncAINodeServiceClient> manager) {

      final CommonConfig conf = CommonDescriptor.getInstance().getConfig();

      GenericKeyedObjectPool<TEndPoint, AsyncAINodeServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncAINodeServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(false)
                      .build(),
                  ThreadName.ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncAINodeServiceClient>()
                  .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());

      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);

      return clientPool;
    }
  }
}
