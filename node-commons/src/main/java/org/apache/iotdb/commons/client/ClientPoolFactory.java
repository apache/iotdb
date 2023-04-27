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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ClientPoolFactory {

  private static final CommonConfig conf = CommonDescriptor.getInstance().getConfig();

  private ClientPoolFactory() {}

  public static class SyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncConfigNodeIServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, SyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncConfigNodeIServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncConfigNodeIServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .build()),
          new ClientPoolProperty.Builder<SyncConfigNodeIServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class AsyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncConfigNodeIServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, AsyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncConfigNodeIServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncConfigNodeIServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .build(),
              ThreadName.ASYNC_CONFIGNODE_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncConfigNodeIServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class SyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .build()),
          new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class AsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .build(),
              ThreadName.ASYNC_DATANODE_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class AsyncConfigNodeHeartbeatServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncConfigNodeIServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, AsyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncConfigNodeIServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncConfigNodeIServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .setPrintLogWhenEncounterException(false)
                  .build(),
              ThreadName.ASYNC_CONFIGNODE_HEARTBEAT_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncConfigNodeIServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class AsyncDataNodeHeartbeatServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .setPrintLogWhenEncounterException(false)
                  .build(),
              ThreadName.ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class SyncDataNodeMPPDataExchangeServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeMPPDataExchangeServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .build()),
          new ClientPoolProperty.Builder<SyncDataNodeMPPDataExchangeServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class AsyncDataNodeMPPDataExchangeServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> {

    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeMPPDataExchangeServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .build(),
              ThreadName.ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncDataNodeMPPDataExchangeServiceClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }
}
